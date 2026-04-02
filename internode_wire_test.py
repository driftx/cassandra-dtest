import logging
import os

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestWriteInternodeWire(Tester):

    @since('4.0')
    def test_writetime_blob_does_not_send_blob_payload_on_wire(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'internode_compression': 'none'})
        cluster.populate(3).start()

        nodes = cluster.nodelist()
        schema_session = self.patient_cql_connection(nodes[0])
        create_ks(schema_session, 'ks', 1)
        schema_session.execute("CREATE TABLE ks.ab (k int PRIMARY KEY, c1 bigint, v blob)")

        key = 424242
        blob_size = 1024 * 1024
        blob = bytearray(os.urandom(blob_size))
        schema_session.execute("INSERT INTO ks.ab (k, c1, v) VALUES (%s, %s, %s)", (key, 7, blob))

        replica = self._identify_replica(nodes, 'ks', 'ab', key)
        coordinator = next(n for n in nodes if n != replica)
        logger.info("Using replica=%s coordinator=%s", replica.name, coordinator.name)

        replica.flush()

        coordinator_session = self.patient_exclusive_cql_connection(coordinator, keyspace='ks')
        replica_session = self.patient_exclusive_cql_connection(replica)

        # Warm connection to reduce one-time handshake noise in the first measured batch.
        warm_stmt = SimpleStatement("SELECT c1 FROM ks.ab WHERE k = %s", consistency_level=ConsistencyLevel.ONE)
        coordinator_session.execute(warm_stmt, [key])

        reads = 50
        small_stmt = SimpleStatement("SELECT c1 FROM ks.ab WHERE k = %s", consistency_level=ConsistencyLevel.ONE)
        wt_stmt = SimpleStatement("SELECT c1, WRITETIME(v) FROM ks.ab WHERE k = %s", consistency_level=ConsistencyLevel.ONE)
        blob_stmt = SimpleStatement("SELECT c1, v FROM ks.ab WHERE k = %s", consistency_level=ConsistencyLevel.ONE)

        delta_small = self._measure_sent_bytes_delta(replica_session, coordinator, coordinator_session, small_stmt, [key], reads)
        delta_wt = self._measure_sent_bytes_delta(replica_session, coordinator, coordinator_session, wt_stmt, [key], reads)
        delta_blob = self._measure_sent_bytes_delta(replica_session, coordinator, coordinator_session, blob_stmt, [key], reads)

        per_small = delta_small / reads
        per_wt = delta_wt / reads
        per_blob = delta_blob / reads

        logger.info("sent_bytes deltas small=%s wt=%s blob=%s", delta_small, delta_wt, delta_blob)
        logger.info("sent_bytes per query small=%.1f wt=%.1f blob=%.1f blob_size=%s",
                    per_small, per_wt, per_blob, blob_size)

        # Positive control: selecting the blob should move approximately blob_size bytes/query over internode.
        assert per_blob > per_small + (0.5 * blob_size), \
            "Selecting blob did not significantly increase internode bytes"

        # Behavior under test: WRITETIME(v) should not send the blob payload over internode.
        assert per_wt < per_small + (0.2 * blob_size), \
            "WRITETIME(v) unexpectedly increased internode bytes close to blob payload size"

        # Tight A/B check: WRITETIME(v) traffic should be far from selecting v directly.
        assert abs(per_wt - per_blob) > (0.5 * blob_size), \
            "WRITETIME(v) internode bytes were too close to SELECT v bytes"

    @since('4.0')
    def test_selecting_simple_column_does_not_send_blob_payload_on_wire(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'internode_compression': 'none'})
        cluster.populate(3).start()

        nodes = cluster.nodelist()
        schema_session = self.patient_cql_connection(nodes[0])
        create_ks(schema_session, 'ks', 1)
        schema_session.execute("""
            CREATE TABLE ks.ts (
                k int,
                c1 bigint,
                c2 int,
                c3 int,
                c4 int,
                marker bigint,
                v blob,
                PRIMARY KEY (k, c1, c2, c3, c4)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC, c3 ASC, c4 ASC)
        """)

        key = 515151
        blob_size = 1024 * 1024
        blob = bytearray(os.urandom(blob_size))
        schema_session.execute("""
            INSERT INTO ks.ts (k, c1, c2, c3, c4, marker, v)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (key, 42, 7, 8, 9, 11, blob))

        replica = self._identify_replica(nodes, 'ks', 'ts', key)
        coordinator = next(n for n in nodes if n != replica)
        logger.info("Using replica=%s coordinator=%s", replica.name, coordinator.name)

        replica.flush()

        coordinator_session = self.patient_exclusive_cql_connection(coordinator, keyspace='ks')
        replica_session = self.patient_exclusive_cql_connection(replica)

        # Warm connection to reduce one-time handshake noise in the first measured batch.
        warm_stmt = SimpleStatement("SELECT c1 FROM ks.ts WHERE k = %s ORDER BY c1 DESC LIMIT 1", consistency_level=ConsistencyLevel.ONE)
        coordinator_session.execute(warm_stmt, [key])

        reads = 50
        simple_stmt = SimpleStatement("SELECT c1 FROM ks.ts WHERE k = %s ORDER BY c1 DESC LIMIT 1", consistency_level=ConsistencyLevel.ONE)
        blob_stmt = SimpleStatement("SELECT c1, v FROM ks.ts WHERE k = %s ORDER BY c1 DESC LIMIT 1", consistency_level=ConsistencyLevel.ONE)

        delta_simple = self._measure_sent_bytes_delta(replica_session, coordinator, coordinator_session, simple_stmt, [key], reads)
        delta_blob = self._measure_sent_bytes_delta(replica_session, coordinator, coordinator_session, blob_stmt, [key], reads)

        per_simple = delta_simple / reads
        per_blob = delta_blob / reads

        logger.info("sent_bytes deltas simple=%s blob=%s", delta_simple, delta_blob)
        logger.info("sent_bytes per query simple=%.1f blob=%.1f blob_size=%s", per_simple, per_blob, blob_size)

        # Positive control (absolute): selecting the blob should move blob-sized bytes/query over internode.
        assert per_blob > (0.5 * blob_size), \
            "Selecting blob did not produce blob-sized internode bytes"

        # Behavior under test (absolute): selecting c1 should stay far below blob-sized payload.
        assert per_simple < (0.25 * blob_size), \
            "Selecting c1 appears to send blob-sized internode payload"

    def _measure_sent_bytes_delta(self, replica_session, coordinator, coordinator_session, stmt, params, reads):
        before = self._get_sent_bytes_to(replica_session, coordinator)
        for _ in range(reads):
            coordinator_session.execute(stmt, params)
        after = self._get_sent_bytes_to(replica_session, coordinator)
        return after - before

    def _get_sent_bytes_to(self, session, peer_node):
        target_address = peer_node.address()
        rows = session.execute("SELECT address, port, sent_bytes FROM system_views.internode_outbound")

        sent = 0
        for row in rows:
            if str(row.address) == target_address:
                sent += row.sent_bytes

        assert sent >= 0
        return sent

    def _identify_replica(self, nodes, keyspace, table, key):
        out, _, _ = nodes[0].nodetool("getendpoints {} {} {}".format(keyspace, table, key))
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        address = lines[-1]
        for node in nodes:
            if node.address() == address:
                return node

        raise AssertionError("Couldn't identify replica from nodetool output: {}".format(out))

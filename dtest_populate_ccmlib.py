#!/usr/bin/env python3
""" tickle the ccm repo for versions in the upgrade manifest """
import ccmlib.repository
from upgrade_tests.upgrade_manifest import MANIFEST

for k, v in MANIFEST.items():
    ccm_repo_cache_dir, _ = ccmlib.repository.setup(k.version)
    for ver in v:
        ccm_repo_cache_dir, _ = ccmlib.repository.setup(ver.version)

"""Module for extraction of data and RO-crate construction from the Print Genomics Lab
"""

from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Organisation,
    User,
)

from src.mt_api.mt_consts import UOA

# default MyTardis user, overwritten by values in env
MY_TARDIS_TEST_USER = User(
    identifier="MyTardis",
    name="MyTardisUser",
    email="test.mytardis@notavalidemail.com",
    pubkey_fingerprints=[],
    affiliation=UOA,
    mt_identifiers=[],
)

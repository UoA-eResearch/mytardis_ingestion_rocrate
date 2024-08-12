from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Organisation,
    User
)
from src.mt_api.mt_consts import UOA

MY_TARDIS_TEST_USER = User(
    identifier="MyTardis",
    name="MyTardisUser",
    email="james.love@auckland.ac.nz",
    pubkey_fingerprints=["C0626AEA19335E5587944C94EE7395E1CBF7668B"],
    affiliation=UOA,
    mt_identifiers=[]
)
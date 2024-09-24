"""Constants for object to be read into or out of MyTardis
"""

import enum

from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Organisation,
    User,
)

UOA = Organisation(
    mt_identifiers=["https://ror.org/03b94tp07"],
    name="The University of Auckland | Waipapa Taumata Rau",
    url="https://auckland.ac.nz",
    location="Auckland, New Zealand",
    research_org=True,
)

MY_TARDIS_USER = User(
    identifier="mtuser",
    name="my_tardis_user",
    email="",
    mt_identifiers=["my_tardis_ingestion"],
    affiliation=UOA,
    schema_type=["Person", "User", "MyTardisUser"],
    pubkey_fingerprints=[],
)


class MtObject(enum.Enum):
    """Enum for mytardis object types"""

    PROJECT = "Project"
    EXPERIMENT = "Experiment"
    DATASET = "Dataset"
    DATAFILE = "Datafile"

"""Constants for object to be read into or out of MyTardis
"""

import enum

from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Organisation,
)

UOA = Organisation(
    identifiers=["https://ror.org/03b94tp07"],
    name="The University of Auckland | Waipapa Taumata Rau",
    url="https://auckland.ac.nz",
    location="Auckland, New Zealand",
    research_org=True,
)


class MtObject(enum.Enum):
    """Enum for mytardis object types"""

    PROJECT = "Project"
    EXPERIMENT = "Experiment"
    DATASET = "Dataset"
    DATAFILE = "Datafile"

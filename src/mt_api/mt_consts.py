"""Constants for object to be read into or out of MyTardis
"""

import enum


class MtObject(enum.Enum):
    """Enum for

    Args:
        enum (_type_): _description_
    """

    PROJECT = "Project"
    EXPERIMENT = "Experiment"
    DATASET = "Dataset"
    DATAFILE = "Datafile"

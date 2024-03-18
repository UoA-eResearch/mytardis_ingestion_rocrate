# pylint: disable=missing-function-docstring
"""Helper functions and classes for managing and moving RO-Crate ingestible dataclasses.
"""

from typing import Dict, List, Optional

from src.rocrate_dataclasses.rocrate_dataclasses import (
    Datafile,
    Dataset,
    Experiment,
    MTMetadata,
    Organisation,
    Participant,
    Person,
    Project,
)


def create_metadata_(
    name: str, value: str, is_sensitive: bool, metadata_type: str
) -> MTMetadata:
    """Construct a metadata dataclass given the name of the metadata from an input file

    Args:
        name (str): name of the metadata in MyTardis
        value (str): the value of the metadata
        is_sensitive (bool): _description_

    Returns:
        MTMetadata: _description_
    """
    return MTMetadata(
        ro_crate_id=name,
        name=name,
        value=value,
        mt_type=metadata_type,
        sensitive=is_sensitive,
    )


class CrateManifest:
    """Manifest for storing all dataclasses that will be built into and RO-Crate"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        projcets: Optional[List[Project]] = None,
        experiments: Optional[List[Experiment]] = None,
        datasets: Optional[List[Dataset]] = None,
        datafiles: Optional[List[Datafile]] = None,
        people: Optional[List[Person]] = None,
        organizations: Optional[List[Organisation]] = None,
        participants: Optional[Dict[str, Participant]] = None,
    ):
        self.projcets = projcets or []
        self.experiments = experiments or []
        self.datasets = datasets or []
        self.datafiles = datafiles or []
        self.people = people or []
        self.organizations = organizations or []
        self.participants = participants or {}

    def add_projects(self, projcets: List[Project]) -> None:
        self.projcets.extend(projcets)

    def add_experiments(self, experiments: List[Experiment]) -> None:
        self.experiments.extend(experiments)

    def add_datasets(self, datasets: List[Dataset]) -> None:
        self.datasets.extend(datasets)

    def add_datafiles(self, datafiles: List[Datafile]) -> None:
        self.datafiles.extend(datafiles)

    def add_people(self, people: List[Person]) -> None:
        self.people.extend(people)

    def add_organizations(self, organizations: List[Organisation]) -> None:
        self.organizations.extend(organizations)

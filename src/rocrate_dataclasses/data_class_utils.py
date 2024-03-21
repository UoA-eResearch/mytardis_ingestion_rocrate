# pylint: disable=missing-function-docstring
"""Helper functions and classes for managing and moving RO-Crate ingestible dataclasses.
"""

import copy
from typing import Dict, List, Optional

from src.rocrate_dataclasses.rocrate_dataclasses import (
    Datafile,
    Dataset,
    Experiment,
    Project,
)

# def create_metadata_(
#     name: str, value: str, is_sensitive: bool, metadata_type: str
# ) -> MTMetadata:
#     """Construct a metadata dataclass given the name of the metadata from an input file

#     Args:
#         name (str): name of the metadata in MyTardis
#         value (str): the value of the metadata
#         is_sensitive (bool): _description_

#     Returns:
#         MTMetadata: _description_
#     """
#     return MTMetadata(
#         ro_crate_id=name,
#         name=name,
#         value=value,
#         mt_type=metadata_type,
#         sensitive=is_sensitive,
#         parents=parents,
#     )


class CrateManifest:
    """Manifest for storing all dataclasses that will be built into and RO-Crate"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        projcets: Optional[Dict[str, Project]] = None,
        experiments: Optional[Dict[str, Experiment]] = None,
        datasets: Optional[List[Dataset]] = None,
        datafiles: Optional[List[Datafile]] = None,
        # people: Optional[List[Person]] = None,
        # organizations: Optional[List[Organisation]] = None,
        # participants: Optional[Dict[str, Participant]] = None,
    ):
        self.projcets = projcets or {}
        self.experiments = experiments or {}
        self.datasets = datasets or []
        self.datafiles = datafiles or []
        # self.people = people or []
        # self.organizations = organizations or []
        # self.participants = participants or {}

    def add_projects(self, projcets: Dict[str, Project]) -> None:
        self.projcets = self.projcets | projcets

    def add_experiments(self, experiments: Dict[str, Experiment]) -> None:
        self.experiments = self.experiments | experiments

    def add_datasets(self, datasets: List[Dataset]) -> None:
        self.datasets.extend(datasets)

    def add_datafiles(self, datafiles: List[Datafile]) -> None:
        self.datafiles.extend(datafiles)

    # def add_people(self, people: List[Person]) -> None:
    #     self.people.extend(people)

    # def add_organizations(self, organizations: List[Organisation]) -> None:
    #     self.organizations.extend(organizations)


def reduce_to_dataset(in_manifest: CrateManifest, dataset: Dataset) -> CrateManifest:
    dataset = copy.deepcopy(dataset)
    project_ids: set[str] = set()
    out_experiments = {}
    for experiment_id in dataset.experiments:
        if out_experiment := in_manifest.experiments.get(experiment_id):
            out_experiments[experiment_id] = out_experiment
            project_ids.update(out_experiment.projects)
    out_projects = {
        project_id: in_manifest.projcets[project_id]
        for project_id in project_ids
        if in_manifest.projcets.get(project_id)
    }

    out_files = [
        copy.deepcopy(datafile)
        for datafile in in_manifest.datafiles
        if datafile.dataset == dataset.directory
    ]
    _ = [df.update_to_root(dataset) for df in out_files]

    # dataset.update_path(Path("./"))
    return CrateManifest(
        projcets=out_projects,
        experiments=out_experiments,
        datasets=[dataset],
        datafiles=out_files,
    )

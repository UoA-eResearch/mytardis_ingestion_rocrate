# pylint: disable=missing-function-docstring
"""Helper functions and classes for managing and moving RO-Crate ingestible dataclasses.
"""

import copy
from typing import Any, Dict, List, Optional

from slugify import slugify

from src.rocrate_dataclasses.rocrate_dataclasses import (
    Datafile,
    Dataset,
    Experiment,
    Project,
)


class CrateManifest:
    """Manifest for storing all dataclasses that will be built into and RO-Crate"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        projcets: Optional[Dict[str, Project]] = None,
        experiments: Optional[Dict[str, Experiment]] = None,
        datasets: Optional[List[Dataset]] = None,
        datafiles: Optional[List[Datafile]] = None,
    ):
        self.projcets = projcets or {}
        self.experiments = experiments or {}
        self.datasets = datasets or []
        self.datafiles = datafiles or []

    def add_projects(self, projcets: Dict[str, Project]) -> None:
        self.projcets = self.projcets | projcets

    def add_experiments(self, experiments: Dict[str, Experiment]) -> None:
        self.experiments = self.experiments | experiments

    def add_datasets(self, datasets: List[Dataset]) -> None:
        self.datasets.extend(datasets)

    def add_datafiles(self, datafiles: List[Datafile]) -> None:
        self.datafiles.extend(datafiles)


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


def convert_to_property_value(
    json_element: Dict[str, Any] | Any, name: str
) -> Dict[str, Any]:
    """convert a json element into property values for compliance with RO-Crate

    Args:
        json_element (Dict[str, Any] | Any): the json to turn into a Property value
        name (str): the name for the partent json

    Returns:
        Dict[str, Any]: the input as a property value
    """
    if not isinstance(json_element, Dict) and not isinstance(json_element, List):
        return {"@type": "PropertyValue", "name": name, "value": json_element}
    if isinstance(json_element, List):
        return {
            "@type": "PropertyValue",
            "name": name,
            "value": [
                convert_to_property_value(item, slugify(f"{name}-{index}"))
                for index, item in enumerate(json_element)
            ],
        }
    json_element["@type"] = "PropertyValue"
    json_element["name"] = name
    for key, value in json_element.items():
        if isinstance(value, (Dict, List)):
            json_element[key] = convert_to_property_value(value, key)
    return json_element

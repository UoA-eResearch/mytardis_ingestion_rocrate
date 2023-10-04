"""A class that reads in a dataset, experiment and project JSON file and parses
them into data classes that can be used for creating an RO-crate"""

import json
from pathlib import Path
from typing import Dict, List

from slugify import slugify

from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Project,
)
from ro_crate_abi_music.src.user_lookup.user_lookup import create_person_object


def combine_json_files(
    files: List[Path],
) -> Dict[str, str | List[str] | Dict[str, str]]:
    """Take a list of file paths for JSON files and combine these into a single dictionary
    to return.

    Note no error handling is done by this function. It is important to ensure that the input
    files are JSON files

    Args:
        files (List[Path]): A list of Path objects to JSON files, typically a dataset, experiment
            and project JSON
    """
    return_dict = {}
    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as json_file:
            return_dict.update(json.load(json_file))
    return return_dict


def process_project(json_dict: Dict[str, str | List[str] | Dict[str, str]]) -> Project:
    """Extract the project specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str,str | List[str] | Dict[str,str]]): The dictionary that
            contains the project necessary information

    Returns:
        Project: A project dataclass
    """
    principal_investigator = create_person_object(json_dict["principal_investigator"])
    contributors = [
        create_person_object(contributor)
        for contributor in json_dict["contributors"]
        if json_dict["contributors"]
    ]
    identifiers = [slugify(identifier) for identifier in json_dict["project_ids"]]
    return Project(
        name=json_dict["project_name"],
        description=json_dict["project_description"],
        principal_investigator=principal_investigator,
        contributors=contributors or None,
        identifiers=identifiers,
    )


def process_experiment(
    json_dict: Dict[str, str | List[str] | Dict[str, str]]
) -> Experiment:
    """Extract the experiment specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            conatins the experiment necessary information

    Returns:
        Experiment: An experiment dataclass
    """
    identifiers = [
        slugify(f'{json_dict["project_ids"][0]}-{identifier}')
        for identifier in json_dict["experiment_ids"]
    ]
    return Experiment(
        name=json_dict["experiment_name"],
        description=json_dict["experiment_description"],
        project=slugify(json_dict["project"]),
        identifiers=identifiers,
    )


def process_dataset(json_dict: Dict[str, str | List[str] | Dict[str, str]]) -> Dataset:
    """Extract the dataset specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            conatins the dataset necessary information

    Returns:
        Dataset: An dataset dataclass
    """
    identifiers = [
        slugify(
            (
                f'{json_dict["project_ids"][0]}-{json_dict["experiment_ids"][0]}-'
                f'{json_dict["Basename"]["Sequence"]}'
            )
        ),
        json_dict["SequenceID"],
    ]
    metadata = {
        "Camera Settings - Fan": json_dict["Camera Settings"]["Fan"],
        "Camera Settings - Pixel Correction": json_dict["Camera Settings"][
            "Pixel Correction"
        ],
        "Camera Settings - Cooling": json_dict["Camera Settings"]["Cooling"],
    }
    return Dataset(
        name=json_dict["Basename"]["Sequence"],
        description=json_dict["Description"],
        identifiers=identifiers,
        experiment=slugify(
            f'{json_dict["project_ids"][0]}-{json_dict["experiment_ids"][0]}'
        ),
        metadata=metadata,
    )

"""A class that reads in a dataset, experiment and project JSON file and parses
them into data classes that can be used for creating an RO-crate"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from slugify import slugify

from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Project,
)
from ro_crate_abi_music.src.user_lookup.user_lookup import create_person_object


def __parse_datestring(datestring: str) -> datetime:
    """Parse a session datetime string into a Python datetime object

    Args:
        datestring (str): The datetime string from the dataset JSON file

    Returns:
        datetime: The Python datetime object
    """
    return datetime.strptime(datestring, "%y%m%d-%H%M%S")


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
        metadata=None,
        created_date=None,
        updated_dates=None,
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
        metadata=None,
        created_date=None,
        updated_dates=None,
    )


def process_dataset(
    json_dict: Dict[str, str | List[str] | Dict[str, str]], dataset_dir: Path
) -> Dataset:
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
    updated_dates = []
    for index, session in enumerate(json_dict["Sessions"]):
        if index == 0:
            created_date = __parse_datestring(session["Session"])
        else:
            updated_dates.append(__parse_datestring(session["Session"]))

    return Dataset(
        name=json_dict["Basename"]["Sequence"],
        description=json_dict["Description"],
        identifiers=identifiers,
        experiment=slugify(
            f'{json_dict["project_ids"][0]}-{json_dict["experiment_ids"][0]}'
        ),
        directory=dataset_dir,
        created_date=created_date,
        updated_dates=updated_dates or None,
        metadata=None,
    )

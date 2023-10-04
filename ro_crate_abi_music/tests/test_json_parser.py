"""Unit tests for JSON parsing functions
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=missing-class-docstring

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest import mock

import pytest

import ro_crate_abi_music
from ro_crate_abi_music.src.constants.organisatons import UOA
from ro_crate_abi_music.src.json_parser.abi_json_parser import (
    combine_json_files,
    process_dataset,
    process_experiment,
    process_project,
)
from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Person,
    Project,
)

PROJECT_JSON = (
    '{"project_name": "Test Project","project_description": "A sample project '
    'for test purposes", "principal_investigator": "csea004","project_ids": '
    '["test-project"],"contributors":["csea004"]}'
)

EXPERIMENT_JSON = (
    '{"experiment_name": "The sample name", "experiment_description": "A short description '
    'of what the sample is", "project": "test_project", "experiment_ids": '
    '["test-experiment"]}'
)

DATASET_JSON = (
    '{"Basename":{"Project":"Atrial-Innerv","Sample":"SHR-220121","Sequence":"Ganglia561"},'
    '"Description":"Atrial Innervation: SHR-220121. R2 (1.485), Oil:1.486, CC:1.485/2.07. '
    '0.5x0.5x0.5um. 50us x2 scaling. Synapsin1 561@30%.","SequenceID":4126026792660213688,'
    '"Camera Settings":{"Fan":false,"Pixel Correction":false,"Cooling":false},'
    '"Offsets":{"Offset Image":"C:\\\\Users\\\\gsan005\\\\Test\\\\Sensor\\\\20190521b\\\\Fan Off Pixel '
    'Off\\\\Dark_8_050_mean.png","SQRT Offset":100,"Loaded?":true},"Sessions":[{"Session":'
    '"220228-103800","SessionID":4126026792660213688,"Exposure (µs)":48.7143, "XScan":{'
    '"X Start":0.0115,"X Increment":-4.180000E-4,"X Steps":8},"YScan":{"Image Start":-0.0145,'
    '"Image End":-0.0115,"Pixel Size":2.332500E-7,"Frame Rate":20525.45,"Overrun":0.0015,'
    '"Scale Factor":2},"ZScan":{"Z Start":0.0086,"Z Increment":4.665000E-7,"Z Steps":3072},'
    '"Planes":{"First":0,"Last":3071},"Laser Mode":false,"Laser Sequence":["561"],"Scan Mode"'
    ":true}]}"
)


class MockOpen:
    builtin_open = open

    def open(self, *args, **kwargs):  # type: ignore
        if args[0] == Path("project.json"):
            return mock.mock_open(read_data=PROJECT_JSON)(*args, **kwargs)
        elif args[0] == Path("experiment.json"):
            return mock.mock_open(read_data=EXPERIMENT_JSON)(*args, **kwargs)
        elif args[0] == Path("dataset.json"):
            return mock.mock_open(read_data=DATASET_JSON)(*args, **kwargs)


@pytest.fixture
def project_dict() -> Dict[str, Any]:
    return {
        "project_name": "Test Project",
        "project_description": "A sample project for test purposes",
        "principal_investigator": "csea004",
        "project_ids": ["test-project"],
        "contributors": ["csea004"],
    }


@pytest.fixture
def experiment_dict() -> Dict[str, Any]:
    return {
        "experiment_name": "The sample name",
        "experiment_description": "A short description of what the sample is",
        "project": "test_project",
        "experiment_ids": ["test-experiment"],
    }


@pytest.fixture
def dataset_dict() -> Dict[str, Any]:
    return {
        "Basename": {
            "Project": "Atrial-Innerv",
            "Sample": "SHR-220121",
            "Sequence": "Ganglia561",
        },
        "Description": (
            "Atrial Innervation: SHR-220121. R2 (1.485), Oil:1.486, CC:1.485/2.07. 0.5x0.5x0.5um. "
            "50us x2 scaling. Synapsin1 561@30%."
        ),
        "SequenceID": 4126026792660213688,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
        "Offsets": {
            "Offset Image": (
                "C:\\Users\\gsan005\\Test\\Sensor\\20190521b\\Fan Off "
                "Pixel Off\\Dark_8_050_mean.png"
            ),
            "SQRT Offset": 100,
            "Loaded?": True,
        },
        "Sessions": [
            {
                "Session": "220228-103800",
                "SessionID": 4126026792660213688,
                "Exposure (µs)": 48.7143,
                "XScan": {"X Start": 0.0115, "X Increment": -4.180000e-4, "X Steps": 8},
                "YScan": {
                    "Image Start": -0.0145,
                    "Image End": -0.0115,
                    "Pixel Size": 2.332500e-7,
                    "Frame Rate": 20525.45,
                    "Overrun": 0.0015,
                    "Scale Factor": 2,
                },
                "ZScan": {
                    "Z Start": 0.0086,
                    "Z Increment": 4.665000e-7,
                    "Z Steps": 3072,
                },
                "Planes": {"First": 0, "Last": 3071},
                "Laser Mode": False,
                "Laser Sequence": ["561"],
                "Scan Mode": True,
            }
        ],
    }


@pytest.fixture
def file_list() -> List[Path]:
    return [Path("project.json"), Path("experiment.json"), Path("dataset.json")]


@pytest.fixture
def ldap_details() -> Person:
    return Person(name="Test Person", email="test@email.com", affliation=UOA)


@pytest.fixture
def ldap_return() -> Tuple[str, str, str]:
    return ("Test", "Person", "test@email.com")


@pytest.mark.parametrize(
    "filepaths,expected_value",
    [
        (Path("project.json"), PROJECT_JSON),
        (Path("experiment.json"), EXPERIMENT_JSON),
        (Path("dataset.json"), DATASET_JSON),
    ],
)
@mock.patch("builtins.open", MockOpen().open)
def test_mock(filepaths: str, expected_value: str) -> None:
    with open(filepaths, "r", encoding="utf-8") as test_file:
        assert test_file.read() == expected_value


@mock.patch("builtins.open", MockOpen().open)
def test_combine_json_files(
    project_dict: Dict[str, Any],
    experiment_dict: Dict[str, Any],
    dataset_dict: Dict[str, Any],
    file_list: List[Path],
) -> None:
    expected_value = project_dict
    expected_value.update(experiment_dict)
    expected_value.update(dataset_dict)
    assert combine_json_files(file_list) == expected_value


@mock.patch("ro_crate_abi_music.src.user_lookup.user_lookup.lookup_user")
def test_process_project(
    mock_create_person_object: mock.Mock,
    project_dict: Dict[str, str | List[str] | Dict[str, str]],
    experiment_dict: Dict[str, str | List[str] | Dict[str, str]],
    dataset_dict: Dict[str, str | List[str] | Dict[str, str]],
    ldap_details: Person,
    ldap_return: Tuple[str, str, str],
) -> None:
    json_dict = project_dict
    json_dict.update(experiment_dict)
    json_dict.update(dataset_dict)
    mock_create_person_object.return_value = ldap_return
    assert process_project(json_dict) == Project(
        name="Test Project",
        description="A sample project for test purposes",
        principal_investigator=ldap_details,
        contributors=[ldap_details],
        identifiers=["test-project"],
    )


def test_process_experiment(
    project_dict: Dict[str, str | List[str] | Dict[str, str]],
    experiment_dict: Dict[str, str | List[str] | Dict[str, str]],
    dataset_dict: Dict[str, str | List[str] | Dict[str, str]],
) -> None:
    json_dict = project_dict
    json_dict.update(experiment_dict)
    json_dict.update(dataset_dict)
    assert process_experiment(json_dict) == Experiment(
        name="The sample name",
        description="A short description of what the sample is",
        identifiers=["test-project-test-experiment"],
        project="test-project",
    )


def test_process_dataset(
    project_dict: Dict[str, str | List[str] | Dict[str, str]],
    experiment_dict: Dict[str, str | List[str] | Dict[str, str]],
    dataset_dict: Dict[str, str | List[str] | Dict[str, str]],
) -> None:
    json_dict = project_dict
    json_dict.update(experiment_dict)
    json_dict.update(dataset_dict)
    assert process_dataset(json_dict) == Dataset(
        name="Ganglia561",
        description=(
            "Atrial Innervation: SHR-220121. R2 (1.485), Oil:1.486, CC:1.485/2.07. 0.5x0.5x0.5um. "
            "50us x2 scaling. Synapsin1 561@30%."
        ),
        identifiers=["test-project-test-experiment-ganglia561", 4126026792660213688],
        experiment="test-project-test-experiment",
        metadata={
            "Camera Settings - Fan": False,
            "Camera Settings - Pixel Correction": False,
            "Camera Settings - Cooling": False,
        },
    )

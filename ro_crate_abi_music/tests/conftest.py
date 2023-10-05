# pylint: disable=missing-function-docstring

"""Fixtures for use in tests"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

from pytest import fixture
from rocrate.model.contextentity import ContextEntity
from rocrate.model.person import Person as ROPerson
from rocrate.rocrate import ROCrate

from ro_crate_abi_music.src.constants.organisatons import UOA
from ro_crate_abi_music.src.rocrate_builder.rocrate_builder import ROBuilder
from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Person,
    Project,
)


@fixture(name="crate")
def fixture_crate() -> ROCrate:
    return ROCrate()


@fixture(name="builder")
def fixture_builder(crate: ROCrate) -> ROBuilder:
    return ROBuilder(crate)


@fixture(name="project_dict")
def fixture_project_dict() -> Dict[str, Any]:
    return {
        "project_name": "Test Project",
        "project_description": "A sample project for test purposes",
        "principal_investigator": "test001",
        "project_ids": ["test-project", "test-raid", "another-id"],
        "contributors": ["test001", "test001"],
    }


@fixture(name="experiment_dict")
def fixture_experiment_dict() -> Dict[str, Any]:
    return {
        "experiment_name": "The sample name",
        "experiment_description": "A short description of what the sample is",
        "project": "test_project",
        "experiment_ids": ["test-experiment"],
    }


@fixture(name="dataset_dict")
def fixture_dataset_dict() -> Dict[str, Any]:
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


@fixture(name="file_list")
def fixture_file_list() -> List[Path]:
    return [
        Path("project.json"),
        Path("experiment.json"),
        Path("dataset.json"),
    ]


@fixture(name="test_person")
def fixture_test_person() -> Person:
    return Person(
        name="Test Person",
        email="test@email.com",
        affiliation=UOA,
        identifiers=["test001"],
    )


@fixture(name="ldap_return")
def fixture_ldap_return() -> Tuple[str, str, str]:
    return ("Test", "Person", "test@email.com")


@fixture(name="test_project")
def fixture_test_project(test_person: Person) -> Project:
    return Project(
        name="Test Project",
        description="A sample project for test purposes",
        principal_investigator=test_person,
        contributors=[test_person, test_person],
        identifiers=["test-project", "test-raid", "another-id"],
    )


@fixture(name="test_experiment")
def fixture_test_experiment() -> Experiment:
    return Experiment(
        name="The sample name",
        description="A short description of what the sample is",
        identifiers=["test-project-test-experiment"],
        project="test-project",
    )


@fixture(name="test_dataset")
def fixture_test_dataset() -> Dataset:
    return Dataset(
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
        directory=Path("/a/test/dir"),
    )


@fixture(name="test_rocrate_person")
def fixture_test_rocrate_person(builder: ROBuilder) -> ROPerson:
    return ROPerson(
        builder.crate,
        identifier="test001",
        properties={
            "name": "Test Person",
            "email": "test@email.com",
            "affiliation": "https://ror.org/03b94tp07",
        },
    )


@fixture(name="test_rocrate_experiment")
def fixture_test_rocrate_experiment(builder: ROBuilder) -> ContextEntity:
    return ContextEntity(
        builder.crate,
        "test-project-test-experiment",
        properties={
            "@type": "DataCatalog",
            "name": "The sample name",
            "description": "A short description of what the sample is",
            "project": "test-project",
        },
    )

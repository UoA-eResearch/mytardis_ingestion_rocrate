# pylint: disable=missing-function-docstring
"""Tests for the RO Builder classs"""

from rocrate.model.contextentity import ContextEntity
from rocrate.model.data_entity import DataEntity
from rocrate.model.person import Person as ROPerson

from ro_crate_abi_music.src.rocrate_builder.rocrate_builder import ROBuilder
from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Person,
    Project,
)


def test_add_principal_investigator(
    builder: ROBuilder,
    test_person: Person,
    test_rocrate_person: ROPerson,
) -> None:
    assert builder.add_principal_investigator(test_person) == test_rocrate_person


def test_add_contributors(
    builder: ROBuilder,
    test_person: Person,
    test_rocrate_person: ROPerson,
) -> None:
    assert builder.add_contributors([test_person, test_person]) == [
        test_rocrate_person,
        test_rocrate_person,
    ]


def test_add_project(
    builder: ROBuilder,
    test_rocrate_person: ROPerson,
    test_project: Project,
) -> None:
    assert builder.add_project(test_project) == ContextEntity(
        builder.crate,
        "test-project",
        properties={
            "@type": "Project",
            "name": "Test Project",
            "description": "A sample project for test purposes",
            "principal_investigator": test_rocrate_person.id,
            "contributors": [test_rocrate_person.id, test_rocrate_person.id],
            "identifiers": ["test-raid", "another-id"],
        },
    )


def test_add_experiment(
    builder: ROBuilder,
    test_experiment: Experiment,
    test_rocrate_experiment: ContextEntity,
) -> None:
    assert builder.add_experiment(test_experiment) == test_rocrate_experiment


def test_add_dataset(
    builder: ROBuilder,
    test_dataset: Dataset,
    test_rocrate_experiment: ContextEntity,
) -> None:
    print(test_dataset)
    expected_dataset = builder.add_dataset(test_dataset, test_rocrate_experiment)
    expected_value = DataEntity(
        builder.crate,
        identifier=f"{test_dataset.directory.relative_to(test_dataset.directory.parent)}/",
        properties={
            "@type": "Dataset",
            "name": test_dataset.name,
            "description": test_dataset.description,
            "identifiers": [
                "test-project-test-experiment-ganglia561",
                4126026792660213688,
            ],
            "Camera Settings - Fan": False,
            "Camera Settings - Pixel Correction": False,
            "Camera Settings - Cooling": False,
            "includedInDataCatalog": test_rocrate_experiment.id,
        },
    )
    assert builder.add_dataset(test_dataset, test_rocrate_experiment) == DataEntity(
        builder.crate,
        identifier=f"{test_dataset.directory.relative_to(test_dataset.directory.parent)}/",
        properties={
            "@type": "Dataset",
            "name": test_dataset.name,
            "description": test_dataset.description,
            "identifiers": [
                "test-project-test-experiment-ganglia561",
                4126026792660213688,
            ],
            "Camera Settings - Fan": False,
            "Camera Settings - Pixel Correction": False,
            "Camera Settings - Cooling": False,
            "includedInDataCatalog": test_rocrate_experiment.id,
        },
    )

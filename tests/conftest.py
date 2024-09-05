# pylint: disable = missing-function-docstring, redefined-outer-name
"""Conftest for RO-Crate format data extractors"""
import random
from typing import Dict, List

import pandas as pd
from faker import Faker
from pytest import fixture
from rocrate.rocrate import ROCrate

from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)


@fixture
def test_print_lab_builder() -> PrintLabROBuilder:
    crate = ROCrate()
    return PrintLabROBuilder(crate)


@fixture
def test_ingestion_project() -> pd.DataFrame:
    projects_data: Dict[str, list[str | None]] = {
        "Project name": ["test_project_name", "other_test_project_name"],
        "Project code": ["1234", "0000"],
        "Project PI": ["pmgc006", "gbak002"],
        "Ethics Approval ID": ["aaaa", None],
        "Ethics Approval Designation": ["www.google.com", None],
        "description": ["text", "this is a project"],
    }
    return pd.DataFrame(projects_data)


def fake_upi(faker: Faker) -> str:
    return faker.bothify(text="???###", letters="abcdefghijklmnopqrstuvwxyz")


def fake_icd11(faker: Faker) -> str:
    return faker.bothify(text="#?##.##", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def fake_national_health_index(faker: Faker) -> str | None:
    # Fake both possible formats of NHI numbers
    return random.choice(
        [
            faker.bothify(text="???####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            faker.bothify(text="???##?#", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            None,
        ]
    )


def fake_groups_list(faker: Faker, num_groups: int) -> List[str]:
    "generate a fake comma delimited list of groups"
    return ["_".join(faker.words()) for _ in range(num_groups)]


@fixture(name="n_rows")
def sheet_nrows(faker: Faker) -> int:
    return faker.random_int(1, 100)


@fixture
def faked_users(faker: Faker, n_rows: int) -> pd.DataFrame:
    users_data: Dict[str, list[str | None]] = {
        "UPI": [fake_upi(faker) for _ in range(n_rows)],
        "Identifier": [random.choice([str(faker.binary), None]) for _ in range(n_rows)],
        "Pubkey": [random.choice([str(faker.sha1()), None]) for _ in range(n_rows)],
        "Name": [random.choice([faker.name(), None]) for _ in range(n_rows)],
        "Email": [random.choice([faker.email(), None]) for _ in range(n_rows)],
    }
    return pd.DataFrame(users_data)


@fixture
def faked_groups(faker: Faker, n_rows: int) -> pd.DataFrame:
    groups_data: Dict[str, list[str | None] | list[str]] = {
        "Name": fake_groups_list(faker, n_rows),
        "is_owner": [
            random.choice([str(faker.boolean()), None]) for _ in range(n_rows)
        ],
        "see_sensitive": [
            random.choice([str(faker.boolean()), None]) for _ in range(n_rows)
        ],
        "can_download": [
            random.choice([str(faker.boolean()), None]) for _ in range(n_rows)
        ],
    }
    return pd.DataFrame(groups_data)


@fixture
def faked_samples(
    faker: Faker, n_rows: int, faked_groups: pd.DataFrame
) -> pd.DataFrame:
    available_groups = faked_groups["Name"].to_list()
    samples_data: Dict[str, list[str | None]] = {
        "Unique identifier": [faker.bothify("######") for _ in range(n_rows)],
        "User": [fake_upi(faker) for _ in range(n_rows)],
        "Sample type text": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Sample type code": [
            random.choice([faker.bothify("##"), None]) for _ in range(n_rows)
        ],
        "Disease type text from ICD11": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Disease type ICD11 code": [
            random.choice([fake_icd11(faker), None]) for _ in range(n_rows)
        ],
        "Other sample information": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Sample anatomical site text from ICD11": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Sample anatomical site ICD11 code": [
            random.choice([fake_icd11(faker), None]) for _ in range(n_rows)
        ],
        "Histological diagnosis detail text from ICD11": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Histological diagnosis detail code from ICD11": [
            random.choice([fake_icd11(faker), None]) for _ in range(n_rows)
        ],
        "Tissue processing": [
            random.choice([faker.bothify("?????"), " ".join(faker.words()), None])
            for _ in range(n_rows)
        ],
        "Analyte": [
            random.choice([faker.random_letter(), None]) for _ in range(n_rows)
        ],
        "Portion": [
            random.choice([str(faker.random_int(min=0, max=99)), None])
            for _ in range(n_rows)
        ],
        "Project": [
            faker.bothify(text="??#######", letters="abcdefghijklmnopqrstuvwxyz")
            for _ in range(n_rows)
        ],
        "Participant": [
            faker.bothify(text="?####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            for _ in range(n_rows)
        ],
        "Groups": [
            random.choice(
                [
                    ",".join(
                        random.sample(
                            population=available_groups,
                            k=random.randint(1, len(faked_groups["Name"])),
                        )
                    ),
                    None,
                ]
            )
            for _ in range(n_rows)
        ],
    }

    def _consturct_sample_id(row_index: int) -> str:
        sample_names = [
            samples_data["Unique identifier"][row_index],
            samples_data["Project"][row_index],
            samples_data["Sample type code"][row_index],
            samples_data["Disease type ICD11 code"][row_index],
            samples_data["Analyte"][row_index],
            samples_data["Participant"][row_index],
        ]

        sample_name_list: List[str] = [
            str(sample_element)
            for sample_element in sample_names
            if sample_element is not None
        ]
        return "-".join(sample_name_list) if sample_name_list else ""

    samples_data["Sample name"] = [_consturct_sample_id(i) for i in range(n_rows)]

    return pd.DataFrame(samples_data)


@fixture
def faked_projects(faker: Faker, n_rows: int) -> pd.DataFrame:
    projects_data: Dict[str, list[str | None]] = {
        "Project name": [f"{faker.word()}_{faker.color()}" for _ in range(n_rows)],
        "Project code": [
            faker.bothify(text="??#######", letters="abcdefghijklmnopqrstuvwxyz")
            for _ in range(n_rows)
        ],
        "Project PI": [fake_upi(faker) for _ in range(n_rows)],
        "Ethics Approval ID": [
            random.choice([str(faker.random_number()), None]) for _ in range(n_rows)
        ],
        "Ethics Approval Designation": [
            random.choice([faker.url(), None]) for _ in range(n_rows)
        ],
        "description": [random.choice([faker.sentence(), None]) for _ in range(n_rows)],
    }
    return pd.DataFrame(projects_data)


@fixture
def faked_participants(
    faker: Faker, n_rows: int, faked_samples: pd.DataFrame
) -> pd.DataFrame:
    n_rows = faked_samples.shape[0]
    participants_data: Dict[str, list[str | None]] = {
        "Participant: Code": [
            faker.bothify(text="?####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            for _ in range(n_rows)
        ],
        "Participant aliases": [
            random.choice([str(faker.random_int(min=0, max=99)), None])
            for _ in range(n_rows)
        ],
        "Participant Date of birth": [
            random.choice([str(faker.date_of_birth()), None]) for _ in range(n_rows)
        ],
        "Participant NHI number": [
            fake_national_health_index(faker) for _ in range(n_rows)
        ],
        "Participant Sex": [
            random.choice([faker.passport_gender(), None]) for _ in range(n_rows)
        ],
        "Participant Ethnicity": [
            random.choice([str(faker.country()), None]) for _ in range(n_rows)
        ],
        "Project": [
            faker.bothify(text="??#######", letters="abcdefghijklmnopqrstuvwxyz")
            for _ in range(n_rows)
        ],
    }
    participants_data["Participant: Code"] = [
        faked_samples["Participant"][i] for i in range(n_rows)
    ]
    return pd.DataFrame(participants_data)


@fixture
def faked_datasets(
    faker: Faker, n_rows: int, faked_samples: pd.DataFrame
) -> pd.DataFrame:
    n_rows = faked_samples.shape[0]
    datasets_data: Dict[str, list[str | None]] = {
        "Directory": ["test_data/" for _ in range(n_rows)],
        "Dataset Name": [
            faker.bothify(text="?????? ####### ???##") for _ in range(n_rows)
        ],
        "Analysis platform": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Analysis code": [
            random.choice([faker.random_letter(), None]) for _ in range(n_rows)
        ],
        "Instrument": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
        "Center": [
            random.choice([" ".join(faker.city()), None]) for _ in range(n_rows)
        ],
        "Sample code from Centre": [faker.bothify(text="?###") for _ in range(n_rows)],
        "Crate Children": [str(faker.boolean()) for _ in range(n_rows)],
    }
    datasets_data["Sample"] = [faked_samples["Sample name"][i] for i in range(n_rows)]
    return pd.DataFrame(datasets_data)


@fixture
def faked_datafiles(faker: Faker, n_rows: int) -> pd.DataFrame:
    n_rows = faker.random_int(1, 100)
    datafiles_data: Dict[str, list[str | None]] = {
        "Filepath": [f"test_data/{faker.file_extension}" for _ in range(n_rows)],
        "Dataset": ["test_data/" for _ in range(n_rows)],
        "Dataset Name": [
            faker.bothify(text="?????? ####### ???##") for _ in range(n_rows)
        ],
        "Genome": [
            random.choice([faker.bothify("##??"), None, "NA"]) for _ in range(n_rows)
        ],
        "Description": [
            random.choice([" ".join(faker.words()), None]) for _ in range(n_rows)
        ],
    }
    return pd.DataFrame(datafiles_data)

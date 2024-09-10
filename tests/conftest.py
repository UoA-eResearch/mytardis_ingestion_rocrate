# pylint: disable = missing-function-docstring, redefined-outer-name
"""Conftest for RO-Crate format data extractors"""
import pathlib
import random
import shutil
from typing import Any, Dict, List
from sys import platform
import pandas as pd
import slugify
from faker import Faker
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    MTMetadata,
    Person,
    Project,
    User
)
from pytest import fixture
from rocrate.rocrate import ROCrate

from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
    Participant,
    SampleExperiment,
    ExtractionDataset
)
from src.metadata_extraction.metadata_extraction import MetadataSchema
from src.mt_api.apiconfigs import AuthConfig
from src.mt_api.mt_consts import UOA, MtObject
from datetime import datetime
from gnupg import GPG, GenKey

from rocrate.model import ContextEntity as ROContextEntity

THIS_DIR = pathlib.Path(__file__).absolute().parent
TEST_DATA_NAME = "examples_for_test"

@fixture(name=crate)
def test_ro_crate() -> ROCrate:
    return ROCrate()

@fixture
def test_print_lab_builder(crate) -> PrintLabROBuilder:
    crate = crate
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
            faker.bothify(text="???##?#", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZÆ"),
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
        "Patient Consent Designation": [faker.word() for _ in range(n_rows)],
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


# pytest's default tmpdir returns a py.path object
@fixture
def tmpdir(tmpdir: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(tmpdir)


@fixture
def test_data_dir(tmpdir: pathlib.Path) -> pathlib.Path:
    d = tmpdir / TEST_DATA_NAME
    shutil.copytree(THIS_DIR / TEST_DATA_NAME, d)
    return d


@fixture
def test_print_lab_data(test_data_dir: pathlib.Path) -> pathlib.Path:
    return test_data_dir / "print_lab_test/sampledata.xlsx"


@fixture
def print_lab_project_json() -> Dict[str, Any]:
    return {
        "@id": "#c3a02949-204b-580c-be49-1350e3f1df9d",
        "@type": "Project",
        "contributors": [],
        "description": "network-16",
        "mt_identifiers": ["16", "16"],
        "mytardis_classification": "DataClassification.SENSITIVE",
        "name": "16",
        "principal_investigator": [{"@id": "#5e43439e-820b-56d3-82ea-59015f4ae1a3"}],
    }


@fixture
def print_lab_experiment_json() -> Dict[str, Any]:
    return {
        "@id": "#ebbd7daf-16e8-5c5f-8213-cad4c9078aeb",
        "@type": "DataCatalog",
        "analyate": "D",
        "approved": False,
        "associated_disease": ["#2C10.1", "#XH8E54"],
        "body_location": "#XA8LA4",
        "description": "This sample sequenced in Korea",
        "gender": "Male",
        "name": "00001-016-C0001-01-2C10.1-D-01",
        "participant": "#20ed40c3-c5e9-5ece-b280-c34cb79169b2",
        "project": [{"@id": "#c3a02949-204b-580c-be49-1350e3f1df9d"}],
        "tissue_processing_method": "FFPE",
    }


@fixture
def print_lab_dataset_json() -> Dict[str, Any]:
    return {
        "@id": "BAM/",
        "@type": "Dataset",
        "description": "Bam",
        "includedInDataCatalog": [{"@id": "#ebbd7daf-16e8-5c5f-8213-cad4c9078aeb"}],
        "instrument": [{"@id": "#Macrogen Illumina HiSeq"}],
        "mt_identifiers": ["BAM"],
        "mytardis_classification": "DataClassification.SENSITIVE",
        "name": "Bam",
    }


@fixture
def print_lab_datafile_json() -> Dict[str, Any]:
    return {
        "@id": "BAM/aligned.bam",
        "@type": "File",
        "datafileVersion": 1,
        "dataset": [{"@id": "BAM/"}],
        "description": "alginment data from human genome",
        "mytardis_classification": "DataClassification.SENSITIVE",
        "name": "BAM/aligned.bam",
    }


@fixture
def print_lab_acl_json() -> Dict[str, Any]:
    return {
        "@id": "#b005c3c4-2386-57e1-a0c0-825e2dd4dce4",
        "@type": "DigitalDocumentPermission",
        "grantee": [{"@id": "#ea82c06f-2825-529d-88c0-75d65ad3876d"}],
        "grantee_type": "Audiance",
        "my_tardis_can_download": True,
        "mytardis_owner": False,
        "mytardis_see_sensitive": False,
        "permission_type": "ReadPermission",
        "subjectOf": [{"@id": "#ebbd7daf-16e8-5c5f-8213-cad4c9078aeb"}],
    }


@fixture
def test_schema() -> Dict[str, Any]:
    return {
        "Ethics Approval Designation": {
            "choices": "",
            "comparison_type": 1,
            "data_type": 4,
            "full_name": "Ethics Approval Designation",
            "id": 198,
            "immutable": False,
            "is_searchable": False,
            "name": "Ethics Approval Designation",
            "order": 9999,
            "resource_uri": "/api/v1/parametername/198/",
            "schema": "/api/v1/schema/18/",
            "sensitive": False,
            "units": "",
        },
        "Ethics Approval ID": {
            "choices": "",
            "comparison_type": 1,
            "data_type": 2,
            "full_name": "Ethics Approval ID",
            "id": 197,
            "immutable": False,
            "is_searchable": False,
            "name": "Ethics Approval ID",
            "order": 9999,
            "resource_uri": "/api/v1/parametername/197/",
            "schema": "/api/v1/schema/18/",
            "sensitive": False,
            "units": "",
        },
        "Patient Consent Designation": {
            "choices": "",
            "comparison_type": 1,
            "data_type": 4,
            "full_name": "Patient Consent Designation",
            "id": 199,
            "immutable": False,
            "is_searchable": False,
            "name": "Patient Consent Designation",
            "order": 9999,
            "resource_uri": "/api/v1/parametername/199/",
            "schema": "/api/v1/schema/18/",
            "sensitive": True,
            "units": "",
        },
        "Project code": {
            "choices": "",
            "comparison_type": 1,
            "data_type": 2,
            "full_name": "Project code",
            "id": 196,
            "immutable": False,
            "is_searchable": False,
            "name": "Project code",
            "order": 9999,
            "resource_uri": "/api/v1/parametername/196/",
            "schema": "/api/v1/schema/18/",
            "sensitive": False,
            "units": "",
        },
    }


@fixture
def test_schema_object(
    test_schema: Dict[str, Any], test_schema_namespace: str
) -> MetadataSchema:
    return MetadataSchema(
        schema=test_schema, url=test_schema_namespace, mt_type=MtObject.PROJECT
    )


@fixture
def test_schema_namespace() -> str:
    return "http://print.lab.mockup/project/v1"


@fixture
def test_metadata_response() -> Dict[str, Any]:
    return {
        "meta": {},
        "objects": [
            {
                "parameter_names": [
                    {
                        "choices": "",
                        "comparison_type": 1,
                        "data_type": 4,
                        "full_name": "Ethics Approval Designation",
                        "id": 198,
                        "immutable": False,
                        "is_searchable": False,
                        "name": "Ethics Approval Designation",
                        "order": 9999,
                        "resource_uri": "/api/v1/parametername/198/",
                        "schema": "/api/v1/schema/18/",
                        "sensitive": False,
                        "units": "",
                    },
                    {
                        "choices": "",
                        "comparison_type": 1,
                        "data_type": 2,
                        "full_name": "Ethics Approval ID",
                        "id": 197,
                        "immutable": False,
                        "is_searchable": False,
                        "name": "Ethics Approval ID",
                        "order": 9999,
                        "resource_uri": "/api/v1/parametername/197/",
                        "schema": "/api/v1/schema/18/",
                        "sensitive": False,
                        "units": "",
                    },
                    {
                        "choices": "",
                        "comparison_type": 1,
                        "data_type": 4,
                        "full_name": "Patient Consent Designation",
                        "id": 199,
                        "immutable": False,
                        "is_searchable": False,
                        "name": "Patient Consent Designation",
                        "order": 9999,
                        "resource_uri": "/api/v1/parametername/199/",
                        "schema": "/api/v1/schema/18/",
                        "sensitive": True,
                        "units": "",
                    },
                    {
                        "choices": "",
                        "comparison_type": 1,
                        "data_type": 2,
                        "full_name": "Project code",
                        "id": 196,
                        "immutable": False,
                        "is_searchable": False,
                        "name": "Project code",
                        "order": 9999,
                        "resource_uri": "/api/v1/parametername/196/",
                        "schema": "/api/v1/schema/18/",
                        "sensitive": False,
                        "units": "",
                    },
                ],
            }
        ],
    }


@fixture
def username() -> str:
    return "upi000"


@fixture
def api_key() -> str:
    return "test_api_key"


@fixture
def auth(
    username: str,
    api_key: str,
) -> AuthConfig:
    return AuthConfig(username=username, api_key=api_key)


# @fixture
# def test_metadata_schema() -> MetadataSchema:
#     return MetadataSchema()

#     schema: Dict[str, Dict[str, Any]]
#     url: str
#     mt_type: Optional[MtObject] = None


@fixture
def faked_projects_row(faked_projects: pd.DataFrame) -> pd.Series:
    return faked_projects.iloc[0]


# ID is based on parent so make a consistent parent object for test metadata
@fixture
def test_parent_project(faked_projects_row: pd.Series) -> Project:
    row = faked_projects_row
    return Project(
        name=slugify.slugify(f'{row["Project code"]}'),
        description=slugify.slugify(f'{row["Project name"]}-{row["Project code"]}'),
        principal_investigator=Person(
            name="person", email="email", affiliation=UOA, mt_identifiers=[]
        ),
    )


@fixture
def test_output_metadata(
    faked_projects_row: pd.Series,
    test_schema_namespace: str,
    test_parent_project: Project,
) -> MTMetadata:
    return MTMetadata(
        name="Project code",
        value=faked_projects_row["Project code"],
        mt_type="STRING",
        mt_schema=test_schema_namespace,
        sensitive=False,
        parent=test_parent_project,
        recipients=None,
    )


@fixture
def test_output_sensitive_metadata(
    faked_projects_row: pd.Series,
    test_schema_namespace: str,
    test_parent_project: Project,
) -> MTMetadata:
    return MTMetadata(
        name="Patient Consent Designation",
        value=faked_projects_row["Patient Consent Designation"],
        mt_type="LINK",
        mt_schema=test_schema_namespace,
        sensitive=True,
        parent=test_parent_project,
        recipients=None,
    )


@fixture
def test_icd_11_code() -> str:
    return "ABC.123"


@fixture
def test_icd_11_text() -> str:
    return "Andromeda Strain"


@fixture
def test_icd_11_source() -> str:
    return "Andromeda Galaxy"


@fixture
def test_icd11_condition(
    test_icd_11_code: str, test_icd_11_source: str, test_icd_11_text: str
) -> Dict[str, Any]:
    return {
        "@id": test_icd_11_code,
        "title": {"@value": test_icd_11_text},
        "source": test_icd_11_source,
    }


@fixture
def test_medical_condition(test_icd_11_code: str) -> MedicalCondition:
    return MedicalCondition(
        code=test_icd_11_code,
        code_type="ICD11 code",
        code_text="unfilled code text",
        code_source="unfilled code source",
    )

@fixture
def test_updated_medical_condition(
    test_icd_11_code: str, test_icd_11_source: str, test_icd_11_text: str
) -> MedicalCondition:
    return MedicalCondition(
        code=test_icd_11_code,
        code_type="ICD11 code",
        code_text=test_icd_11_text,
        code_source=test_icd_11_source,
    )

@fixture
def test_RO_crate_medical_conditon(crate:ROCrate, test_medical_condition:MedicalCondition) -> ROContextEntity:
    return ROContextEntity(
        crate,
        identifier=test_medical_condition,
        properties={
            "@type": "MedicalCondition",
            "name": test_medical_condition.code,
            "code_type": test_medical_condition.code_type,
            "code_source": test_medical_condition.code_source,
            "code_text": test_medical_condition.code_text
        }
    )

@fixture
def test_date_of_birth() -> str:
    return datetime.today.isoformat()


@fixture
def test_nhi(faker) -> str:
    return fake_national_health_index(faker)

@fixture
def test_gpg_binary_location() -> str:
    if platform in ["linux", "linux2"]:
        # linux
        return "/usr/bin/gpg"
    elif platform == "darwin":
        # OS X
        return "/opt/homebrew/bin/gpg"
    elif platform == "win32":
        # Windows
        return "C:\\Program Files (x86)\\GnuPG\\bin\\gpg.exe"
    raise NotImplementedError(
        "Unknown OS, please define where the gpg executable binary can be located"
    )
    return ""


@fixture()
def test_gpg_object(test_gpg_binary_location):
    gpg = GPG(test_gpg_binary_location)
    return gpg

@fixture
def test_passphrase():
    return "JosiahCarberry1929/13/09"


@fixture
def test_gpg_key(test_gpg_object: GPG, test_passphrase: str) -> GenKey:
    key_input = test_gpg_object.gen_key_input(
        key_type="RSA",
        key_length=1024,
        Passphrase=test_passphrase,
        key_usage="sign encrypt",
    )
    key = test_gpg_object.gen_key(key_input)
    yield key
    test_gpg_object.delete_keys(key.fingerprint, True, passphrase=test_passphrase)
    test_gpg_object.delete_keys(key.fingerprint, passphrase=test_passphrase)
    


@fixture
def test_user() -> User:
    return User(name="test_user",
    email="test@email.com",
    pubkey_fingerprints=test_gpg_key.fingerprint)

@fixture
def test_participant( test_date_of_birth: str, test_nhi:str) -> Participant:
    return Participant(
        gender="test_gender",
        ethnicity="test_ethnicity",
        project="test_project",
        raw_data= {"ignore_me":"do_not_ingest!"},
        date_of_birth= datetime.today.isoformat(),
        nhi_number=test_nhi,
        recipients= None
    )

# @fixture
# def RO_Participant()
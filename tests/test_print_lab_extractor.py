# pylint: disable = redefined-outer-name, invalid-name, protected-access
"""Tests for the Print Lab data extractor functions """
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from mock import MagicMock
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Facility,
    Instrument,
    Person,
)
from pytest import fixture

from src.ingestion_targets.print_lab_genomics.extractor import PrintLabExtractor
from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
)
from src.mt_api.mt_consts import UOA


def test_faked_participant_extraction(
    faked_participants: pd.DataFrame, test_print_lab_builder: PrintLabROBuilder
) -> None:
    """Test extraction of participant data from a dataframe into RO-Crate
    Args:
        faked_participants (pd.DataFrame): faked dataframe of participant data data
        test_print_lab_builder (PrintLabROBuilder): test RO-Crate builder
    """
    MyTardisRestAgent = MagicMock()
    ICD_11_Api_Agent = MagicMock()
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    participants = extractor._parse_participants(particpant_sheet=faked_participants)
    assert len(participants) == faked_participants.shape[0]
    for participant in participants.values():
        participant_entity = test_print_lab_builder.add_participant(participant)
        assert participant_entity


def test_faked_users_extraction(
    faked_users: pd.DataFrame, test_print_lab_builder: PrintLabROBuilder
) -> None:
    """Test extraction of user data from a dataframe then adding these users to an RO-Crate

    Args:
        faked_users (pd.DataFrame): a dataframe of user data populated by faker
        test_print_lab_builder (PrintLabROBuilder): an RO-Crate builder for Print Lab data
    """
    MyTardisRestAgent = MagicMock()
    ICD_11_Api_Agent = MagicMock()
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    users = extractor._parse_users(users_sheet=faked_users)
    # including mytardis user
    assert len(users) == faked_users.shape[0]
    for user in users:
        user_entity = test_print_lab_builder.add_user(user)
        assert user_entity


def test_faked_dataset_extraction(
    faked_datasets: pd.DataFrame, test_print_lab_builder: PrintLabROBuilder
) -> None:
    """Test extraction of dataset data from a dataframe

    Args:
        faked_datasets (pd.DataFrame): a dataframe of dataset data populated by faker
        test_print_lab_builder (PrintLabROBuilder): an RO-Crate builder for Print Lab data
    """
    MyTardisRestAgent = MagicMock()
    ICD_11_Api_Agent = MagicMock()
    # mock experiment data
    Experiment = MagicMock()
    Experiment.return_value.created_by = None
    Experiment.return_value.roc_id = "#test_mocked experiment"
    Experiment.return_value.id = "test_mocked experiment"
    experiment_dict = {
        faked_datasets["Sample"][i]: Experiment() for i in range(0, len(faked_datasets))
    }
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    datasets = extractor._parse_datasets(
        dataset_sheet=faked_datasets, experiments=experiment_dict
    )
    assert len(datasets) == faked_datasets.shape[0]
    test_print_lab_builder.crate.source = Path("crate_soruce")
    for dataset in datasets.values():
        dataset_entity = test_print_lab_builder.add_dataset(dataset)
        assert dataset_entity
        # datasets only differ from default datasets on crate_children field
        if dataset.copy_unlisted:
            assert (
                Path(dataset_entity.source).as_posix()
                == (dataset_entity.crate.source / dataset.directory).as_posix()
            )


def test_faked_datafie_extraction(
    faked_datafiles: pd.DataFrame, test_print_lab_builder: PrintLabROBuilder
) -> None:
    """Tes extraction of datafile data from a dataframe into MyTardis RO-Crate data

    Args:
        faked_datafiles (pd.DataFrame): a dataframe of datafile content populated by faker
        test_print_lab_builder (PrintLabROBuilder): an RO-Crate builder for Print Lab data
    """
    MyTardisRestAgent = MagicMock()
    ICD_11_Api_Agent = MagicMock()
    Dataset = MagicMock()
    Dataset.return_value.roc_id = "#test_mocked_dataset"
    Dataset.return_value.id = "test_mocked_dataset"
    Dataset.return_value.instrument = Instrument(
        name="test_instrument",
        description="test_instrument desc",
        location=Facility(name="test_facillity", description="test facillity desc"),
    )
    dataset_dict = {
        faked_datafiles["Dataset Name"][i]: Dataset()
        for i in range(0, len(faked_datafiles))
    }
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    datafiles = extractor._parse_datafiles(
        datasets=dataset_dict, files_sheet=faked_datafiles
    )
    assert len(datafiles) == faked_datafiles.shape[0]
    for datafile in datafiles:
        datafile_entity = test_print_lab_builder.add_datafile(datafile)
        assert datafile_entity


@fixture
def mocked_project_class() -> MagicMock:
    """Mock a project so that RO-Crate dereference works

    Returns:
        MagicMock : A mock that functions sufficently well as a projcet
    """
    Project = MagicMock()
    Project.return_value.id = "mocked project"
    Project.return_value.roc_id = "#mocked project"
    Project.return_value.principal_investigator.id = "doctor_hotel"
    Project.return_value.principal_investigator.affiliation.id = "Doctor Hotel's Hotel"
    Project.return_value.institution.id = "Doctor Hotel's Hotel"
    Project.return_value.institution.roc_id = "#Doctor Hotel's Hotel"
    Project.return_value.created_by = None
    return Project


def test_faked_samples_extraction(  # pylint: disable=too-many-locals
    faked_samples: pd.DataFrame,
    faked_groups: pd.DataFrame,
    test_print_lab_builder: PrintLabROBuilder,
    mocked_project_class: MagicMock,
) -> None:
    """Test extraction of sample data (experiments) and groups (acls) into RO-Crate data

    Args:
        faked_samples (pd.DataFrame): a dataframe of samples/experiment data populated by faker
        faked_groups (pd.DataFrame): a dataframe of ACL groups populated by faker
        test_print_lab_builder (PrintLabROBuilder): an RO-Crate builder for Print Lab data

    Returns:
        _type_: _description_
    """
    MyTardisRestAgent = MagicMock()
    ICD_11_Api_Agent = MagicMock()
    ICD_11_Api_Agent.request_token.return_value = ""
    ICD_11_Api_Agent.return_value.request_ICD11_data = None

    def x(medical_condition: MedicalCondition) -> MedicalCondition:
        return medical_condition

    ICD_11_Api_Agent.return_value.update_medial_entity_from_ICD11 = x
    Participant = MagicMock()
    Participant.return_value.id = "doctor_house"
    Participant.return_value.roc_id = "#doctor_house"
    # mock project fields (RO-crate ids can't be mocks)
    Project = mocked_project_class

    parcicipants_dict = {
        faked_samples["Participant"][i]: Participant()
        for i in range(0, len(faked_samples))
    }
    projects_dict = {
        faked_samples["Project"][i]: Project() for i in range(0, len(faked_samples))
    }
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    acls = extractor._index_acls(faked_groups)
    experiments = extractor._parse_experiments(
        experiments_sheet=faked_samples,
        acls=acls,
        particpants_dict=parcicipants_dict,
        projects=projects_dict,
    )
    assert len(experiments) == faked_samples.shape[0]
    for experiment in experiments.values():
        experiment_entity = test_print_lab_builder.add_experiment(experiment)
        assert experiment_entity
    for acl in extractor.collected_acls:
        _ = test_print_lab_builder.add_acl(acl)
        assert test_print_lab_builder.crate.dereference(acl.parent.roc_id) is not None


def test_faked_project_extraction(
    faked_projects: pd.DataFrame, test_print_lab_builder: PrintLabROBuilder
) -> None:
    """Extract data from a projects dataframe into RO-Crate projects

    Args:
        faked_projects (pd.DataFrame): a dataframe populated by faker of project data
        test_print_lab_builder (PrintLabROBuilder): an RO-Crate builder for print lab data
    """
    MyTardisRestAgent = MagicMock()

    def x(s: str) -> Person:
        return Person(
            name=s,
            identifier=s,
            schema_type=s,
            email=s,
            mt_identifiers=s,
            affiliation=UOA,
            full_name=s,
        )

    MyTardisRestAgent.return_value.create_person_object = x
    ICD_11_Api_Agent = MagicMock()
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )

    projects = extractor._parse_projects(projects_sheet=faked_projects)
    assert len(projects) == faked_projects.shape[0]
    for i, _ in enumerate(faked_projects):
        assert (
            projects[faked_projects["Project code"][i]].name
            == faked_projects["Project code"][i]
        )
    for project in projects.values():
        project_entity = test_print_lab_builder.add_project(project=project)
        assert project_entity is not None


def test_full_extraction(  # pylint: disable=too-many-positional-arguments, too-many-locals
    test_print_lab_data: Path,
    test_print_lab_builder: PrintLabROBuilder,
    print_lab_project_json: Dict[str, Any],
    print_lab_experiment_json: Dict[str, Any],
    print_lab_dataset_json: Dict[str, Any],
    print_lab_datafile_json: Dict[str, Any],
    print_lab_acl_json: Dict[str, Any],
) -> None:
    """Test extraction from a spreadsheet into RO-Crate dataclasses

    Args:
        test_print_lab_data (Path): the location of the ingestion data
        test_print_lab_builder (PrintLabROBuilder):
            RO-Crate data for print lab data
        print_lab_project_json (Dict[str, Any]):
        print_lab_experiment_json (Dict[str, Any]):
        print_lab_dataset_json (Dict[str, Any]):
        print_lab_datafile_json (Dict[str, Any]):
        print_lab_acl_json (Dict[str, Any]):
            Expected ro-crate classes from spreadsheet
    Returns:
        _type_: _description_
    """
    MyTardisRestAgent = MagicMock()

    def x(s: str) -> Person:
        return Person(
            name=s,
            identifier=s,
            schema_type=s,
            email=s,
            mt_identifiers=s,
            affiliation=UOA,
            full_name=s,
        )

    MyTardisRestAgent.return_value.create_person_object = x
    ICD_11_Api_Agent = MagicMock()

    def mc(medical_condition: MedicalCondition) -> MedicalCondition:
        return medical_condition

    ICD_11_Api_Agent.return_value.update_medial_entity_from_ICD11 = mc
    extractor = PrintLabExtractor(
        api_agent=MyTardisRestAgent(),
        schemas=None,
        collect_all=False,
        pubkey_fingerprints=None,
        icd_11_agent=ICD_11_Api_Agent(),
    )
    manifest = extractor.extract(test_print_lab_data)
    builder = test_print_lab_builder
    crate_projects = [
        test_print_lab_builder.add_project(project)
        for project in manifest.projects.values()
    ]
    assert crate_projects[0].as_jsonld() == print_lab_project_json
    crate_experiments = [
        builder.add_experiment(experiment)
        for experiment in manifest.experiments.values()
    ]
    assert crate_experiments[0].as_jsonld() == print_lab_experiment_json
    crate_datasets = [
        builder.add_dataset(dataset) for dataset in manifest.datasets.values()
    ]
    assert crate_datasets[0].as_jsonld() == print_lab_dataset_json
    crate_datafiles = [
        builder.add_datafile(datafile) for datafile in manifest.datafiles
    ]
    assert crate_datafiles[0].as_jsonld() == print_lab_datafile_json
    # crate_metadata = [builder.add_metadata(metadata) for metadata in manifest.metadata]
    # assert crate_metadata[0].as_jsonld() == {}
    crate_acls = [builder.add_acl(acl) for acl in manifest.acls]
    assert crate_acls[0].as_jsonld() == print_lab_acl_json

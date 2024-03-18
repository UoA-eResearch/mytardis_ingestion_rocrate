# import json
"""Extractor classes for reading metadata provided as spreadsheets by the Print Genomics Lab
Into dataclasses that can be built into an RO-Crate.
"""

import logging
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

import src.profiles.print_lab_genomics.consts as profile_consts
from src.encryption.encrypt_metadata import Encryptor
from src.metadata_extraction.metadata_extraction import MetadataHanlder
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MtObject
from src.profiles.extractor import Extractor
from src.rocrate_dataclasses.data_class_utils import CrateManifest
from src.rocrate_dataclasses.rocrate_dataclasses import (
    Datafile,
    Dataset,
    Experiment,
    Instrument,
    MedicalCondition,
    Participant,
    Project,
    SampleExperiment,
)
from src.user_lookup.user_lookup import create_person_object
from src.utils.file_utils import is_xslx

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PrintLabExtractor(Extractor):
    """An extractor that takes and XLS datasheet
    as defined by the Print Genomics Lab into a Dataframe
    Encryption of strings occurs during extraction

    Args:
        Extractor (_type_): the extractor base class invoked by the CLI
    """

    api_agent: MyTardisRestAgent

    def __init__(self, options: Dict[str, Any]) -> None:
        self.encryptor = options["encryptor"]
        self.api_agent = options["api_agent"]
        self.metadata_handler = MetadataHanlder(
            self.api_agent, profile_consts.NAMESPACES
        )

    #
    def _contruct_sensitive_dict(
        self, sensitive_feild_names: list[str], encryptor: Encryptor
    ) -> dict[str, Callable[[str], str]]:
        return {
            field_name: encryptor.encrypt_string for field_name in sensitive_feild_names
        }

    def datasheet_to_dataframe(
        self, input_data_source: Any, sheet_name: str, sensitive_fields: List[str]
    ) -> pd.DataFrame:
        """Read the contents of an XLSX datasheet into a pandas dataframe
        invoking the chosen encryptor

        Args:
            input_data_source (Any): path to a valid xlsx file
            sheet_name (str): name of the sheet to parse
            sensitive_fields (List[str]): sensitive fields within the sheet

        Raises:
            ValueError: if invalid or incorrect formatted sheet is provided do not read in

        Returns:
            pd.DataFrame: pandas dataframe of the chosen sheet
        """
        if not isinstance(input_data_source, Path) or not is_xslx(input_data_source):
            raise ValueError("Print lab genomics file must be an excel file")
        sensitive_dict = self._contruct_sensitive_dict(sensitive_fields, self.encryptor)
        worksheet_file = pd.ExcelFile(input_data_source, engine="openpyxl")
        parsed_df: Dict[str, pd.DataFrame] = pd.read_excel(
            worksheet_file,
            engine="openpyxl",
            converters=sensitive_dict,
            sheet_name=sheet_name,
        )
        return parsed_df

    def extract(self, input_data_source: Any) -> CrateManifest:
        def list_sensitive_fields(mt_sensitive: Dict[str, Dict[str, Any]]) -> List[str]:
            return [
                metadata_name
                for metadata_name, metadata_object in mt_sensitive.items()
                if metadata_object.get("sensitive")
            ]

        crate_manifest = CrateManifest()
        metadata_dict = self.metadata_handler.get_metdata_lookup_dict(MtObject.PROJECT)
        sensitive_fields = list_sensitive_fields(metadata_dict)
        project_df = self.datasheet_to_dataframe(
            input_data_source, "Projects", sensitive_fields
        )
        crate_manifest.add_projects(
            projcets=(
                self._parse_projects(
                    projects_sheet=project_df, metadata_obj_schema=metadata_dict
                )
            )
        )

        metadata_dict = self.metadata_handler.get_metdata_lookup_dict(
            MtObject.EXPERIMENT
        )
        sensitive_fields = list_sensitive_fields(metadata_dict)
        participants_df = self.datasheet_to_dataframe(
            input_data_source, "Participants", sensitive_fields
        )
        participants = self._parse_participants(participants_df, metadata_dict)
        experiments_df = self.datasheet_to_dataframe(
            input_data_source=input_data_source,
            sheet_name="Samples",
            sensitive_fields=sensitive_fields,
        )
        experiments = self._parse_experiments(
            experiments_df, participants, metadata_dict
        )
        crate_manifest.add_experiments(experiments)

        metadata_dict = self.metadata_handler.get_metdata_lookup_dict(MtObject.DATASET)
        sensitive_fields = list_sensitive_fields(metadata_dict)
        dataset_df = self.datasheet_to_dataframe(
            input_data_source, "Datasets", sensitive_fields
        )
        crate_manifest.add_datasets(self._parse_datasets(dataset_df, metadata_dict))
        metadata_dict = self.metadata_handler.get_metdata_lookup_dict(MtObject.DATAFILE)
        sensitive_fields = list_sensitive_fields(metadata_dict)
        datafile_df = self.datasheet_to_dataframe(
            input_data_source, "Files", sensitive_fields
        )
        crate_manifest.add_datafiles(self._parse_datafiles(datafile_df, metadata_dict))
        return crate_manifest

    def _parse_projects(
        self,
        projects_sheet: pd.DataFrame,
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> List[Project]:
        def parse_project(row: pd.Series) -> Project:
            metadata_dict = self.metadata_handler.create_metadata_objects(
                row, metadata_obj_schema
            )
            pi = create_person_object(row["Project PI"])
            new_project = Project(
                name=row["Project code"],
                metadata=metadata_dict,
                description=row["Project name"],
                identifiers=[row["Project name"]],
                principal_investigator=pi,
                date_created=None,
                date_modified=None,
                contributors=None,
                mytardis_classification="SENSITIVE",
                ethics_policy=row["Ethics Approval ID"],
                accessibility_control=None,
            )
            return new_project

        projects: List[Project] = projects_sheet.apply(parse_project, axis=1).to_list()
        return projects

    def _parse_experiments(
        self,
        experiments_sheet: pd.DataFrame,
        particpants_dict: Dict[str, Participant],
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> List[Experiment]:
        def parse_experiment(row: pd.Series) -> Experiment:
            metadata_dict = self.metadata_handler.create_metadata_objects(
                row, metadata_obj_schema
            )
            participant = particpants_dict[row["Participant"]]
            new_experiment = SampleExperiment(
                name=row["Sample name"],
                description=row["Other sample information"],
                identifiers=[row["Sample name"]],
                date_created=None,
                date_modified=None,
                contributors=None,
                mytardis_classification="",
                metadata=metadata_dict | participant.metadata,  # type: ignore
                project=row["Project"],
                participant=row["Participant"],
                additional_property=None,
                sex=participant.sex,
                associated_disease=[
                    MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                        name=row["Disease type ICD11 code"],
                        code_text=row["Disease type text from ICD11"],
                        code_type="Disease type ICD11 code",
                        code_source=Path("https://icd.who.int/en"),
                    ),
                    MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                        name=row["Histological diagnosis detail code from ICD11"],
                        code_text=row["Histological diagnosis detail text from ICD11"],
                        code_type="Histological diagnosis detail code from ICD11",
                        code_source=Path("https://icd.who.int/en"),
                    ),
                ],
                body_location=MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                    name=row["Sample anatomical site ICD11 code"],
                    code_text=row["Histological diagnosis detail code from ICD11"],
                    code_type="Sample anatomical site ICD11 code",
                    code_source=Path("https://icd.who.int/en"),
                ),
                tissue_processing_method=row["Tissue processing"],
                analyate=row["Analyte"],
                portion=row["Portion"],
                accessibility_control=None,
                participant_metadata=participant.metadata,
            )
            return new_experiment

        experiments: List[Experiment] = experiments_sheet.apply(
            parse_experiment, axis=1
        ).to_list()
        return experiments

    def _parse_participants(
        self,
        particpant_sheet: pd.DataFrame,
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Participant]:
        def parse_participant(row: pd.Series) -> Participant:
            metadata_dict = self.metadata_handler.create_metadata_objects(
                row, metadata_obj_schema
            )
            new_participant = Participant(
                name=row["Participant: Code"],
                description="",
                identifiers=[row["Participant: Code"], row["Participant aliases"]],
                date_created=None,
                date_modified=None,
                metadata=metadata_dict,
                accessibility_control=None,
                date_of_birth=row["Participant Date of birth"],
                nhi_number=row["Participant NHI number"],
                sex=row["Participant Sex"],
                ethnicity=row["Participant Ethnicity"],
                project=row["Project"],
            )
            return new_participant

        participants_dict = {
            participant_value.name: participant_value
            for participant_value in particpant_sheet.apply(
                parse_participant, axis=1
            ).to_list()
        }
        return participants_dict

    def _parse_datasets(
        self,
        dataset_sheet: pd.DataFrame,
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> List[Dataset]:
        def parse_dataset(row: pd.Series) -> Dataset:
            metadata_dict = self.metadata_handler.create_metadata_objects(
                row, metadata_obj_schema
            )
            new_dataset = Dataset(
                name=row["Dataset Name"],
                description=row["Dataset Name"],
                identifiers=[row["Directory"]],
                date_created=None,
                date_modified=None,
                metadata=metadata_dict,
                accessibility_control=None,
                experiment=row["Sample"],
                directory=Path(row["Directory"]),
                contributors=None,
                instrument=Instrument(
                    name=row["Instrument"],
                    location=row["Center"],
                    identifiers=[str(row["Instrument"])],
                    description="_".join([row["Instrument"], row["Center"]]),
                    date_created=None,
                    date_modified=None,
                    metadata=None,
                    accessibility_control=None,
                ),
            )
            return new_dataset

        datasets: List[Dataset] = dataset_sheet.apply(parse_dataset, axis=1).to_list()
        return datasets

    def _parse_datafiles(
        self, files_sheet: pd.DataFrame, metadata_obj_schema: Dict[str, Dict[str, Any]]
    ) -> List[Datafile]:
        def parse_datafile(row: pd.Series) -> Datafile:
            metadata_dict = self.metadata_handler.create_metadata_objects(
                row, metadata_obj_schema
            )
            new_datafile = Datafile(
                name=Path(row["Filepath"]).name,
                description=row["Description"],
                identifiers=[],
                metadata=metadata_dict,
                date_created=None,
                date_modified=None,
                accessibility_control=None,
                filepath=Path(row["Filepath"]),
                dataset=row["Dataset"],
            )
            return new_datafile

        datafiles: List[Datafile] = files_sheet.apply(parse_datafile, axis=1).to_list()
        return datafiles

# import json
"""Extractor classes for reading metadata provided as spreadsheets by the Print Genomics Lab
Into dataclasses that can be built into an RO-Crate.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from mytardis_rocrate_builder.rocrate_dataclasses.data_class_utils import CrateManifest
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    ACL,
    Datafile,
    Dataset,
    Experiment,
    Instrument,
    Project,
)
from slugify import slugify

import src.ingestion_targets.print_lab_genomics.consts as profile_consts
from src.cli.mytardisconfig import SchemaConfig
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
    Participant,
    SampleExperiment,
    ExtractionDataset
)
from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    create_metadata_objects,
    load_optional_schemas,
)
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MtObject
from src.utils.file_utils import is_xslx

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PrintLabExtractor:
    """An extractor that takes and XLS datasheet
    as defined by the Print Genomics Lab into a Dataframe
    Encryption of strings occurs during extraction
    """

    api_agent: MyTardisRestAgent
    collect_all: bool

    def __init__(
        self,
        api_agent: MyTardisRestAgent,
        schemas: SchemaConfig | None,
        collect_all: bool,
    ) -> None:
        self.api_agent = api_agent
        self.schemas = schemas

        namespaces = profile_consts.NAMESPACES
        namespaces = load_optional_schemas(namespaces=namespaces, schemas=self.schemas)
        self.metadata_handler = MetadataHanlder(self.api_agent, namespaces)
        self.collect_all = collect_all

    #
    # def _contruct_sensitive_dict(
    #     self, sensitive_feild_names: list[str]
    # ) -> dict[str, Callable[[str], str]]:
    #     return {field_name: field_name for field_name in sensitive_feild_names}

    def datasheet_to_dataframe(
        self, input_data_source: Any, sheet_name: str
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
        worksheet_file = pd.ExcelFile(input_data_source, engine="openpyxl")
        parsed_df: Dict[str, pd.DataFrame] = pd.read_excel(
            worksheet_file,
            engine="openpyxl",
            sheet_name=sheet_name,
        )
        return parsed_df

    def _parse_acls(
        self,
        acls_sheet: pd.DataFrame,
    ) -> Dict[str, ACL]:
        def parse_acl(row: pd.Series) -> ACL:
            identifier = slugify(f'{row["Name"]}')
            new_acl = ACL(
                name=identifier,
                description=identifier,
                identifiers=[identifier],
                grantee=row["Name"],
                date_created=None,
                date_modified=None,
                additional_properties=None,
                grantee_type="organization",
                mytardis_see_sensitive=row["see_sensitive"],
                mytardis_can_download=row["can_download"],
                mytardis_owner=row["is_owner"],
                schema_type="DigitalDocumentPermission",
            )
            return new_acl

        acls: Dict[str, ACL] = {
            acl.id: acl for acl in acls_sheet.apply(parse_acl, axis=1).to_list()
        }
        return acls

    def _parse_projects(
        self,
        projects_sheet: pd.DataFrame,
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Project]:
        def parse_project(row: pd.Series) -> Project:
            identifier = slugify(f'{row["Project name"]}-{row["Project code"]}')
            metadata_dict = create_metadata_objects(
                row, metadata_obj_schema, self.collect_all, row["Project name"]
            )
            pi = self.api_agent.create_person_object(row["Project PI"])
            new_project = Project(
                name=identifier,
                metadata=metadata_dict,
                description=row["Project name"],
                identifiers=[slugify(f'{row["Project code"]}'), identifier],
                principal_investigator=pi,
                date_created=None,
                date_modified=None,
                contributors=None,
                additional_properties={},
                schema_type="Project",
                acls=None,
            )
            return new_project

        projects: Dict[str, Project] = {
            project.id: project
            for project in projects_sheet.apply(parse_project, axis=1).to_list()
        }
        return projects

    def _parse_experiments(
        self,
        experiments_sheet: pd.DataFrame,
        particpants_dict: Dict[str, Participant],
        acls_dict: Dict[str, ACL],
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Experiment]:
        def parse_experiment(row: pd.Series) -> Experiment:
            metadata_dict = create_metadata_objects(
                row, metadata_obj_schema, self.collect_all, row["Sample name"]
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
                metadata=metadata_dict | participant.metadata,
                projects=[slugify(f'{row["Project"]}')],
                participant=participant,
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
                participant_metadata=participant.metadata,
                additional_properties={},
                schema_type="DataCatalog",
                acls=[acls_dict[slugify(f"{acl}")] for acl in row["Groups"].split(",")],
            )
            return new_experiment

        experiments: Dict[str, Experiment] = {
            experiment.id: experiment
            for experiment in experiments_sheet.apply(
                parse_experiment, axis=1
            ).to_list()
        }
        return experiments

    def _parse_participants(
        self,
        particpant_sheet: pd.DataFrame,
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dataset]:
        def parse_participant(row: pd.Series) -> Participant:
            metadata_dict = create_metadata_objects(
                row, metadata_obj_schema, self.collect_all, row["Participant: Code"]
            )
            new_participant = Participant(
                name=row["Participant: Code"],
                description="",
                identifiers=[
                    slugify(f'{row["Participant: Code"]}'),
                    row["Participant aliases"],
                ],
                date_created=None,
                date_modified=None,
                metadata=metadata_dict,
                date_of_birth=str(row["Participant Date of birth"]),
                nhi_number=row["Participant NHI number"],
                sex=row["Participant Sex"],
                ethnicity=row["Participant Ethnicity"],
                project=slugify(f'{row["Project"]}'),
                additional_properties={},
                schema_type="Person",
                acls=None,
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
        experiments: Dict[str, Experiment],
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> Dict[str, ExtractionDataset]:
        def parse_dataset(row: pd.Series) -> ExtractionDataset:
            metadata_dict = create_metadata_objects(
                row, metadata_obj_schema, self.collect_all, row["Directory"]
            )
            new_dataset = ExtractionDataset(
                name=row["Dataset Name"],
                description=row["Dataset Name"],
                identifiers=[row["Directory"]],
                date_created=None,
                date_modified=None,
                metadata=metadata_dict,
                experiments=[experiments[row["Sample"]]],
                directory=Path(row["Directory"]),
                contributors=None,
                instrument=Instrument(
                    name=row["Instrument"],
                    location=row["Center"],
                    identifiers=[str(row["Instrument"])],
                    description="_".join([row["Instrument"], row["Center"]]),
                    date_created=None,
                    date_modified=None,
                    additional_properties={},
                    schema_type=None,
                ),
                additional_properties={},
                schema_type="Dataset",
                acls=None,
                copy_unlisted=row["Crate Children"]
            )
            return new_dataset

        datasets = {
            datasets_value.id: datasets_value
            for datasets_value in dataset_sheet.apply(parse_dataset, axis=1).to_list()
        }
        return datasets

    def _parse_datafiles(
        self,
        files_sheet: pd.DataFrame,
        datasets: Dict[str, Dataset],
        metadata_obj_schema: Dict[str, Dict[str, Any]],
    ) -> List[Datafile]:
        def parse_datafile(row: pd.Series) -> Datafile:
            metadata_dict = create_metadata_objects(
                row, metadata_obj_schema, self.collect_all, row["Filepath"]
            )
            new_datafile = Datafile(
                name=Path(row["Filepath"]).name,
                description=row["Description"],
                identifiers=[],
                metadata=metadata_dict,
                date_created=None,
                date_modified=None,
                filepath=Path(row["Filepath"]),
                dataset=datasets[Path(row["Dataset"]).as_posix()],
                additional_properties={},
                schema_type="File",
                acls=None,
            )
            return new_datafile

        datafiles: List[Datafile] = files_sheet.apply(parse_datafile, axis=1).to_list()
        return datafiles

    def extract(self, input_data_source: Any) -> CrateManifest:
        """Extract data from sampledata.xlsx metadata file

        Args:
            input_data_source (Any): sampledata input file

        Returns:
            CrateManifest: manifest of all the contents of the RO-Crate
        """

        # def list_sensitive_fields(mt_sensitive: Dict[str, Dict[str, Any]]) -> List[str]:
        #     return [
        #         metadata_name
        #         for metadata_name, metadata_object in mt_sensitive.items()
        #         if metadata_object.get("sensitive")
        #     ]

        crate_manifest = CrateManifest()

        metadata_dict = self.metadata_handler.get_mtobj_schema(MtObject.PROJECT)
        project_df = self.datasheet_to_dataframe(input_data_source, "Projects")
        crate_manifest.add_projects(
            projcets=(
                self._parse_projects(
                    projects_sheet=project_df, metadata_obj_schema=metadata_dict
                )
            )
        )

        metadata_dict = self.metadata_handler.get_mtobj_schema(MtObject.EXPERIMENT)
        participants_df = self.datasheet_to_dataframe(input_data_source, "Participants")
        participants = self._parse_participants(participants_df, metadata_dict)
        acl_df = self.datasheet_to_dataframe(input_data_source, "Groups")
        acls = self._parse_acls(acl_df)
        experiments_df = self.datasheet_to_dataframe(
            input_data_source=input_data_source,
            sheet_name="Samples",
        )
        experiments = self._parse_experiments(
            experiments_df, participants, acls, metadata_dict
        )

        crate_manifest.add_experiments(experiments)

        metadata_dict = self.metadata_handler.get_mtobj_schema(MtObject.DATASET)
        dataset_df = self.datasheet_to_dataframe(input_data_source, "Datasets")
        datasets = self._parse_datasets(dataset_df, experiments, metadata_dict)
        crate_manifest.add_datasets(datasets)

        metadata_dict = self.metadata_handler.get_mtobj_schema(MtObject.DATAFILE)
        datafile_df = self.datasheet_to_dataframe(input_data_source, "Files")
        crate_manifest.add_datafiles(
            self._parse_datafiles(datafile_df, datasets, metadata_dict)
        )

        return crate_manifest

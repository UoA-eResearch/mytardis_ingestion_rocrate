# import json
"""Extractor classes for reading metadata provided as spreadsheets by the Print Genomics Lab
Into dataclasses that can be built into an RO-Crate.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import CrateManifest
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    MyTardisContextObject,
    ACL,
    Datafile,
    Dataset,
    Experiment,
    Instrument,
    Project,
    Facility,
    MTMetadata,
    Group,
)
from slugify import slugify

import src.ingestion_targets.print_lab_genomics.consts as profile_consts
from src.cli.mytardisconfig import SchemaConfig
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    ExtractionDataset,
    MedicalCondition,
    Participant,
    SampleExperiment,
)
from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    load_optional_schemas
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
    collected_acls: List[ACL] = []
    collected_metadata: Dict[str,MTMetadata] = {}
    
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

    

    def _index_acls(
        self,
        acls_sheet: pd.DataFrame,
    ) -> Dict[str, Dict[str,Any]]:
        return {slugify(f'{row["Name"]}'):row  for row in acls_sheet.to_dict('index').values()}

    def parse_acls(
        self,
        indexed_acls: Dict[str, Dict[str,Any]],
        acls_to_read: list[str],
        parent: MyTardisContextObject
    ) -> List[ACL]:
        def create_acl(row: Dict[str,Any]) -> ACL:
            identifier = slugify(f'{row["Name"]}')
            new_acl = ACL(
                name=identifier,
                grantee=Group(name=row["Name"]),
                grantee_type="Audiance",
                mytardis_see_sensitive=row["see_sensitive"],
                mytardis_can_download=row["can_download"],
                mytardis_owner=row["is_owner"],
                parent=parent
            )
            return new_acl
        acl_list = []
        for acl_id in acls_to_read:
            if acl_data := indexed_acls.get(slugify(f'{acl_id}')):
                acl_list.append(create_acl(acl_data))
        return acl_list

    def _parse_projects(
        self,
        projects_sheet: pd.DataFrame,
    ) -> Dict[str, Project]:
        def parse_project(row: pd.Series) -> Project:
            identifier = slugify(f'{row["Project name"]}-{row["Project code"]}')
            pi = self.api_agent.create_person_object(row["Project PI"])
            new_project = Project(
                name=identifier,
                description=row["Project name"],
                mt_identifiers=[slugify(f'{row["Project code"]}'), identifier],
                principal_investigator=pi,
                date_created=None,
                date_modified=None,
                contributors=None,
                additional_properties={},
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row, mt_object=MtObject.PROJECT, collect_all=self.collect_all, parent=new_project
                )
            self.collected_metadata.update(metadata_dict)
            return new_project

        projects: Dict[str, Project] = {
            project.name: project
            for project in projects_sheet.apply(parse_project, axis=1).to_list()
        }
        return projects

    def _parse_experiments(
        self,
        experiments_sheet: pd.DataFrame,
        particpants_dict: Dict[str, Participant],
        acls: Dict[str,Dict[str, Any]],
    ) -> Dict[str, Experiment]:
        def parse_experiment(row: pd.Series) -> Experiment:
            participant = particpants_dict[row["Participant"]]
            new_experiment = SampleExperiment(
                name=row["Sample name"],
                description=row["Other sample information"],
                mt_identifiers=[row["Sample name"]],
                date_created=None,
                date_modified=None,
                contributors=None,
                mytardis_classification="",
                projects=[slugify(f'{row["Project"]}')],
                participant=participant,
                additional_property=None,
                gender=participant.gender,
                associated_disease=[
                    MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                        code=row["Disease type ICD11 code"],
                        code_text=row["Disease type text from ICD11"],
                        code_type="Disease type ICD11 code",
                        code_source=Path("https://icd.who.int/en"),
                    ),
                    MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                        code=row["Histological diagnosis detail code from ICD11"],
                        code_text=row["Histological diagnosis detail text from ICD11"],
                        code_type="Histological diagnosis detail code from ICD11",
                        code_source=Path("https://icd.who.int/en"),
                    ),
                ],
                body_location=MedicalCondition(  # REPLACE WITH LOOKUPS FOR IDC11
                    code=row["Sample anatomical site ICD11 code"],
                    code_text=row["Histological diagnosis detail code from ICD11"],
                    code_type="Sample anatomical site ICD11 code",
                    code_source=Path("https://icd.who.int/en"),
                ),
                tissue_processing_method=row["Tissue processing"],
                analyate=row["Analyte"],
                portion=row["Portion"],
                additional_properties={},
                schema_type="DataCatalog",
            )
            metadata_raw = participant.raw_data
            metadata_raw.update(row)
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=metadata_raw, mt_object=MtObject.EXPERIMENT, collect_all=self.collect_all, parent=new_experiment
            )
            acl_data = self.parse_acls(acls, str(row["Groups"]).split(),new_experiment)
            self.collected_acls.extend(acl_data)
            self.collected_metadata.update(metadata_dict)
            return new_experiment

        experiments: Dict[str, Experiment] = {
            experiment.name: experiment
            for experiment in experiments_sheet.apply(
                parse_experiment, axis=1
            ).to_list()
        }
        return experiments

    def _parse_participants(
        self,
        particpant_sheet: pd.DataFrame,
    ) -> Dict[str, Dataset]:
        def parse_participant(row: pd.Series) -> Participant:
            new_participant = Participant(
                name=row["Participant: Code"],
                description="",
                mt_identifiers=[
                    slugify(f'{row["Participant: Code"]}'),
                    row["Participant aliases"],
                ],
                date_of_birth=str(row["Participant Date of birth"]),
                nhi_number=row["Participant NHI number"],
                gender=row["Participant gender"],
                ethnicity=row["Participant Ethnicity"],
                project=slugify(f'{row["Project"]}'),
                additional_properties={},
                schema_type="Person",
                raw_data = row
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
    ) -> Dict[str, ExtractionDataset]:
        def parse_dataset(row: pd.Series) -> ExtractionDataset:
            new_dataset = ExtractionDataset(
                name=row["Dataset Name"],
                description=row["Dataset Name"],
                mt_identifiers=[row["Directory"]],
                experiments=[experiments[row["Sample"]]],
                directory=Path(row["Directory"]),
                instrument=Instrument(
                    name=row["Instrument"],
                    location=Facility(
                        name=row["Center"], 
                        description=row["Center"],
                        mt_identifiers=None,
                        manager_group=Group(
                            name="facility manager group"
                        )),
                    description="_".join([row["Instrument"], row["Center"]]),
                    mt_identifiers = None
                ),
                additional_properties={},
                schema_type="Dataset",
                copy_unlisted=row["Crate Children"],
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row, mt_object=MtObject.DATASET, collect_all=self.collect_all, parent=new_dataset
            )
            self.collected_metadata.update(metadata_dict)
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
    ) -> List[Datafile]:
        def parse_datafile(row: pd.Series) -> Datafile:
            new_datafile = Datafile(
                name=Path(row["Filepath"]).name,
                description=row["Description"],
                filepath=Path(row["Filepath"]),
                dataset=datasets[Path(row["Dataset"]).as_posix()],
                additional_properties={},
                mt_identifiers = []
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row, mt_object=MtObject.DATAFILE, collect_all=self.collect_all, parent=new_datafile
            )
            self.collected_metadata.update(metadata_dict)
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
        self.collected_metadata = {}
        self.collected_acls = []
        project_df = self.datasheet_to_dataframe(input_data_source, "Projects")
        crate_manifest.add_projects(
            projcets=(
                self._parse_projects(
                    projects_sheet=project_df
                )
            )
        )

        participants_df = self.datasheet_to_dataframe(input_data_source, "Participants")
        participants = self._parse_participants(participants_df)
        acl_df = self.datasheet_to_dataframe(input_data_source, "Groups")
        acls = self._index_acls(acl_df)
        experiments_df = self.datasheet_to_dataframe(
            input_data_source=input_data_source,
            sheet_name="Samples",
        )
        experiments = self._parse_experiments(
            experiments_df, participants, acls
        )

        crate_manifest.add_experiments(experiments)

        dataset_df = self.datasheet_to_dataframe(input_data_source, "Datasets")
        datasets = self._parse_datasets(dataset_df, experiments)
        crate_manifest.add_datasets(datasets)

        
        datafile_df = self.datasheet_to_dataframe(input_data_source, "Files")
        crate_manifest.add_datafiles(
            self._parse_datafiles(datafile_df, datasets)
        )
        logger.debug("acls collected is %s", self.collected_acls)
        crate_manifest.add_acls(self.collected_acls)
        
        crate_manifest.add_metadata(self.collected_metadata.values())
        return crate_manifest

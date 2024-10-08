# import json
"""Extractor classes for reading metadata provided as spreadsheets by the Print Genomics Lab
Into dataclasses that can be built into an RO-Crate.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import CrateManifest
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    ACL,
    Datafile,
    Dataset,
    Experiment,
    Facility,
    Group,
    Instrument,
    MTMetadata,
    MyTardisContextObject,
    Project,
    User,
)
from slugify import slugify

import src.ingestion_targets.print_lab_genomics.consts as profile_consts
from src.cli.mytardisconfig import SchemaConfig
from src.ingestion_targets.print_lab_genomics.ICD11_API_agent import ICD11ApiAgent
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    ExtractionDataset,
    MedicalCondition,
    Participant,
    SampleExperiment,
)
from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    load_optional_schemas,
)
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MY_TARDIS_USER, UOA, MtObject
from src.utils.file_utils import is_xslx

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
cwd = os.getcwd()


class PrintLabExtractor:  # pylint: disable = too-many-instance-attributes
    """An extractor that takes and XLS datasheet
    as defined by the Print Genomics Lab into a Dataframe
    Encryption of strings occurs during extraction
    """

    api_agent: MyTardisRestAgent
    collect_all: bool
    collected_acls: List[ACL] = []
    collected_metadata: List[MTMetadata] = []
    users: List[User] = [MY_TARDIS_USER]
    icd_11_agent: ICD11ApiAgent

    def __init__(  # pylint: disable=too-many-arguments
        self,
        api_agent: MyTardisRestAgent,
        schemas: SchemaConfig | None,
        collect_all: bool,
        pubkey_fingerprints: Optional[List[str]],
        icd_11_agent: ICD11ApiAgent,
    ) -> None:
        self.api_agent = api_agent
        namespaces = load_optional_schemas(
            namespaces=profile_consts.NAMESPACES, schemas=schemas
        )
        self.metadata_handler = MetadataHanlder(self.api_agent, namespaces)
        self.collect_all = collect_all
        self.icd_11_agent: ICD11ApiAgent = (
            icd_11_agent  # pylint: disable = invalid-name
        )
        if pubkey_fingerprints:
            MY_TARDIS_USER.pubkey_fingerprints = pubkey_fingerprints

    def datasheet_to_dataframes(self, input_data_source: Any) -> pd.DataFrame:
        """Read the contents of an XLSX datasheet into a pandas dataframes

        Args:
            input_data_source (Any): path to a valid xlsx file
            sensitive_fields (List[str]): sensitive fields within the sheet

        Raises:
            ValueError: if invalid or incorrect formatted sheet is provided do not read in

        Returns:
            pd.DataFrame: pandas dataframe of spreadsheet
        """
        if not isinstance(input_data_source, Path) or not is_xslx(input_data_source):
            raise ValueError("Print lab genomics file must be an excel file")
        worksheet_file = pd.ExcelFile(input_data_source, engine="openpyxl")
        parsed_dfs: Dict[str, pd.DataFrame] = pd.read_excel(
            worksheet_file,
            engine="openpyxl",
            sheet_name=None,
        )
        return parsed_dfs

    def _index_acls(
        self,
        acls_sheet: pd.DataFrame,
    ) -> Dict[str, Dict[str, Any]]:
        return {
            slugify(f'{row["Name"]}'): row
            for row in acls_sheet.to_dict("index").values()
        }

    def _parse_acls(
        self,
        indexed_acls: Dict[str, Dict[str, Any]],
        acls_to_read: list[str],
        parent: MyTardisContextObject,
    ) -> List[ACL]:
        """parse access level controls from those found in groups sheet

        Args:
            indexed_acls (Dict[str, Dict[str, Any]]): an indexed set of ACLS
            acls_to_read (list[str]): all access level controls that are to be read
            parent (MyTardisContextObject): parent of this acl

        Returns:
            List[ACL]: all ACLs returned
        """

        def create_acl(row: Dict[str, Any]) -> ACL:
            identifier = slugify(f'{row["Name"]}')
            new_acl = ACL(
                name=identifier,
                grantee=Group(name=row["Name"]),
                grantee_type="Audiance",
                mytardis_see_sensitive=row["see_sensitive"],
                mytardis_can_download=row["can_download"],
                mytardis_owner=row["is_owner"],
                parent=parent,
            )
            return new_acl

        acl_list = []
        for acl_id in acls_to_read:
            if acl_data := indexed_acls.get(slugify(f"{acl_id}")):
                acl_list.append(create_acl(acl_data))
        return acl_list

    def _parse_projects(
        self,
        projects_sheet: pd.DataFrame,
    ) -> Dict[str, Project]:
        def parse_project(row: pd.Series) -> Project:
            identifier = slugify(f'{row["Project code"]}')
            pi = self.api_agent.create_person_object(row["Project PI"])
            new_project = Project(
                name=identifier,
                description=slugify(f'{row["Project name"]}-{row["Project code"]}'),
                mt_identifiers=[slugify(f'{row["Project code"]}'), identifier],
                principal_investigator=pi,
                date_created=None,
                date_modified=None,
                contributors=None,
                additional_properties={},
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row,
                mt_object=MtObject.PROJECT,
                collect_all=self.collect_all,
                parent=new_project,
            )
            self.collected_metadata.extend(metadata_dict.values())
            return new_project

        projects: Dict[str, Project] = {
            project.name: project
            for project in projects_sheet.apply(parse_project, axis=1).to_list()
        }
        return projects

    def _parse_medical_condition(
        self,
        row: pd.Series,
        code_title: str,
        text_title: str,
        code_source: str = "https://icd.who.int/en",
    ) -> None | MedicalCondition:
        if not pd.notna(row[code_title]):
            return None
        condtion = MedicalCondition(
            code=row[code_title],
            code_type=code_title,
            code_source=code_source,
            code_text="",
        )
        condtion = self.icd_11_agent.update_medial_entity_from_ICD11(condtion)
        if condtion.code_text is not None:
            row[text_title] = condtion.code_text
        else:
            condtion.code_text = row[text_title] if pd.notna(row[text_title]) else None
        return condtion

    def _parse_experiments(
        self,
        experiments_sheet: pd.DataFrame,
        particpants_dict: Dict[str, Participant],
        acls: Dict[str, Dict[str, Any]],
        projects: Dict[str, Project],
    ) -> Dict[str, Experiment]:
        def parse_experiment(row: pd.Series) -> Experiment:
            row.dropna()
            participant = particpants_dict[row["Participant"]]
            disease = []
            project_entity = projects.get(slugify(f'{row["Project"]}'))
            if project_entity is None:
                logger.error(
                    "Samples should all have a matching project, no project found for %s",
                    row["Sample name"],
                )
                raise ValueError()
            condition = self._parse_medical_condition(
                row=row,
                code_title="Disease type ICD11 code",
                text_title="Disease type text from ICD11",
            )
            if condition is not None:
                disease.append(condition)
            condition = self._parse_medical_condition(
                row=row,
                code_title="Histological diagnosis detail code from ICD11",
                text_title="Histological diagnosis detail text from ICD11",
            )
            if condition is not None:
                disease.append(condition)
            anatomical_site = self._parse_medical_condition(
                row=row,
                code_title="Sample anatomical site ICD11 code",
                text_title="Sample anatomical site text from ICD11",
            )
            new_experiment = SampleExperiment(
                name=row["Sample name"],
                description=row["Other sample information"],
                mt_identifiers=[row["Sample name"]],
                date_created=None,
                date_modified=None,
                contributors=None,
                mytardis_classification="",
                projects=[project_entity],
                participant=participant,
                additional_property=None,
                gender=participant.gender,
                associated_disease=disease,
                body_location=anatomical_site,
                tissue_processing_method=row["Tissue processing"],
                analyate=row["Analyte"],
                portion=row["Portion"],
                additional_properties={},
                schema_type="DataCatalog",
            )
            metadata = row.to_dict()
            metadata.update(participant.raw_data)
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=metadata,
                mt_object=MtObject.EXPERIMENT,
                collect_all=self.collect_all,
                parent=new_experiment,
            )
            acl_data = self._parse_acls(
                acls, str(row["Groups"]).split(","), new_experiment
            )
            self.collected_acls.extend(acl_data)
            self.collected_metadata.extend(metadata_dict.values())
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
            row.dropna()
            new_participant = Participant(
                name=row["Participant: Code"],
                description="",
                mt_identifiers=[
                    slugify(f'{row["Participant: Code"]}'),
                    row["Participant aliases"],
                ],
                date_of_birth="",
                nhi_number="",
                gender=row["Participant Sex"],
                ethnicity=row["Participant Ethnicity"],
                project=slugify(f'{row["Project"]}'),
                additional_properties={},
                schema_type="Person",
                raw_data=row,
            )
            if self.users:
                new_participant.nhi_number = row["Participant NHI number"]
                new_participant.date_of_birth = str(row["Participant Date of birth"])
                new_participant.recipients = self.users
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
            row.dropna()
            instrument_description = "_".join(
                [
                    component
                    for component in [row["Instrument"], row["Center"]]
                    if component is not None
                ]
            )
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
                        manager_group=Group(name="facility manager group"),
                    ),
                    description=instrument_description,
                    mt_identifiers=None,
                ),
                additional_properties={},
                schema_type="Dataset",
                copy_unlisted=row["Crate Children"],
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row,
                mt_object=MtObject.DATASET,
                collect_all=self.collect_all,
                parent=new_dataset,
            )
            self.collected_metadata.extend(metadata_dict.values())
            return new_dataset

        datasets = {
            datasets_value.name: datasets_value
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
                name=Path(row["Filepath"]),
                description=row["Description"],
                filepath=Path(row["Filepath"]),
                dataset=datasets[row["Dataset Name"]],
                additional_properties={},
                mt_identifiers=[],
            )
            metadata_dict = self.metadata_handler.create_metadata_from_schema(
                input_metadata=row,
                mt_object=MtObject.DATAFILE,
                collect_all=self.collect_all,
                parent=new_datafile,
            )
            self.collected_metadata.extend(metadata_dict.values())
            return new_datafile

        datafiles: List[Datafile] = files_sheet.apply(parse_datafile, axis=1).to_list()
        return datafiles

    def _parse_users(
        self,
        users_sheet: pd.DataFrame,
    ) -> List[User]:
        """Parse users from dataframe into user objects to be added to an RO-Crate

        Args:
            users_sheet (pd.DataFrame): the dataframe for constructing users

        Returns:
            List[User]: users to be added to the dataframe
        """

        def parse_user(row: Dict[str, Any]) -> User:
            identifier = slugify(f'{row["UPI"]}')
            new_user = User(
                identifier=identifier,
                name=row["Name"],
                email=row["Email"],
                affiliation=UOA,
                full_name=row["Name"],
                pubkey_fingerprints=[row["Pubkey"]],
                mt_identifiers=[identifier],
            )
            if pd.notna(row["Identifier"]):
                new_user.mt_identifiers.append(row["Identifier"])
            return new_user

        users: List[User] = users_sheet.apply(parse_user, axis=1).to_list()
        return users

    def extract(self, input_data_source: Any) -> CrateManifest:
        """Extract data from sampledata.xlsx metadata file

        Args:
            input_data_source (Any): sampledata input file

        Returns:
            CrateManifest: manifest of all the contents of the RO-Crate
        """
        crate_manifest = CrateManifest()
        self.collected_metadata = []
        self.collected_acls = []
        self.users = [MY_TARDIS_USER]

        data_df = self.datasheet_to_dataframes(
            input_data_source,
        )
        self.users.extend(self._parse_users(data_df["Users"]))
        projects = self._parse_projects(projects_sheet=data_df["Projects"])
        participants = self._parse_participants(data_df["Participants"])
        acls = self._index_acls(data_df["Groups"])
        experiments = self._parse_experiments(
            data_df["Samples"], participants, acls, projects
        )
        datasets = self._parse_datasets(data_df["Datasets"], experiments)
        crate_manifest.add_projects(
            {project.id: project for project in projects.values()}
        )
        crate_manifest.add_experiments(
            {experiment.id: experiment for experiment in experiments.values()}
        )
        crate_manifest.add_datasets(datasets)
        crate_manifest.add_datafiles(self._parse_datafiles(data_df["Files"], datasets))
        crate_manifest.add_acls(self.collected_acls)
        for metadata in self.collected_metadata:
            if metadata.sensitive:
                metadata.recipients = self.users
        crate_manifest.add_metadata(self.collected_metadata)
        return crate_manifest

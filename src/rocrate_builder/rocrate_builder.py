"""Builder class and functions for translating RO-Crate dataclasses into RO-Crate Entities
"""
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rocrate.model.contextentity import ContextEntity
from rocrate.model.data_entity import DataEntity
from rocrate.model.encryptedcontextentity import (  # pylint: disable=import-error, no-name-in-module
    EncryptedContextEntity,
)
from rocrate.model.person import Person as ROPerson
from rocrate.rocrate import ROCrate

from src.rocrate_dataclasses.rocrate_dataclasses import ACL  # BaseObject,
from src.rocrate_dataclasses.rocrate_dataclasses import (
    ContextObject,
    Datafile,
    Dataset,
    Experiment,
    MTMetadata,
    MyTardisContextObject,
    Organisation,
    Person,
    Project,
)

MT_METADATA_TYPE = "my_tardis_metadata"
logger = logging.getLogger(__name__)

JsonProperties = Dict[str, str | List[str] | Dict[str, Any]]
IdentiferType = str | int | float


class ROBuilder:
    """A class to hold and add entries to an ROCrate

    Attr:
        crate (ROCrate): an RO-Crate to build or modifiy
        metadata_dict (Dict): a dictionary read in from a series of JSON files
    """

    def __init__(
        self, crate: ROCrate, flatten_additional_properties: bool = True
    ) -> None:
        """Initialisation of the ROBuilder

        Args:
            crate (ROCrate): an RO-Crate, either empty or reread in
            metadata_dict (Dict[str, str | List[str] | Dict[str,str]]): A dictionary of metadata
                relating to the entries to be added.
        """
        self.crate = crate
        self.flatten_additional_properties = flatten_additional_properties

    def _add_metadata_to_crate(
        self, metadata_obj: MTMetadata, metadata_id: str, parent_id: IdentiferType
    ) -> None:
        """Add a MyTardis Metadata object to the crate

        Args:
            metadata_obj (MTMetadata): the MyTardis Metadata object
            metadata_id (str): the id used to identify the entity in the crate
        """

        if metadata_obj.sensitive:
            if not self.crate.pubkey_fingerprints:
                return
            metadata = EncryptedContextEntity(
                self.crate,
                metadata_id,
                properties={
                    "@type": MT_METADATA_TYPE,
                    "name": metadata_obj.name,
                    "value": metadata_obj.value,
                    "myTardis-type": metadata_obj.mt_type,
                    "sensitive": metadata_obj.sensitive,
                    "parents": [parent_id],
                },
                pubkey_fingerprints=[],
            )
        else:
            metadata = ContextEntity(
                self.crate,
                metadata_id,
                properties={
                    "@type": MT_METADATA_TYPE,
                    "name": metadata_obj.name,
                    "value": metadata_obj.value,
                    "myTardis-type": metadata_obj.mt_type,
                    "sensitive": metadata_obj.sensitive,
                    "parents": [parent_id],
                },
            )

        self.crate.add(metadata)

    def _add_object_acls(
        self, properties: JsonProperties, acls: List[ACL], partent_id: IdentiferType
    ) -> JsonProperties:
        """add access level controls to an object

        Args:
            properties (Dict[str, str | List[str] | Dict[str,str]]):
                the properties dict of the parent object
            acls (List[ACL]): lsit of access level control objects
            partent_id (str): the id of the parent object

        Returns:
            JsonProperties: the properties updated with the ACLs
        """
        has_digital_permission = []
        for acl in acls:
            acl_entity = self.crate.dereference(acl.id) or self.add_acl(acl)
            acl_entity.append_to("subjectOf", partent_id)
            has_digital_permission.append(acl_entity.id)
        properties["hasDigitalDocumentPermission"] = has_digital_permission
        return properties

    def __add_organisation(self, organisation: Organisation) -> None:
        """Read in an Organisation object and create a Organization entity in the crate

        Args:
            organisation (Organisation): The Organisation to add
        """
        identifier = organisation.id
        org_type = "Organization"
        if organisation.research_org:
            org_type = "ResearchOrganization"
        org = ContextEntity(
            self.crate,
            identifier,
            properties={
                "@type": org_type,
                "name": organisation.name,
            },
        )
        if organisation.url:
            org.append_to("url", organisation.url)
        if len(organisation.identifiers) > 1:
            for index, identifier in enumerate(organisation.identifiers):
                if index != 0:
                    org.append_to("identifier", identifier)
        self.crate.add(org)

    def __add_person_to_crate(self, person: Person) -> ROPerson:
        """Read in a Person object and create an entry for them in the crate.
        Without Active Directory auth this will just default to providing UPI


        Args:
            person (Person): the person to add to the crate
        """
        orcid_regex = re.compile(
            r"https://orcid\.org/[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]{1}"
        )
        upi_regex = re.compile(r"^[a-z]{2,4}[0-9]{3}$")
        person_id = None
        # Check to see if any of the identifiers are orcids - these are preferred
        for identifier in person.identifiers:
            if _ := orcid_regex.fullmatch(identifier):
                person_id = identifier
        # If no orcid is found check for UPIs
        if not person_id:
            for identifier in person.identifiers:
                if _ := upi_regex.fullmatch(identifier):
                    person_id = identifier
        # Finally, if no orcid or UPI is found, use the first identifier
        if not person_id:
            person_id = person.identifiers[0]
        if not any(
            (
                entity.type in ["Organization", "ResearchOrganization"]
                and entity.id == person.affiliation.id
            )
            for entity in self.crate.get_entities()
        ):
            self.__add_organisation(person.affiliation)
        person_obj = ROPerson(
            self.crate,
            person_id,
            properties={
                "name": person.name,
                "email": person.email,
                "affiliation": person.affiliation.id,
            },
        )
        if len(person.identifiers) > 1:
            for identifier in person.identifiers:
                if identifier != person_id:
                    person_obj.append_to("identifier", identifier)
        self.crate.add(person_obj)
        return person_obj

    def add_principal_investigator(self, principal_investigator: Person) -> ROPerson:
        """Read in the principal investigator from the project and create an entry for them
        in the crate

        Args:
            principal_investigator (Person): _description_
        """
        return self.__add_person_to_crate(principal_investigator)

    def add_contributors(self, contributors: List[Person]) -> List[ROPerson]:
        """Add the contributors to a project into the crate

        Args:
            contributors (List[Person]): A list of people to add as contributors
        """
        return [self.__add_person_to_crate(contributor) for contributor in contributors]

    def _add_identifiers(
        self,
        obj_dataclass: ContextObject,
        rocrate_obj: ContextEntity | DataEntity,
    ) -> ContextEntity | DataEntity:
        """Boilerplate code to add identifiers to the RO Crate objects

        Args:
            obj_dataclass (BaseObject): A Project, Experiment or Dataset object
            rocrate_obj (ContextEntity | DataEntity): A RO-Crate object that the
                identifiers are added to

        Returns:
            ContextEntity | DataEntity: The modified RO-Crate object
        """
        if len(obj_dataclass.identifiers) > 1:
            for index, identifier in enumerate(obj_dataclass.identifiers):
                if index != 0:
                    rocrate_obj.append_to("identifiers", identifier)
        self.crate.add(rocrate_obj)
        return rocrate_obj

    def _crate_contains_metadata(self, metadata: MTMetadata) -> ContextEntity | None:
        if crate_metadata := self.crate.dereference(metadata.ro_crate_id):
            if metadata.name == crate_metadata.get(
                "name"
            ) and metadata.value == crate_metadata.get("value"):
                return crate_metadata
        return None

    def _add_metadata(
        self,
        parent_name: IdentiferType,
        properties: JsonProperties,
        metadata: Dict[str, MTMetadata],
    ) -> JsonProperties:
        """Add generic metadata to the properties dictionary for a RO-Crate obj

        Args:
            properties (JsonProperties): The properties to be
                added to the RO-Crate
            metadata (JsonProperties): A dictionary of metadata
                to be added to the RO-Crate obj

        Returns:
            Dict[str, str|List[str]|Dict[str, Any]]: The updated properties dictionary
        """
        properties["metadata"] = []
        for _, metadata_object in metadata.items():
            if metadata_object.parents is None:
                metadata_object.parents = [str(parent_name)]
            metadata_id = "_".join([str(parent_name), metadata_object.name])
            if existing_metadata := self._crate_contains_metadata(metadata_object):
                existing_metadata.append_to("parents", parent_name)
            else:
                self._add_metadata_to_crate(metadata_object, metadata_id, parent_name)
            if metadata_id not in properties["metadata"]:  # pylint: disable=C0201
                properties["metadata"].append(metadata_id)  # type: ignore
        return properties

    def _add_additional_properties(
        self,
        properties: Dict[str, Any],
        additional_properties: Dict[str, Any],
    ) -> JsonProperties:
        entity_properties = properties
        if not self.flatten_additional_properties:
            properties["additonal properties"] = {}
            entity_properties = properties["additonal properties"]
        for key, value in additional_properties.items():
            if isinstance(value, List):
                for index, item in enumerate(value):
                    value[index] = (
                        self.add_context_object(item).id
                        if isinstance(item, ContextObject)
                        else item
                    )
            if isinstance(value, ContextObject):
                value = self.add_context_object(value).id
            if key not in entity_properties.keys():  # pylint: disable=C0201
                entity_properties[key] = value
            elif isinstance(entity_properties[key], list):
                entity_properties[key].append(value)
            else:
                entity_properties[key] = [
                    properties,
                    value,
                ]
        return properties

    def _add_dates(
        self,
        properties: JsonProperties,
        date_created: datetime,
        date_modified: Optional[List[datetime]] = None,
    ) -> JsonProperties:
        """Add dates, where present, to the metadata

        Args:
            properties (Dict[str, str  |  List[str]  |  Dict[str, Any]]): properties of the RO-Crate
            date_created (datetime): created date of of the object
            date_modified (Optional[List[datetime]], optional): last modified date of the object
                Defaults to None.

        Returns:
            JsonProperties: _description_
        """
        properties["dateCreated"] = date_created.isoformat()
        if date_modified:
            properties["dateModified"] = [date.isoformat() for date in date_modified]
        properties["datePublished"] = date_created.isoformat()
        return properties

    def _update_properties(
        self, data_object: MyTardisContextObject, properties: JsonProperties
    ) -> JsonProperties:
        if data_object.metadata:
            properties = self._add_metadata(
                data_object.id, properties, data_object.metadata
            )
        if data_object.acls:
            properties = self._add_object_acls(
                properties, data_object.acls, data_object.id
            )
        if data_object.date_created:
            properties = self._add_dates(
                properties,
                data_object.date_created,
                data_object.date_modified,
            )
        if data_object.additional_properties:
            properties = self._add_additional_properties(
                properties=properties,
                additional_properties=data_object.additional_properties,
            )
        return properties

    def add_project(self, project: Project) -> ContextEntity:
        """Add a project to the RO crate

        Args:
            project (Project): The project to be added to the crate
        """
        principal_investigator = self.add_principal_investigator(
            project.principal_investigator
        )
        contributors = []
        if project.contributors:
            contributors = self.add_contributors(project.contributors)
        properties = {
            "@type": "Project",
            "name": project.name,
            "description": project.description,
            "principal_investigator": principal_investigator.id,
            "contributors": [contributor.id for contributor in contributors],
        }

        properties = self._update_properties(data_object=project, properties=properties)
        project_obj = ContextEntity(
            self.crate,
            project.id,
            properties=properties,
        )

        return self._add_identifiers(project, project_obj)

    def _update_experiment_meta(
        self,
        experiment: Experiment,
        properties: JsonProperties,
    ) -> ContextEntity:
        """Update the metadata for an experiment and create the context entity for the experiment

        Args:
            experiment (Experiment): the experiment object

        Returns:
            ContextEntity: the returned crate context entity
        """
        identifier = experiment.id
        properties = self._update_properties(
            data_object=experiment, properties=properties
        )
        return ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )

    def add_experiment(self, experiment: Experiment) -> ContextEntity:
        """Add an experiment to the RO crate

        Args:
            experiment (Experiment): The experiment to be added to the crate
        """
        # Note that this is being created as a data catalog object as there are no better
        # fits

        properties: JsonProperties = {
            "@type": "DataCatalog",
            "name": experiment.name,
            "description": experiment.description,
            "project": experiment.projects,
        }
        experiment_obj = self._update_experiment_meta(
            experiment=experiment, properties=properties
        )
        return self._add_identifiers(experiment, experiment_obj)

    def add_dataset(self, dataset: Dataset) -> DataEntity:
        """Add a dataset to the RO crate

        Args:
            dataset (Dataset): The dataset to be added to the crate
        """
        directory = dataset.directory
        identifier = directory.as_posix()
        dataset.identifiers.append(identifier)
        experiments: List[str] = [
            self.crate.dereference("#" + experiment).id
            for experiment in dataset.experiments
        ]

        properties: JsonProperties = {
            "identifiers": identifier,
            "name": dataset.name,
            "description": dataset.description,
            "includedInDataCatalog": experiments,
        }
        instrument_id = dataset.instrument.id
        if dataset.instrument and isinstance(dataset.instrument, ContextObject):
            instrument_id = self.add_context_object(dataset.instrument).id
        properties["instrument"] = str(instrument_id)
        properties = self._update_properties(data_object=dataset, properties=properties)

        if identifier == ".":
            logger.debug("Updating root dataset")
            self.crate.root_dataset.properties().update(properties)
            self.crate.root_dataset.source = self.crate.source / Path(directory)
            dataset_obj = self.crate.root_dataset
        else:
            dataset_obj = self.crate.add_dataset(
                source=self.crate.source / Path(directory),
                properties=properties,
                dest_path=Path(directory),
            )
        return self._add_identifiers(dataset, dataset_obj)

    def add_datafile(self, datafile: Datafile) -> DataEntity:
        """Add a datafile to the RO-Crate,
        adding it to it's parent dataset has-part or the root if apropriate

        Args:
            datafile (Datafile): datafile to be added to the crate

        Returns:
            DataEntity: the datafile RO-Crate entity that will be written to the json-LD
        """
        identifier = datafile.filepath.as_posix()
        datafile.identifiers.append(identifier)
        properties: Dict[str, Any] = {
            "identifiers": identifier,
            "name": datafile.name,
            "description": datafile.description,
        }
        properties = self._update_properties(
            data_object=datafile, properties=properties
        )
        source = (
            self.crate.source / datafile.filepath
            if (self.crate.source / datafile.filepath).exists()
            else identifier
        )
        dataset_obj: DataEntity = self.crate.dereference(datafile.dataset)
        if not dataset_obj:
            dataset_obj = self.crate.root_dataset
        destination_path = source
        datafile_obj = self.crate.add_file(
            source=source,
            properties=properties,
            dest_path=destination_path,
        )
        logger.info("Adding File to Crate %s", identifier)
        dataset_obj.append_to("hasPart", datafile_obj)
        return self._add_identifiers(datafile, datafile_obj)

    def add_acl(self, acl: ACL) -> DataEntity:
        """Add an individual ACL to the RO-Crate

        Args:
            acl (ACL): the ACL to be added

        Returns:
            DataEntity: the RO-Crate context entity representing the ACL
        """
        if not self.crate.dereference(acl.grantee):
            self.add_context_object(
                context_object=ContextObject(
                    name=f"{acl.grantee} ACL holder",
                    description=f"Owner of ACL: {acl.grantee}",
                    identifiers=[acl.grantee],
                    schema_type=["Organization"]
                    if "organization" in acl.grantee_type
                    else ["Person"],
                    date_created=None,
                    date_modified=None,
                    additional_properties=None,
                )
            )
        return self.add_context_object(acl)

    def add_context_object(self, context_object: ContextObject) -> DataEntity:
        """Add a generic context object to the RO crate

        Args:
            context_object (ContextObject): the context object to be added

        Returns:
            DataEntity: the DataEntity in the RO-Crate
        """

        identifier = context_object.id
        properties = {
            key: value for key, value in context_object.__dict__.items() if value
        }
        if properties.get("schema_type"):
            properties["@type"] = properties.pop("schema_type")
        if context_object.date_created:
            properties = self._add_dates(
                properties,
                context_object.date_created,
                context_object.date_modified,
            )
            properties.pop("date_created")
            properties.pop("date_modified")
        context_entitiy = self.crate.add(
            ContextEntity(self.crate, identifier, properties=properties)
        )
        return context_entitiy

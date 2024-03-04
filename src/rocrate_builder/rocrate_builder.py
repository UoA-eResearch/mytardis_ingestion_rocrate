"""Defines the functions required to build an RO-crate"""
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from rocrate.model.contextentity import ContextEntity
from rocrate.model.data_entity import DataEntity
from rocrate.model.person import Person as ROPerson
from rocrate.rocrate import ROCrate

from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    BaseObject,
    Dataset,
    Experiment,
    Organisation,
    Person,
    Project,
)


class ROBuilder:
    """A class to hold and add entries to an ROCrate

    Attr:
        crate (ROCrate): an RO-Crate to build or modifiy
        metadata_dict (Dict): a dictionary read in from a series of JSON files
    """

    def __init__(
        self,
        crate: ROCrate,
    ) -> None:
        """Initialisation of the ROBuilder

        Args:
            crate (ROCrate): an RO-Crate, either empty or reread in
            metadata_dict (Dict[str, str | List[str] | Dict[str,str]]): A dictionary of metadata
                relating to the entries to be added.
        """
        self.crate = crate

    def __add_organisation(self, organisation: Organisation) -> None:
        """Read in an Organisation object and create a Organization entity in the crate

        Args:
            organisation (Organisation): The Organisation to add
        """
        identifier = organisation.identifiers[0]
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
        """Read in a Person object and create an entry for them in the crate

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
                and entity.id == person.affiliation.identifiers[0]
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
                "affiliation": person.affiliation.identifiers[0],
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
        obj_dataclass: BaseObject,
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

    def _add_metadata(
        self,
        properties: Dict[str, str | List[str] | Dict[str, Any]],
        metadata: Dict[str, str | List[str] | Dict[str, Any]],
    ) -> Dict[str, str | List[str] | Dict[str, Any]]:
        """Add generic metadata to the properties dictionary for a RO-Crate obj

        Args:
            properties (Dict[str, str | List[str] | Dict[str, Any]]): The properties to be
                added to the RO-Crate
            metadata (Dict[str, str | List[str] | Dict[str, Any]]): A dictionary of metadata
                to be added to the RO-Crate obj

        Returns:
            Dict[str, str|List[str]|Dict[str, Any]]: The updated properties dictionary
        """
        for key, value in metadata.items():
            if key not in properties.keys():  # pylint: disable=C0201
                properties[key] = value
            elif isinstance(properties[key], list):
                properties[key].append(value)  # type: ignore
            else:
                properties[key] = [properties[key], value]  # type: ignore
        return properties

    def _add_dates(
        self,
        properties: Dict[str, str | List[str] | Dict[str, Any]],
        created_date: datetime,
        updated_dates: Optional[List[datetime]] = None,
    ) -> Dict[str, str | List[str] | Dict[str, Any]]:
        """Add dates, where present, to the metadata

        Args:
            properties (Dict[str, str  |  List[str]  |  Dict[str, Any]]): _description_
            created_date (datetime): _description_
            updated_dates (Optional[List[datetime]], optional): _description_. Defaults to None.

        Returns:
            Dict[str, str | List[str] | Dict[str, Any]]: _description_
        """
        properties["dateCreated"] = created_date.isoformat()
        if updated_dates:
            properties["dateModified"] = [date.isoformat() for date in updated_dates]
        properties["datePublished"] = created_date.isoformat()
        return properties

    def add_project(self, project: Project) -> ContextEntity:
        """Add a project to the RO crate

        Args:
            project (Project): The project to be added to the crate
        """
        principal_investigator = self.add_principal_investigator(
            project.principal_investigator
        )
        if project.contributors:
            contributors = self.add_contributors(project.contributors)
        properties = {
            "@type": "Project",
            "name": project.name,
            "description": project.description,
            "principal_investigator": principal_investigator.id,
            "contributors": [contributor.id for contributor in contributors],
        }
        if project.metadata:
            properties = self._add_metadata(properties, project.metadata)
        if project.created_date:
            properties = self._add_dates(
                properties,
                project.created_date,
                project.updated_dates,
            )
        project_obj = ContextEntity(
            self.crate,
            project.identifiers[0],
            properties=properties,
        )
        return self._add_identifiers(project, project_obj)

    def add_experiment(self, experiment: Experiment) -> ContextEntity:
        """Add an experiment to the RO crate

        Args:
            experiment (Experiment): The experiment to be added to the crate
        """
        # Note that this is being created as a data catalog object as there are no better
        # fits

        identifier = experiment.identifiers[0]
        properties = {
            "@type": "DataCatalog",
            "name": experiment.name,
            "description": experiment.description,
            "project": experiment.project,
        }
        if experiment.metadata:
            properties = self._add_metadata(properties, experiment.metadata)  # type: ignore
        if experiment.created_date:
            properties = self._add_dates(  # type: ignore
                properties,  # type: ignore
                experiment.created_date,
                experiment.updated_dates,
            )
        experiment_obj = ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )
        return self._add_identifiers(experiment, experiment_obj)

    def add_dataset(
        self, dataset: Dataset, experiment_obj: ContextEntity
    ) -> DataEntity:
        """Add a dataset to the RO crate

        Args:
            dataset (Dataset): The dataset to be added to the crate
        """
        identifier = dataset.identifiers[0]
        properties = {
            "identifiers": identifier,
            "name": dataset.name,
            "description": dataset.description,
            "includedInDataCatalog": experiment_obj.id,
        }
        if dataset.metadata:
            properties = self._add_metadata(properties, dataset.metadata)
        if dataset.created_date:
            properties = self._add_dates(
                properties,
                dataset.created_date,
                dataset.updated_dates,
            )
        dataset_obj = self.crate.add_dataset(
            dataset.directory,
            properties=properties,
        )

        return self._add_identifiers(dataset, dataset_obj)

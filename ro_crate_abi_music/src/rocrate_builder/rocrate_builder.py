"""Defines the functions required to build an RO-crate"""
import re
from typing import Dict, List

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
        self, crate: ROCrate, metadata_dict: Dict[str, str | List[str] | Dict[str, str]]
    ) -> None:
        """Initialisation of the ROBuilder

        Args:
            crate (ROCrate): an RO-Crate, either empty or reread in
            metadata_dict (Dict[str, str | List[str] | Dict[str,str]]): A dictionary of metadata
                relating to the entries to be added.
        """
        self.crate = crate
        self.metadata_dict = metadata_dict

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

    def __add_person_to_crate(self, person: Person) -> None:
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
                "affiliation": person.affiliation.name,
            },
        )
        if len(person.identifiers) > 1:
            for identifier in person.identifiers:
                if identifier != person_id:
                    person_obj.append_to("identifier", identifier)
        self.crate.add(person_obj)

    def _add_principal_investigator(self, principal_investigator: Person) -> None:
        """Read in the principal investigator from the project and create an entry for them
        in the crate

        Args:
            principal_investigator (Person): _description_
        """
        self.__add_person_to_crate(principal_investigator)

    def _add_contributors(self, contributors: List[Person]):
        """Add the contributors to a project into the crate

        Args:
            contributors (List[Person]): A list of people to add as contributors
        """
        for contributor in contributors:
            self.__add_person_to_crate(contributor)

    def add_project(self, project: Project) -> ContextEntity:
        """Add a project to the RO crate

        Args:
            project (Project): The project to be added to the crate
        """
        self._add_principal_investigator(project.principal_investigator)
        self._add_contributors(project.contributors)
        project_obj = ContextEntity(
            self.crate,
            project.identifiers[0],
            properties={
                "@type": "Project",
                "name": project.name,
                "description": project.description,
            },
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
        experiment_obj = ContextEntity(
            self.crate,
            identifier,
            properties={
                "name": experiment.name,
                "description": experiment.description,
                "project": experiment.project,
            },
        )
        return self._add_identifiers(experiment, experiment_obj)

    def _add_identifiers(
        self,
        abi_dataclass: BaseObject,
        rocrate_obj: ContextEntity | DataEntity,
    ):
        if len(abi_dataclass.identifiers) > 1:
            for index, identifier in enumerate(abi_dataclass.identifiers):
                if index != 0:
                    rocrate_obj.append_to("identifier", identifier)
        self.crate.add(rocrate_obj)
        return rocrate_obj

    def add_dataset(
        self, dataset: Dataset, experiment_obj: ContextEntity
    ) -> DataEntity:
        """Add a dataset to the RO crate

        Args:
            dataset (Dataset): The dataset to be added to the crate
        """
        identifier = dataset.identifiers[0]
        dataset_obj = self.crate.add_dataset(
            dataset.directory.as_posix(),
            properties={
                "identifier": identifier,
                "name": dataset.name,
                "description": dataset.description,
                "includedInDataCatalog": experiment_obj.id,
            },
        )
        if dataset.metadata:
            for key, value in dataset.metadata.items():
                dataset_obj.append_to(key, value)
        self.crate.add(dataset_obj)
        return self._add_identifiers(dataset, dataset_obj)

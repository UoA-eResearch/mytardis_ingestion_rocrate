"""Definition of RO-Crate dataclasses"""

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class Organisation:
    """Dataclass to hold the details of an organisation for RO-Crate

    Attr:
        identifier (List[str]): A list of identifiers that can be used to uniquely
            identify the organisation - typically RoR
        name (str): The name of the organisation
        url (str): An optional URL for the organisation - default None
    """

    identifiers: List[str]
    name: str
    location: Optional[str] = None
    url: Optional[str] = None
    research_org: bool = True

    @property
    def id(self) -> str | int | float:
        """Retrieve first ID to act as RO-Crate ID"""
        return self.identifiers[0]


@dataclass
class Person:
    """Dataclass to hold the details of a person for RO-Crate

    Attr:
        identifier (List[str]): An optional list of unique identifiers for the person
            - Must contain UPI for import into MyTardis
            - typically ORCID - default None
        name (str): The full name (first name/last name) of the person
        email (str): A contact email address for the person named
        affilitation (Organisation): An organisation that is the primary affiliation for
            the person
    """

    name: str
    email: str
    affiliation: Organisation
    identifiers: List[str]

    @property
    def id(self) -> str | int | float:
        """Retrieve first ID to act as RO-Crate ID"""
        return self.identifiers[0]


@dataclass
class BaseObject(ABC):
    """Abstract Most basic object that can be turned into an RO-Crate entity"""

    name: str


@dataclass
class MTMetadata(BaseObject):
    """Concrete Metadata class for RO-crate
    Contains all information to store or recreate MyTardis metadata.
    Used as backup and recovery option.

    creates an RO-Crate JSON-LD entity matching this schema
    "@id": string - unique ID in the RO-Crate,
    "@type": string = "MyTardis-Metadata_field" - RO-Crate type,
    "name": string - name of the meadata in MyTardis,
    "value": string | Any - Metadata value in my tardis,
    "mt-type": string - Metadata type as recorded in MyTardis,
    "sensitive": bool - Metadata ,
    "parents": List[string] - list of ids for all the parents

    Attr:
        experiment (str): An identifier for an experiment
    """

    ro_crate_id: str
    value: str
    mt_type: str
    sensitive: bool
    parents: List[str] | None


@dataclass
class ContextObject(BaseObject):
    """Abstract dataclass for an object for RO-Crate

    Attr:
        name (str): The name of the object
        description (str): A longer form description of the object
        identifiers (List[str]):
            A list of identifiers for the object
            the first of which will be used as a UUID in the RO-Crate
        date_created (Optional[datetime]) : when was the object created
        date_modified (Optional[List[datetime]]) : when was the object last changed
        metadata (Optional[Dict[str, MTMetadata]])  a list of the mytardis metadata elements
        additional_properties Optional[Dict[str, Any]] : metadata not in schema
        schema_type (Optional[str | list[str]]) :Schema.org types or type
    """

    description: str
    identifiers: List[str | int | float]
    date_created: Optional[datetime]
    date_modified: Optional[List[datetime]]
    metadata: Optional[Dict[str, MTMetadata]]  # NOT IN SCHEMA.ORG
    additional_properties: Optional[Dict[str, Any]]
    schema_type: Optional[str | list[str]]

    @property
    def id(self) -> str | int | float:
        """Retrieve first ID to act as RO-Crate ID"""
        return self.identifiers[0]


@dataclass
class Instrument(ContextObject):
    """Dataclass for Instruments to be assoicated with MyTardis Datasets"""

    location: str
    schema_type = ["Instrument", "Thing"]


@dataclass
class MedicalCondition(BaseObject):
    """object for medical condtions that correspond to various
    standards and codes from https://schema.org/MedicalCondition
    """

    code_type: str
    code_text: str
    code_source: Path
    schema_type = "MedicalCondition"


@dataclass
class Participant(ContextObject):
    """participants of a study
    # to be flattend back into Experiment when read into MyTardis
    # person biosample object"""

    date_of_birth: str
    nhi_number: str
    sex: str
    ethnicity: str
    project: str


@dataclass
class Project(ContextObject):
    """Concrete Project class for RO-Crate - inherits from ContextObject
    https://schema.org/Project

    Attr:
        principal_investigator (Person): The project lead
        contributors (List[Person]): A list of people associated with the project who are not
            the principal investigator
    """

    principal_investigator: Person  # NOT IN SCHEMA.ORG
    contributors: Optional[List[Person]]
    mytardis_classification: Optional[str]  # NOT IN SCHEMA.ORG
    ethics_policy: Optional[str]
    schema_type = "Project"


@dataclass
class Experiment(ContextObject):
    """Concrete Experiment/Data-Catalog class for RO-Crate - inherits from ContextObject
    https://schema.org/DataCatalog
    Attr:
        project (str): An identifier for a project
    """

    projects: List[str]  # NOT IN SCHEMA.ORG
    contributors: Optional[List[Person]]
    mytardis_classification: Optional[str]  # NOT IN SCHEMA.ORG
    participant: Optional[str]
    schema_type = "DataCatalog"


@dataclass
class SampleExperiment(Experiment):  # pylint: disable=too-many-instance-attributes
    """Concrete Experiment/Data-Catalog class for RO-Crate - inherits from Experiment
    https://schema.org/DataCatalog
    Combination type with bioschemas biosample for additional sample data feilds
    https://bioschemas.org/types/BioSample/0.1-RELEASE-2019_06_19
    Attr:
        project (str): An identifier for a project
    """

    additional_property: Optional[List[Dict[str, str]]]
    sex: Optional[str]
    # isControl: Optional[bool]
    # itemLocation: Optional[str]
    # samplingAge:  Optional[int] #calculated from person DOB
    associated_disease: Optional[List[MedicalCondition]]
    body_location: Optional[
        MedicalCondition
    ]  # not defined in either sample or data catalog
    # but found here https://schema.org/body_location
    tissue_processing_method: Optional[str]
    analyate: Optional[str]
    portion: Optional[str]
    participant_metadata: Optional[Dict[str, MTMetadata]]
    schema_type = "DataCatalog"


@dataclass
class Dataset(ContextObject):
    """Concrete Dataset class for RO-crate - inherits from ContextObject

    Attr:
        experiment (str): An identifier for an experiment
    """

    experiments: List[str]
    directory: Path
    contributors: Optional[List[Person]]
    instrument: Instrument
    schema_type = "Dataset"

    # mytardis_classification: str #NOT IN SCHEMA.ORG
    def update_path(self, new_path: Path) -> None:
        """Update the path of a dataset chanigng it's name and identifiers

        Args:
            new_path (Path): path to update the dataset to
        """
        self.directory = new_path
        self.identifiers = [new_path.as_posix()]


@dataclass
class Datafile(ContextObject):
    """Concrete datafile class for RO-crate - inherits from ContextObject

    Attr:
        experiment (str): An identifier for an experiment
    """

    filepath: Path
    # mytardis_classification: str #NOT IN SCHEMA.ORG
    dataset: Path
    schema_type = "File"

    def update_to_root(self, dataset: Dataset) -> Path:
        """Update a datafile that is a child of a dataset so that dataset is now the root

        Args:
            dataset (Dataset): the dataset that is being updated to be root
        """
        self.dataset = Path("/.")
        try:
            new_filepath = self.filepath.relative_to(dataset.directory)
        except ValueError:
            new_filepath = self.filepath
        self.filepath = new_filepath
        return self.filepath

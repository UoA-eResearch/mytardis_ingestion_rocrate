# pylint: disable=R0801
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


@dataclass
class Person:
    """Dataclass to hold the details of a person for RO-Crate

    Attr:
        identifier (List[str]): An optional list of unique identifiers for the person
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


@dataclass
class BaseObject(ABC):
    """Abstract dataclass for an object for RO-Crate

    Attr:
        name (str): The name of the object
        description (str): A longer form description of the object
        identifiers (List[str]): A list of identifiers for the object
    """

    name: str
    description: str
    identifiers: List[str | int | float]
    created_date: Optional[datetime]
    updated_dates: Optional[List[datetime]]
    metadata: Optional[Dict[str, Any]]


@dataclass
class Project(BaseObject):
    """Concrete Project class for RO-Crate - inherits from BaseObject

    Attr:
        principal_investigator (Person): The project lead
        contributors (List[Person]): A list of people associated with the project who are not
            the principal investigator
    """

    principal_investigator: Person
    contributors: Optional[List[Person]]


@dataclass
class Experiment(BaseObject):
    """Concrete Experiment class for RO-Crate - inherits from BaseObject

    Attr:
        project (str): An identifier for a project
    """

    project: str


@dataclass
class Dataset(BaseObject):
    """Concrete Dataset class for RO-crate - inherits from BaseObject

    Attr:
        experiment (str): An identifier for an experiment
    """

    experiment: str
    directory: Path

"""Dataclasses specific to RO-Crates built for the Print Lab Group.
Extend existing RO-Crate MyTardis dataclasses
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    BaseObject,
    Dataset,
    Experiment,
    MyTardisContextObject,
    User,
)


@dataclass(kw_only=True)
class MedicalCondition(BaseObject):  # type: ignore
    """object for medical condtions that correspond to various
    standards and codes from https://schema.org/MedicalCondition
    """

    code: str
    code_type: str
    code_text: str
    code_source: str
    schema_type = "MedicalCondition"

    def __post_init__(self) -> None:
        object.__setattr__(self, "identifier", self.code)


@dataclass(kw_only=True)
class Participant(MyTardisContextObject):  # type: ignore
    """participants of a study
    # to be flattend back into Experiment when read into MyTardis
    # person biosample object"""

    gender: str
    ethnicity: str
    project: str
    raw_data: Dict[str, Any]
    date_of_birth: Optional[str] = None
    nhi_number: Optional[str] = None
    recipients: Optional[User] = None


@dataclass(kw_only=True)
class SampleExperiment(
    Experiment  # type: ignore
):  # pylint: disable=too-many-instance-attributes # type: ignore
    """Concrete Experiment/Data-Catalog class for RO-Crate - inherits from Experiment
    https://schema.org/DataCatalog
    Combination type with bioschemas biosample for additional sample data feilds
    https://bioschemas.org/types/BioSample/0.1-RELEASE-2019_06_19
    Attr:
        project (str): An identifier for a project
    """

    additional_property: Optional[List[Dict[str, str]]] = None
    gender: Optional[str]
    associated_disease: Optional[List[MedicalCondition]]
    body_location: Optional[
        MedicalCondition
    ]  # not defined in either sample or data catalog
    # but found here https://schema.org/body_location
    tissue_processing_method: Optional[str]
    participant: Participant
    analyate: Optional[str]
    portion: Optional[str]
    schema_type = "DataCatalog"


@dataclass(kw_only=True)
class ExtractionDataset(Dataset):  # type: ignore
    """Dataset information for extraction of a dataset
    that may need to copy unlisted"""

    copy_unlisted: bool

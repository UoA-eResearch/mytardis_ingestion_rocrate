"""
Interface for ingestion profiles, which define the specific ingestion behaviour for
different groups and/or instruments.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

from src.profiles.extractor import Extractor


class Profile(ABC):
    """Abstract base class defining the interface for an ingestion profile"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of this profile"""
        raise NotImplementedError("Concrete implementations must specify their name")

    @abstractmethod
    def get_extractor(self, options: Dict[str, Any]) -> Extractor:
        """Get the metadata extractor associated with this profile"""
        raise NotImplementedError("No metadata extractor has been implemented")

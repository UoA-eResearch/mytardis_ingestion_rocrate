# pylint: disable=missing-module-docstring, too-few-public-methods
from abc import ABC, abstractmethod
from typing import Any

from src.rocrate_dataclasses.data_class_utils import CrateManifest


class Extractor(ABC):
    """Abstract base class for extracting information from
    raw metadata files into RO-Crate dataclasses
    """

    @abstractmethod
    def extract(self, input_data_source: Any) -> CrateManifest:
        """Extract data from a directory or metadatafile into RO-Crate dataclass objects

        Args:
            input_data_source (Any): a directory or metadatafile path

        Raises:
            NotImplementedError: error for calling methods from this abstract class

        Returns:
            CrateManifest: manifest of the contents of an RO-Crate
        """
        raise NotImplementedError("Extraction behaviour must be defined in a subclass.")

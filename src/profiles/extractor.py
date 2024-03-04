# pylint: disable=missing-module-docstring
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class Extractor(ABC):
    """Abstract base class for extracting information from
    raw metadata files into RO-Crate dataclasses
    """

    @abstractmethod
    def extract_to_dataframe(self, input_data_source: Any) -> pd.DataFrame:
        """
        Taking a file path or io stream convert the contents
         into a pandas dataframe based on the expectations of the profile
        """
        raise NotImplementedError("Extraction behaviour must be defined in a subclass.")

    @abstractmethod
    def dataframe_to_dataclasses(self, df: pd.DataFrame) -> None:
        """
        Taking a dataframe convert the contents into RO-Crate packagable dataclasses
        """
        raise NotImplementedError("Extraction behaviour must be defined in a subclass.")

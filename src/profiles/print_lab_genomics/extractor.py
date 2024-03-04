# pylint: disable=missing-module-docstring
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.encryption.encrypt_metadata import Encryptor
from src.profiles.extractor import Extractor
from src.utils.file_utils import is_xslx

PRINT_LAB_SENSITIVE_FIELDS = ["NHINUMBER", "SECRET"]
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PrintLabExtractor(Extractor):
    """An extractor that takes and XLS datasheet
    as defined by the Print Genomics Lab into a Dataframe
    Encryption of strings occurs during extraction

    Args:
        Extractor (_type_): the extractor base class invoked by the CLI
    """

    def __init__(self, encryptor: Encryptor) -> None:
        self.encryptor = encryptor
        self.accepted_format = ["xlsx"]

    def _contruct_sensitive_dict(
        self, sensitive_feild_names: list[str], encryptor: Encryptor
    ) -> dict[str, Callable[[str], str]]:
        return {
            field_name: encryptor.encrypt_string for field_name in sensitive_feild_names
        }

    def extract_to_dataframe(self, input_data_source: Any) -> pd.DataFrame:
        if not isinstance(input_data_source, Path) or not is_xslx(input_data_source):
            raise ValueError("Print lab genomics file must be an excel file")
        sensitive_dict = self._contruct_sensitive_dict(
            PRINT_LAB_SENSITIVE_FIELDS, self.encryptor
        )
        logger.debug(isinstance(sensitive_dict, dict))

        parsed_df = pd.read_excel(
            input_data_source, engine="openpyxl", converters=sensitive_dict
        )
        return parsed_df

    def dataframe_to_dataclasses(self, df: pd.DataFrame) -> None:
        """
        Taking a dataframe convert the contents into RO-Crate packagable dataclasses
        """
        raise NotImplementedError("Extraction behaviour must be defined in a subclass.")

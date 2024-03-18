# pylint: disable=missing-module-docstring
from typing import Any, Dict

from src.profiles.extractor import Extractor
from src.profiles.print_lab_genomics.extractor import PrintLabExtractor
from src.profiles.profile_base import Profile


class PrintLabGenomicsProfile(Profile):
    """Profile defining the ingestion behaviour for metadata from RO-crates"""

    def __init__(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "ro_crate"

    def get_extractor(self, options: Dict[str, Any]) -> Extractor:
        return PrintLabExtractor(options)


def get_profile() -> Profile:
    """Entry point for the profile - returns the profile corresponding to the requested version"""
    return PrintLabGenomicsProfile()

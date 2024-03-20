"""RO-Crate generation profile for ABI-Music Data
"""
import logging
from pathlib import Path
from typing import Any, Dict

import src.profiles.abi_music.consts as profile_consts
from src.metadata_extraction.metadata_extraction import MetadataHanlder
from src.profiles.abi_music.abi_json_parser import parse_raw_data
from src.profiles.abi_music.filesystem_nodes import DirectoryNode
from src.profiles.extractor import Extractor
from src.rocrate_dataclasses.data_class_utils import CrateManifest


class ABIExtractor(Extractor):  # pylint: disable=too-few-public-methods
    """Abstract base class for extracting information from
    raw metadata files into RO-Crate dataclasses
    """

    def __init__(self, options: Dict[str, Any]) -> None:
        if not options.get("api_agent"):
            logging.error("Insufficent API information, can't load metadata schemas")
        else:
            self.api_agent = options["api_agent"]
            self.metadata_handler = MetadataHanlder(
                self.api_agent, profile_consts.NAMESPACES
            )

    def extract(self, input_data_source: Path) -> CrateManifest:
        """Extract RO-Crate data from ABI music file structure

        Args:
            input_data_source (Path): source file or directory to begin parsing ABI data

        Returns:
            CrateManifest: manifest of RO-Crate ingestible dataclasses
        """
        if input_data_source.is_file():
            root_dir = DirectoryNode(input_data_source.parent)
        root_dir = DirectoryNode(input_data_source)
        return parse_raw_data(
            raw_dir=root_dir,
            metadata_handler=self.metadata_handler,
        )

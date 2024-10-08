"""Class for independently generating ro-crates from ABI data
"""

from pathlib import Path
from typing import Optional

from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import CrateManifest

import src.ingestion_targets.abi_music.consts as profile_consts
from src.cli.mytardisconfig import SchemaConfig
from src.ingestion_targets.abi_music.abi_json_parser import parse_raw_data
from src.ingestion_targets.abi_music.filesystem_nodes import DirectoryNode
from src.metadata_extraction.metadata_extraction import (
    MetadataHandlder,
    load_optional_schemas,
)
from src.mt_api.apiconfigs import MyTardisRestAgent


class ABICrateExtractor:  # pylint: disable=too-few-public-methods
    """_summary_

    Returns:
        _type_: _description_
    """

    metadata_handler: MetadataHandlder

    def __init__(
        self, api_agent: MyTardisRestAgent, schemas: Optional[SchemaConfig]
    ) -> None:
        namespaces = profile_consts.NAMESPACES
        namespaces = load_optional_schemas(namespaces=namespaces, schemas=schemas)
        self.api_agent = api_agent
        self.metadata_handler = MetadataHandlder(api_agent, profile_consts.NAMESPACES)

    def extract_crates(
        self, input_data_source: Path, collect_all: bool = False
    ) -> CrateManifest:
        """Build crates from datasets found in an ABI directory

        Args:
            input_data_source (Path): ABI Directory root
            collect_all (bool): Collect all data found within ABI_json into poteintial MT Metadata

        Returns:
            CrateManifest: a manifest of all objects found in the ABI directory
        """
        if input_data_source.is_file():
            root_dir = DirectoryNode(input_data_source.parent)
        else:
            root_dir = DirectoryNode(input_data_source)
        return parse_raw_data(
            raw_dir=root_dir,
            metadata_handler=self.metadata_handler,
            collect_all=collect_all,
            api_agent=self.api_agent,
        )

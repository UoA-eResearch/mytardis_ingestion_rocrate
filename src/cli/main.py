# pylint: disable=fixme
"""
Script for CLI interface to invoke conversion of various input formats
into RO-Crates suitable for MyTardis ingestion
Should read input from a filepath or stdin and convert into metadata
and file objects recorded as an RO-Crate
"""

import logging
import sys
from io import StringIO
from pathlib import Path

import click

from src.encryption.encrypt_metadata import Encryptor
from src.profiles.profile_register import load_profile
from src.utils.log_utils import init_logging


@click.command()
@click.option(
    "--input_metadata", help="compose file to work with", type=str, default=""
)
@click.option("--profile_name", type=str, default="print_lab_genomics")
@click.option("--encryption_key", type=str, multiple=True, default=[])
@click.option(
    "--log_file", type=click.Path(writable=True), default=Path("ingestion.log")
)
def main(
    input_metadata: str, profile_name: str, encryption_key: list[str], log_file: Path
) -> None:
    """
    Load a metadata file and datafiles on disk into an RO-Crate
    """
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # load profile
    # load encryptor
    # Read public keys ect
    encryptor: Encryptor = Encryptor(encryption_key)
    profile = load_profile(profile_name)
    extractor = profile.get_extractor(encryptor)

    input_data_source: Path | StringIO | None = None
    if input_metadata == "-":
        input_data_source = StringIO(sys.stdin.read())
        # TODO check and sanitize input from stdin
    else:
        input_data_source = Path(input_metadata)
    dataframe = extractor.extract_to_dataframe(input_data_source)
    logger.debug(dataframe.head())


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

# pylint: disable=fixme, too-many-arguments, too-many-locals
"""
Script for CLI interface to invoke conversion of various input formats
into RO-Crates suitable for MyTardis ingestion
Should read input from a filepath or stdin and convert into metadata
and file objects recorded as an RO-Crate
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import click

from src.encryption.encrypt_metadata import Encryptor
from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.profiles.abi_music.crate_builder import ABICrateBuilder
from src.profiles.profile_register import load_profile
from src.rocrate_builder.rocrate_writer import archive_crate, bagit_crate, write_crate
from src.rocrate_dataclasses.data_class_utils import reduce_to_dataset
from src.utils.log_utils import init_logging

OPTION_INPUT_PATH = click.option(
    "-i",
    "--input_metadata",
    help="input file or directory to be converted into an RO-Crate",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    default=os.getcwd(),
)
OPTION_HOSTNAME = click.option(
    "--mt_hostname",
    type=str,
    default=CONNECTION__HOSTNAME,
    help="hostname for MyTardis API",
)
OPTION_MT_USER = click.option(
    "--mt_user", type=str, default=None, help="username for MyTardis API"
)
OPTION_MT_APIKEY = click.option(
    "--mt_api_key", type=str, default=None, help="API key for MyTardis API"
)
OPTION_LOG = click.option(
    "--log_file", type=click.Path(writable=True), default=Path("ingestion.log")
)

OPTION_COLLECT_ALL = click.option(
    "--collect-all",
    type=bool,
    is_flag=True,
    default=False,
    help="collect all values into MyTardis metadata.\n even those not found in schema",
)


@click.group()
def cli() -> None:
    "Commands to generate an RO-Crate with MyTardis Metadata"


@click.command()
@OPTION_INPUT_PATH
@OPTION_LOG
@OPTION_HOSTNAME
@OPTION_MT_USER
@OPTION_MT_APIKEY
@OPTION_COLLECT_ALL
def crate_abi(
    input_metadata: Path,
    log_file: Path,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    collect_all: bool = False,
) -> None:
    """
    Create RO-Crates by dataset from ABI-music filestructure
    """
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    mt_user = mt_user if mt_user else os.environ.get("AUTH__USERNAME")
    mt_api_key = mt_api_key if mt_api_key else os.environ.get("AUTH__API_KEY")
    logger.info("Loading MyTardis API agent")
    if mt_user and mt_api_key:
        auth_config = AuthConfig(username=mt_user, api_key=mt_api_key)
    else:
        auth_config = None
    api_agent = MyTardisRestAgent(
        auth_config=auth_config,
        connection_hostname=mt_hostname,
        connection_proxies=None,
        verify_certificate=True,
    )
    builder = ABICrateBuilder(api_agent)
    builder.build_crates(input_metadata, collect_all)


@click.command()
@OPTION_INPUT_PATH
@click.option("--profile_name", type=str, default="print_lab_genomics")
@click.option("--encryption_key", type=str, multiple=True, default=[])
@OPTION_LOG
@OPTION_HOSTNAME
@OPTION_MT_USER
@OPTION_MT_APIKEY
@click.option(
    "-o", "--output", type=Path, default=None, help="output location for RO-Crate(s)"
)
@click.option(
    "-b",
    "--bag_crate",
    type=bool,
    default=True,
    help="Create a bagit manifest for the RO-Crate",
)
@click.option(
    "-a",
    "--archive_type",
    type=str,
    default=None,
    help="Archive the RO-Crate in one of the following formats: [tar, tar.gz, zip]",
)
@click.option(
    "-d",
    "--split_datasets",
    type=bool,
    is_flag=True,
    default=False,
    help="Produce an RO-Crate for each dataset\n (bagging and archiving each crate individually)",
)
def crate_general(
    input_metadata: Path,
    profile_name: str,
    encryption_key: list[str],
    log_file: Path,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    output: Optional[Path],
    archive_type: Optional[str],
    bag_crate: Optional[bool],
    split_datasets: Optional[bool],
    collect_all: Optional[bool],
) -> None:
    """
    Load a metadata file and datafiles on disk into an RO-Crate
    """
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    # load profile
    # load encryptor
    # Read public keys ect
    mt_user = mt_user if mt_user else os.environ.get("AUTH__USERNAME")
    mt_api_key = mt_api_key if mt_api_key else os.environ.get("AUTH__API_KEY")
    logger.info("Loading MyTardis API agent")
    if mt_user and mt_api_key:
        auth_config = AuthConfig(username=mt_user, api_key=mt_api_key)
    else:
        auth_config = None
    api_agent = MyTardisRestAgent(
        auth_config=auth_config,
        connection_hostname=mt_hostname,
        connection_proxies=None,
        verify_certificate=True,
    )

    logger.info("Loading Encryption (this does nothing right now!)")
    encryptor: Encryptor = Encryptor(encryption_key)
    options = {
        "encryptor": encryptor,
        "api_agent": api_agent,
        "collect_all": collect_all,
    }

    logger.info("Loading extraction profile")
    profile = load_profile(profile_name)
    extractor = profile.get_extractor(options)
    logger.info("extracting crate metadata")
    crate_manifest = extractor.extract(input_metadata)

    if split_datasets:
        logger.info("splitting metadata into one crate per dataset")
        crates = {
            dataset.directory: reduce_to_dataset(crate_manifest, dataset)
            for dataset in crate_manifest.datasets
        }
    else:
        crates = {Path(""): crate_manifest}
    logger.debug("we've loaded these crates %s", crates)
    source_path = input_metadata
    if Path(input_metadata).is_file():
        source_path = Path(input_metadata).parent
    if not output:
        output = source_path
    for crate_dir in crates.keys():
        logger.info("writing RO-Crate from %s", crate_dir)

        final_output = output / crate_dir
        if not final_output.parent.exists():
            final_output.parent.mkdir(parents=True)
        crate_destination = final_output

        if archive_type:
            logger.info("writing pre-archive temporary crate")
            tmp_crate_location = (  # pylint: disable=consider-using-with
                tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
            )
            crate_destination = Path(Path(tmp_crate_location.name) / source_path.name)
            crate_destination.mkdir()

        logger.info("writing crate %s", source_path / crate_dir)
        write_crate(
            crate_source=crate_dir,
            crate_destination=crate_destination,
            crate_contents=crates[crate_dir],
        )
        if bag_crate:
            bagit_crate(crate_destination, mt_user or "")
        archive_crate(archive_type, final_output, crate_destination)

        if tmp_crate_location:
            tmp_crate_location.cleanup()


cli.add_command(crate_general)
cli.add_command(crate_abi)


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter

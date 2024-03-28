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

from src.cli.mytardisconfig import MyTardisEnvConfig
from src.encryption.encrypt_metadata import Encryptor
from src.ingestion_targets.abi_music.crate_builder import ABICrateBuilder
from src.ingestion_targets.print_lab_genomics.extractor import PrintLabExtractor

# from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.rocrate_builder.rocrate_writer import archive_crate, bagit_crate, write_crate
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
    help="""Collect all input metadata into MyTardis metadata and RO-Crate.
    Even those not found in schema.
    This is a 'put everything in and deal with it later'
    option that will probably invalidate the RO-Crate but preserves all possible metadata.""",
)
OPTION_ENV_PREFIX = click.option(
    "--env_prefix",
    type=str,
    default="",
    help="""enviroment file prefix for loading MyTardis API and Schemas config,
    Config options are overwritten by CLI arguments.""",
)


@click.group()
def cli() -> None:
    "Commands to generate an RO-Crate with MyTardis Metadata"


@click.command()
@OPTION_INPUT_PATH
@OPTION_LOG
@OPTION_ENV_PREFIX
@OPTION_HOSTNAME
@OPTION_MT_USER
@OPTION_MT_APIKEY
@OPTION_COLLECT_ALL
def abi(
    input_metadata: Path,
    log_file: Path,
    env_prefix: str,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    collect_all: Optional[bool] = False,
) -> None:
    """
    Create RO-Crates by dataset from ABI-music filestructure.
    Input Metadata is the same root directory used for MyTardis ingest
    """
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    env_config = None
    if (Path(env_prefix) / ".env").exists():
        env_config = MyTardisEnvConfig(env_prefix=env_prefix)  # type: ignore
        mt_user = mt_user if mt_user else env_config.auth.username
        mt_api_key = mt_api_key if mt_api_key else env_config.auth.api_key
        mt_hostname = mt_hostname if mt_hostname else env_config.connection.hostname
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
    builder = ABICrateBuilder(
        api_agent, env_config.default_schema if env_config else None
    )
    builder.build_crates(input_metadata, bool(collect_all))


@click.command()
@OPTION_INPUT_PATH
@click.option("--encryption_key", type=str, multiple=True, default=[])
@OPTION_LOG
@OPTION_ENV_PREFIX
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
@OPTION_COLLECT_ALL
def print_lab(
    input_metadata: Path,
    encryption_key: list[str],
    log_file: Path,
    env_prefix: str,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    output: Optional[Path],
    archive_type: Optional[str],
    bag_crate: Optional[bool],
    collect_all: Optional[bool],
) -> None:
    """
    Create an RO-Crate based on a Print Lab metadata file
    """
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    env_config = None
    if (Path(env_prefix) / ".env").exists():
        env_config = MyTardisEnvConfig(env_prefix=env_prefix)  # type: ignore
        mt_user = mt_user if mt_user else env_config.auth.username
        mt_api_key = mt_api_key if mt_api_key else env_config.auth.api_key
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
    extractor = PrintLabExtractor(
        encryptor=encryptor,
        api_agent=api_agent,
        schemas=env_config.default_schema if env_config else None,
        collect_all=collect_all if collect_all else False,
    )
    logger.info("extracting crate metadata")
    crate_manifest = extractor.extract(input_metadata)

    source_path = input_metadata
    if Path(input_metadata).is_file():
        source_path = Path(input_metadata).parent
    if not output:
        output = source_path
    logger.info("writing RO-Crate from %s", source_path)

    final_output = output
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

    logger.info("writing crate %s", source_path)
    write_crate(
        crate_source=source_path,
        crate_destination=crate_destination,
        crate_contents=crate_manifest,
        meta_only=False,
    )
    if bag_crate:
        bagit_crate(crate_destination, mt_user or "")
    archive_crate(archive_type, final_output, crate_destination)


cli.add_command(print_lab)
cli.add_command(abi)


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter

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
from typing import List, Optional, Type

import click
from mytardis_rocrate_builder.rocrate_builder import ROBuilder
from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import (
    CrateManifest,
    reduce_to_dataset,
)
from mytardis_rocrate_builder.rocrate_writer import (
    archive_crate,
    bagit_crate,
    bulk_encrypt_file,
    receive_keys_for_crate,
    write_crate,
)
from rocrate.rocrate import ROCrate
from slugify import slugify

from src.cli.mytardisconfig import MyTardisEnvConfig
from src.ingestion_targets.abi_music.crate_builder import ABIROBuilder
from src.ingestion_targets.abi_music.crate_extractor import ABICrateExtractor
from src.ingestion_targets.print_lab_genomics.extractor import PrintLabExtractor
from src.ingestion_targets.print_lab_genomics.ICD11_API_agent import ICD11ApiAgent
from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)

# from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.utils.log_utils import init_logging

OPTION_INPUT_PATH = click.option(
    "-i",
    "--input_metadata",
    help="input file or directory to be converted into an RO-Crate",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    default=os.getcwd(),
)
OPTION_OUTPUT_PATH = click.option(
    "-o", "--output", type=Path, default=None, help="output location for RO-Crate(s)"
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

OPTION_CLONE_DIRECTORY = click.option(
    "--duplicate_directory",
    type=bool,
    is_flag=True,
    default=False,
    help="""duplicate the contents of the target directory even if not explicitly listed.""",
)

OPTION_ENV_PREFIX = click.option(
    "--env_prefix",
    type=str,
    default="",
    help="""environment file prefix for loading MyTardis API and Schemas config,
    Config options are overwritten by CLI arguments.""",
)

OPTION_SPLIT_DATASETS = click.option(
    "--split_datasets",
    type=bool,
    is_flag=True,
    default=False,
    help="Split into individual RO-Crates for each dataset",
)

OPTION_DRY_RUN = click.option(
    "--dry-run",
    "-d",
    type=bool,
    is_flag=True,
    default=False,
    help="only generate metadata without moving or copying files",
)
OPTION_ARCHIVE_TYPE = click.option(
    "-a",
    "--archive_type",
    type=str,
    default=None,
    help="Archive the RO-Crate in one of the following formats: [tar, tar.gz, zip]",
)
OPTION_TMP_DIR = click.option(
    "--tmp_dir",
    type=Path,
    help="replace default temporary file location",
)



@click.group()
def cli() -> None:
    "Commands to generate an RO-Crate with MyTardis Metadata"


@click.command()
@OPTION_INPUT_PATH
@OPTION_OUTPUT_PATH
@OPTION_LOG
@OPTION_ENV_PREFIX
@OPTION_HOSTNAME
@OPTION_MT_USER
@OPTION_MT_APIKEY
@OPTION_COLLECT_ALL
@OPTION_SPLIT_DATASETS
@OPTION_DRY_RUN
@OPTION_ARCHIVE_TYPE
@OPTION_TMP_DIR
@click.option(
    "--in_place",
    type=bool,
    is_flag=True,
    default=False,
    help="""Write RO-Crates and BagIT manifests in-place at the directory level. 
    Requires crate to contain only one dataset or --split-datasets. 
    Warning! will move files to create BagIT Manifest""",
)
@click.option(
    "--experiment_dir",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    default=None,
    help="Specify an individual experiment to be packaged"
)
@click.option(
    "--dataset_dir",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    default=None,
    help="Specify an individual dataset to be packaged"
)
def abi(  # pylint: disable=too-many-positional-arguments
    input_metadata: Path,
    output: Path,
    log_file: Path,
    env_prefix: str,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    collect_all: Optional[bool] = False,
    split_datasets: bool = True,
    dry_run: Optional[bool] = False,
    archive_type: Optional[str] = None,
    tmp_dir: Optional[Path] = None,
    in_place: Optional[bool] = False,
    experiment_dir: Optional[Path] = None,
    dataset_dir: Optional[Path] = None,
) -> None:
    """
    Create RO-Crates by dataset from ABI-music filestructure.
    Input Metadata is the same root directory used for MyTardis ingest
    """
    if tmp_dir:
        tempfile.tempdir = str(tmp_dir)
    input_metadata = input_metadata.absolute()
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    env_config = None
    if (Path(env_prefix) / ".env").exists():
        env_config = MyTardisEnvConfig(_env_prefix=env_prefix)  # type: ignore
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
    extractor = ABICrateExtractor(
        api_agent, env_config.default_schema if env_config else None
    )
    os.chdir(input_metadata)
    crate_manifest = extractor.extract_crates(input_metadata, bool(collect_all), experiment_dir, dataset_dir)
    crate_manifests = split_manifests(split_datasets, crate_manifest)
    exclude: list[str] = []
    source_path = input_metadata
    if Path(input_metadata).is_file():
        source_path = Path(input_metadata).parent
        exclude.append(input_metadata.name)
    for manifest in crate_manifests:
        if in_place:
            write_inplace(manifest, exclude=exclude, gpg_binary=None, crate_builder=ABIROBuilder, bag_crate=True)
        else:
            write_and_archive_manifest(
                manifest=manifest,
                crate_builder=ABIROBuilder,
                source_path=source_path,
                output=output,
                archive_type=archive_type,
                gpg_binary=None,
                exclude=exclude,
                dry_run=dry_run,
                duplicate_directory=False,
                bag_crate=True,
                bulk_encrypt=False,
                separate_manifests=True,
                pubkey_fingerprints=[],
            )


@click.command()
@OPTION_INPUT_PATH
@OPTION_OUTPUT_PATH
@click.option(
    "--pubkey_fingerprints",
    "-k",
    type=str,
    multiple=True,
    default=[],
    help="pgp public key fingerprints for encryption of metadata, accepts multiple",
)
@OPTION_LOG
@OPTION_ENV_PREFIX
@OPTION_HOSTNAME
@OPTION_MT_USER
@OPTION_MT_APIKEY
@click.option(
    "-b",
    "--bag_crate",
    type=bool,
    default=True,
    help="Create a bagit manifest for the RO-Crate",
)
@OPTION_ARCHIVE_TYPE
@click.option(
    "--gpg_binary", type=Path, default=None, help="binary for running gpg encryption"
)
@OPTION_COLLECT_ALL
@OPTION_CLONE_DIRECTORY
@click.option(
    "--bulk_encrypt",
    type=bool,
    is_flag=True,
    default=False,
    help="Bulk encrypt the entire crate or archive",
)
@OPTION_SPLIT_DATASETS
@OPTION_DRY_RUN
@OPTION_TMP_DIR
@click.option(
    "--separate_manifests",
    type=bool,
    is_flag=True,
    default=False,
    help="generate a separate copy of any file manifest before output",
)
def print_lab(  # pylint: disable=too-many-positional-arguments,too-many-branches,too-many-statements
    input_metadata: Path,
    output: Path,
    pubkey_fingerprints: list[str],
    log_file: Path,
    env_prefix: str,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    archive_type: Optional[str],
    bag_crate: Optional[bool],
    collect_all: Optional[bool],
    gpg_binary: Optional[Path],
    duplicate_directory: Optional[bool],
    bulk_encrypt: Optional[bool],
    split_datasets: bool,
    dry_run: Optional[bool],
    tmp_dir: Optional[Path],
    separate_manifests: Optional[bool],
) -> None:
    """
    Create an RO-Crate based on a Print Lab metadata file
    """
    if tmp_dir:
        tempfile.tempdir = str(tmp_dir)
    output = Path(os.path.abspath(output))
    init_logging(file_name=str(log_file), level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    env_config = None
    # if (Path(env_prefix) / ".env").exists():
    env_config = MyTardisEnvConfig(_env_prefix=env_prefix)  # type: ignore
    mt_user = mt_user if mt_user else env_config.auth.username
    mt_api_key = mt_api_key if mt_api_key else env_config.auth.api_key
    if env_config.mytardis_pubkey.key:
        pubkey_fingerprints = list(pubkey_fingerprints)
        pubkey_fingerprints.append(env_config.mytardis_pubkey.key)
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
    extractor = PrintLabExtractor(
        api_agent=api_agent,
        schemas=env_config.default_schema if env_config else None,
        collect_all=collect_all if collect_all else False,
        pubkey_fingerprints=(
            [env_config.mytardis_pubkey.key]
            if env_config and env_config.mytardis_pubkey.key
            else []
        ),
        icd_11_agent=ICD11ApiAgent(),
    )
    logger.info("extracting crate metadata")
    os.chdir(input_metadata.parent)
    crate_manifest = extractor.extract(input_metadata)
    exclude = [(input_metadata / "sampledata.xlsx").as_posix()]
    source_path = input_metadata
    if Path(input_metadata).is_file():
        source_path = Path(input_metadata).parent
        exclude.append(input_metadata.name)
    crate_manifests = split_manifests(split_datasets, crate_manifest)
    for manifest in crate_manifests:
        write_and_archive_manifest(
            manifest=manifest,
            crate_builder=PrintLabROBuilder,
            source_path=source_path,
            output=output,
            archive_type=archive_type,
            gpg_binary=gpg_binary,
            exclude=exclude,
            dry_run=dry_run,
            duplicate_directory=duplicate_directory,
            bag_crate=bag_crate,
            bulk_encrypt=bulk_encrypt,
            separate_manifests=separate_manifests,
            pubkey_fingerprints=pubkey_fingerprints,
        )


def write_and_archive_manifest(
    manifest: CrateManifest,
    crate_builder: Type[ROBuilder],
    source_path: Path,
    output: Path,
    exclude: List[str],
    pubkey_fingerprints: list[str],
    archive_type: str | None = None,
    gpg_binary: Path | None = None,
    dry_run: bool | None = False,
    duplicate_directory: bool | None = False,
    bag_crate: bool | None = True,
    bulk_encrypt: bool | None = False,
    separate_manifests: bool | None = True,
) -> None:
    """Write and archive RO-crates using appropriate locations for archiving
    and bulk encryption.

    Args:
        crate_manifest (CrateManifest): the manifest of an RO-Crate
        crate_builder (Type[ROBuilder]): the crate builder class
        source_path (Path): the origin of the RO-Crate data
        output (Path): the final destination of the output crate
        archive_type (str): what type of archive is the crate to be saved as
        gpg_binary (Path): gpg binary used for encryption
        exclude (List[str]): files to exclude from the crate
        dry_run (bool): only produce manifests without moving files
        duplicate_directory (bool): move all files in the child directory
        bag_crate (bool): produce a bagit for the crate
        bulk_encrypt (bool): bulk encrypt the archived crate (very slow)
        separate_manifests (bool): copy manifests to a separate directory
        pubkey_fingerprints (list[str]): pubkey fingerprints used for encryption
    """
    logger = logging.getLogger(__name__)
    output = output.absolute()
    final_output = make_output_dir(output=output, manifest_id=manifest.identifier)
    logger.info("writing RO-Crate from %s to %s", source_path, final_output)
    crate_destination = final_output
    if archive_type:
        tmp_crate_location = (  # pylint: disable=consider-using-with
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )
        crate_destination = Path(
            Path(tmp_crate_location.name) / source_path.name / manifest.identifier
        )
        logger.info(
            "Archiving crate writing, temp crate to tmpdir: %s",
            crate_destination.as_posix(),
        )
        crate_destination.mkdir(parents=True)
    logger.info("writing crate %s", source_path)

    logger.info("Initializing crate")
    crate = ROCrate(  # pylint: disable=unexpected-keyword-arg
        gpg_binary=gpg_binary, exclude=exclude
    )
    receive_keys_for_crate(crate.gpg_binary, crate_contents=manifest)

    crate.source = source_path if duplicate_directory else None
    builder = crate_builder(crate)
    write_crate(
        builder=builder,
        crate_source=crate.source,
        crate_destination=crate_destination,
        crate_contents=manifest,
        meta_only=dry_run,
    )
    if bag_crate:
        bagit_crate(crate_destination, "The University of Auckland")
    if bulk_encrypt:
        archive_crate(
            archive_type,
            crate_destination,
            crate_destination,
            True,
            separate_manifests,
        )
        logger.info("Bulk Encrypting RO-Crate")
        target = (
            crate_destination.with_suffix("." + archive_type)
            if archive_type
            else crate_destination
        )
        bulk_encrypt_file(
            gpg_binary=crate.gpg_binary,
            pubkey_fingerprints=pubkey_fingerprints,
            data_to_encrypt=target,
            output_path=final_output,
        )
    else:
        archive_crate(
            archive_type, final_output, crate_destination, True, separate_manifests
        )


def split_manifests(
    split_datasets: bool, crate_manifest: CrateManifest
) -> List[CrateManifest]:
    """Split a crate manifest into a list of individual manifests

    Args:
        split_datasets (bool): split the manifest or not
        crate_manifest (CrateManifest): the initial manifest

    Returns:
        List[CrateManifest]: the list of manifests to return
    """
    if not split_datasets:
        return [crate_manifest]
    return [
        reduce_to_dataset(crate_manifest, dataset=dataset)
        for dataset in crate_manifest.datasets.values()
    ]

def write_inplace(
    crate_manifest: CrateManifest,
    exclude:List[str],
    crate_builder: Type[ROBuilder],
    bag_crate: bool | None = True, 
    gpg_binary: Path | None = None
) -> None:
    """Write single directory crate in-place at the directory level

    Args:
        crate_manifest (CrateManifest): a manifest containing a single dataset
        exclude (List[str]): files to be excluded from RO-Crate construction
        crate_builder (Type[ROBuilder]): the crate builder for writing the crate in-place
        bag_crate (bool | None, optional): create a bagit. Defaults to True.
        gpg_binary (Path | None, optional): the binary for crate encryption. Defaults to None.

    Raises:
        ValueError: If the manifest contains too many datasets then writing in-place is not possible
    """
    if len(crate_manifest.datasets) != 1:
        raise ValueError(f"{crate_manifest.identifier} Contains too many datasets {len(crate_manifest.datasets)}, please split datasets if writing in-place")
    dataset = next(iter(crate_manifest.datasets.values()))
    current_dir = Path(os.getcwd()) / dataset.directory
    # os.chdir(current_dir)
    dataset.directory = Path("./")
    crate = ROCrate(  # pylint: disable=unexpected-keyword-arg
            gpg_binary=gpg_binary, exclude=exclude
        )
    receive_keys_for_crate(crate.gpg_binary, crate_contents=crate_manifest)
    builder = crate_builder(crate)
    write_crate(
        builder=builder,
        crate_source=Path(current_dir),
        crate_destination=Path(current_dir),
        crate_contents=crate_manifest,
        meta_only=True,
    )
    if bag_crate:
        bagit_crate(current_dir, "The University of Auckland")


def make_output_dir(output: Path, manifest_id: str) -> Path:
    """Create the path for an output RO-Crate if the directory does not exist create it

    Args:
        output (Path): the output path of the RO-Crate
        manifest_id (str): the manifest of the RO-Crate

    Returns:
        Path:the destination of the crate
    """
    final_output = output / manifest_id
    if not final_output.parent.exists():
        final_output.parent.mkdir(parents=True, exist_ok=True)
    return final_output


@click.command()
@OPTION_INPUT_PATH
@OPTION_LOG
@click.option(
    "--participant_id",
    type=str,
    default=None,
    help="id of participant",
)
def extract_participant_sensitive(
    input_metadata: Path, log_file: Path, participant_id: str
) -> None:
    """Testing function to decrypt sensitive

    Args:
        input_metadata (Path): _description_
        log_file (Path): _description_
        participant_id (str): _description_
    """
    init_logging(file_name=str(log_file), level=logging.INFO)
    logger = logging.getLogger(__name__)
    crate = ROCrate(source=input_metadata)
    decrypted_result = crate.dereference(f"#{slugify(participant_id)}-sensitive")
    if decrypted_result:
        logger.info(  # pylint: disable=protected-access
            "\n\n ### \n Sensitive Json for %s is: \n %s",
            participant_id,
            decrypted_result._jsonld,  # pylint: disable=protected-access
        )
    else:
        logger.error("Participant sensitive not available")


cli.add_command(print_lab)
cli.add_command(abi)
cli.add_command(extract_participant_sensitive)

if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter

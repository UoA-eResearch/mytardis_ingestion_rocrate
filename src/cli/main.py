# pylint: disable=fixme, too-many-arguments, too-many-locals
"""
Script for CLI interface to invoke conversion of various input formats
into RO-Crates suitable for MyTardis ingestion
Should read input from a filepath or stdin and convert into metadata
and file objects recorded as an RO-Crate
"""

import logging
import os
import sys
import tarfile
import tempfile
import zipfile
from io import StringIO
from pathlib import Path
from typing import Optional

import bagit
import click
from rocrate.rocrate import ROCrate

from src.encryption.encrypt_metadata import Encryptor
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.profiles.profile_register import load_profile
from src.rocrate_builder.rocrate_builder import ROBuilder
from src.rocrate_dataclasses.data_class_utils import CrateManifest
from src.utils.log_utils import init_logging


def build_crate(
    crate_source: Path, crate_destination: Path, crate_contents: CrateManifest
) -> ROCrate:
    """Build an RO-Crate given a manifest of files

    Args:
        crate_source (Path): The soruce location, may contain an existing crate
        crate_destination (Path): where the RO-Crate is to be written
            -either location on disk if directly writing crate
            -or tmpfile location if crate is to be output as an archive
        crate_contents (CrateManifest): manifest of the RO-Crate

    Returns:
        ROCrate: _The RO-Crate object that has been written
    """
    crate = ROCrate()
    crate.source = crate_source
    builder = ROBuilder(crate)
    _ = [builder.add_project(project) for project in crate_contents.projcets]
    _ = [
        builder.add_experiment(experiment) for experiment in crate_contents.experiments
    ]
    _ = [builder.add_dataset(dataset) for dataset in crate_contents.datasets]
    _ = [builder.add_datafile(datafile) for datafile in crate_contents.datafiles]
    crate.write(crate_destination)
    return ROCrate


@click.command()
@click.option(
    "-i", "--input_metadata", help="compose file to work with", type=str, default=""
)
@click.option("--profile_name", type=str, default="print_lab_genomics")
@click.option("--encryption_key", type=str, multiple=True, default=[])
@click.option(
    "--log_file", type=click.Path(writable=True), default=Path("ingestion.log")
)
@click.option("--mt_hostname", type=str, default=None)
@click.option("--mt_user", type=str, default=None)
@click.option("--mt_api_key", type=str, default=None)
@click.option("-o", "--output", type=Path, default=None)
@click.option("-b", "--bag_crate", type=bool, default=True)
@click.option("-a", "--archive_type", type=str, default=None)
def main(
    input_metadata: str,
    profile_name: str,
    encryption_key: list[str],
    log_file: Path,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    output: Path,
    archive_type: Optional[str],
    bag_crate: Optional[bool],
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
    encryptor: Encryptor = Encryptor(encryption_key)
    options = {
        "encryptor": encryptor,
        "api_agent": api_agent,
    }

    profile = load_profile(profile_name)
    extractor = profile.get_extractor(options)

    ##STDin no loger makes sense - REMOVE THIS
    input_data_source: Path | StringIO | None = None
    if input_metadata == "-":
        input_data_source = StringIO(sys.stdin.read())
        # TODO check and sanitize input from stdin
    else:
        input_data_source = Path(input_metadata)
    crate_manifest = extractor.extract(input_data_source)

    if isinstance(input_data_source, Path) and Path(input_data_source).is_file():
        source_path = Path(input_data_source).parent
    else:
        source_path = input_data_source  # type: ignore

    crate_destination = output
    if archive_type:
        tmp_crate_location = (  # pylint: disable=consider-using-with
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )  # pylint: disable=consider-using-with
        crate_destination = Path(Path(tmp_crate_location.name) / source_path.name)
        crate_destination.mkdir()

    build_crate(
        crate_source=source_path,
        crate_destination=crate_destination,
        crate_contents=crate_manifest,
    )

    if bag_crate:
        bagit.make_bag(crate_destination, {"Contact-Name": mt_user}, processes=4)
    match archive_type:
        case "tar.gz":
            with tarfile.open(output, mode="w:bz2") as out_tar:
                out_tar.add(
                    crate_destination, arcname=crate_destination.name, recursive=True
                )
            out_tar.close()
        case "tar":
            with tarfile.open(output, mode="w") as out_tar:
                out_tar.add(
                    crate_destination, arcname=crate_destination.name, recursive=True
                )
            out_tar.close()
        case "zip":
            with zipfile.ZipFile(output, "w") as out_zip:
                for root, _, files in os.walk(crate_destination):
                    for filename in files:
                        arcname = (
                            crate_destination.name
                            / Path(root).relative_to(crate_destination)
                            / filename
                        )
                        logger.info("wirting to archived path %s", arcname)
                        out_zip.write(os.path.join(root, filename), arcname=arcname)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

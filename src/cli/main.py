# pylint: disable=fixme, too-many-arguments, too-many-locals
"""
Script for CLI interface to invoke conversion of various input formats
into RO-Crates suitable for MyTardis ingestion
Should read input from a filepath or stdin and convert into metadata
and file objects recorded as an RO-Crate
"""

import logging
import os
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import bagit
import click
from rocrate.rocrate import ROCrate

from src.encryption.encrypt_metadata import Encryptor
from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.profiles.profile_register import load_profile
from src.rocrate_builder.rocrate_builder import ROBuilder
from src.rocrate_dataclasses.data_class_utils import CrateManifest, reduce_to_dataset
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
    _ = [builder.add_project(project) for project in crate_contents.projcets.values()]
    _ = [
        builder.add_experiment(experiment)
        for experiment in crate_contents.experiments.values()
    ]
    _ = [builder.add_dataset(dataset) for dataset in crate_contents.datasets]
    print("What are your datafiles", crate_contents.datafiles)
    _ = [builder.add_datafile(datafile) for datafile in crate_contents.datafiles]
    crate.source = None
    crate.write(crate_destination)
    return ROCrate


@click.command()
@click.option(
    "-i",
    "--input_metadata",
    help="input file or directory to be converted into an RO-Crate",
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    default=os.getcwd(),
)
@click.option("--profile_name", type=str, default="print_lab_genomics")
@click.option("--encryption_key", type=str, multiple=True, default=[])
@click.option(
    "--log_file", type=click.Path(writable=True), default=Path("ingestion.log")
)
@click.option(
    "--mt_hostname",
    type=str,
    default=CONNECTION__HOSTNAME,
    help="hostname for MyTardis API",
)
@click.option("--mt_user", type=str, default=None, help="username for MyTardis API")
@click.option("--mt_api_key", type=str, default=None, help="API key for MyTardis API")
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
@click.option(
    "--collect_all",
    type=bool,
    is_flag=True,
    default=False,
    help="collect all values into MyTardis metadata.\n even those not found in schema",
)
def main(
    input_metadata: Path,
    profile_name: str,
    encryption_key: list[str],
    log_file: Path,
    mt_hostname: Optional[str],
    mt_user: Optional[str],
    mt_api_key: Optional[str],
    output: Path,
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
        "collect_all": collect_all,
    }

    profile = load_profile(profile_name)
    extractor = profile.get_extractor(options)

    crate_manifest = extractor.extract(input_metadata)

    if split_datasets:
        crates = {
            dataset.directory: reduce_to_dataset(crate_manifest, dataset)
            for dataset in crate_manifest.datasets
        }
    else:
        crates = {Path(""): crate_manifest}

    for crate_dir, crate_objs in crates.items():
        source_path = input_metadata
        if Path(input_metadata).is_file():
            source_path = Path(input_metadata).parent

        logger.debug("outputting crate %s", crate_dir)

        final_output = output / crate_dir
        if not final_output.parent.exists():
            logger.debug("trying to write %s", final_output)
            final_output.parent.mkdir(parents=True)
        crate_destination = final_output
        if archive_type:
            tmp_crate_location = (  # pylint: disable=consider-using-with
                tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
            )  # pylint: disable=consider-using-with
            crate_destination = Path(Path(tmp_crate_location.name) / source_path.name)
            crate_destination.mkdir()

        build_crate(
            crate_source=source_path / crate_dir,
            crate_destination=crate_destination,
            crate_contents=crate_objs,
        )

        if bag_crate:
            bagit.make_bag(crate_destination, {"Contact-Name": mt_user}, processes=4)
        match archive_type:
            case "tar.gz":
                with tarfile.open(
                    final_output.parent / (final_output.name + ".tar.gz"), mode="w:bz2"
                ) as out_tar:
                    out_tar.add(
                        crate_destination,
                        arcname=crate_destination.name,
                        recursive=True,
                    )
                out_tar.close()
            case "tar":
                with tarfile.open(
                    final_output.parent / (final_output.name + ".tar"), mode="w"
                ) as out_tar:
                    out_tar.add(
                        crate_destination,
                        arcname=crate_destination.name,
                        recursive=True,
                    )
                out_tar.close()
            case "zip":
                with zipfile.ZipFile(
                    final_output.parent / (final_output.name + ".zip"), "w"
                ) as out_zip:
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

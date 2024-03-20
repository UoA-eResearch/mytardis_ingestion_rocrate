"""Functions for witing and archiving RO-Crates on disk
"""

import logging
import os
import tarfile
import zipfile
from pathlib import Path

import bagit
from rocrate.rocrate import ROCrate

from src.rocrate_builder.rocrate_builder import ROBuilder
from src.rocrate_dataclasses.data_class_utils import CrateManifest

logger = logging.getLogger(__name__)

PROCESSES = 8


def write_crate(
    crate_source: Path,
    crate_destination: Path,
    crate_contents: CrateManifest,
    meta_only: bool = True,
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
    logger.info("Initalizing crate")
    crate = ROCrate()
    crate.source = crate_source
    builder = ROBuilder(crate)
    logger.info("adding projects")
    _ = [builder.add_project(project) for project in crate_contents.projcets.values()]
    logger.info("adding experiments")
    _ = [
        builder.add_experiment(experiment)
        for experiment in crate_contents.experiments.values()
    ]
    logger.info("adding datasets")
    _ = [builder.add_dataset(dataset) for dataset in crate_contents.datasets]
    logger.info("adding datafiles")
    _ = [builder.add_datafile(datafile) for datafile in crate_contents.datafiles]
    # crate.source = None
    logger.info(
        "writing crate metadata and moving from %s files to %s",
        crate_source,
        crate_destination,
    )
    if not crate_destination.exists():
        crate_destination.mkdir(parents=True)
    if meta_only:
        crate.metadata.write(crate_destination)
        return ROCrate
    crate.write(crate_destination)
    return ROCrate


def bagit_crate(crate_path: Path, contact_name: str) -> None:
    """Put an RO-Crate into a bagit archive, moving all contents down one directory

    Args:
        crate_path (Path): location of the RO-Crate
        contact_name (str): contact name listed on the RO-Crate
    """
    bagit.make_bag(crate_path, {"Contact-Name": contact_name}, processes=PROCESSES)


def archive_crate(
    archive_type: str | None, output_location: Path, crate_location: Path
) -> None:
    """Archive the RO-Crate as a TAR, GZIPPED TAR or ZIP archive

    Args:
        archive_type (str | None): the archive format [tar.gz, tar, or zip]
        output_location (Path): the path where the archive should be written to
        crate_location (Path): the path of the RO-Crate to be archived
    """

    if not archive_type:
        return
    match archive_type:
        case "tar.gz":
            logger.info("Tar GZIP archiving %s", crate_location.name)
            with tarfile.open(
                output_location.parent / (output_location.name + ".tar.gz"),
                mode="w:bz2",
            ) as out_tar:
                out_tar.add(
                    crate_location,
                    arcname=crate_location.name,
                    recursive=True,
                )
            out_tar.close()
        case "tar":
            logger.info("Tar archiving %s", crate_location.name)
            with tarfile.open(
                output_location.parent / (output_location.name + ".tar"), mode="w"
            ) as out_tar:
                out_tar.add(
                    crate_location,
                    arcname=crate_location.name,
                    recursive=True,
                )
            out_tar.close()
        case "zip":
            logger.info("zip archiving %s", crate_location.name)
            with zipfile.ZipFile(
                output_location.parent / (output_location.name + ".zip"), "w"
            ) as out_zip:
                for root, _, files in os.walk(crate_location):
                    for filename in files:
                        arcname = (
                            crate_location.name
                            / Path(root).relative_to(crate_location)
                            / filename
                        )
                        logger.info("wirting to archived path %s", arcname)
                        out_zip.write(os.path.join(root, filename), arcname=arcname)

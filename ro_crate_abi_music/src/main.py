"""Scripts to read in a JSON file from a directory, get project and sample information
from the parent and grandparent directories and creata an RO crate in a second location
"""

import argparse
import os
import tarfile
from pathlib import Path
from typing import Optional, Tuple

from rocrate.rocrate import ROCrate

from ro_crate_abi_music.src.json_parser.abi_json_parser import (
    combine_json_files,
    process_dataset,
    process_experiment,
    process_project,
)
from ro_crate_abi_music.src.rocrate_builder.rocrate_builder import ROBuilder
from ro_crate_abi_music.src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Project,
)


def read_jsons(
    filepath: Path,
    sample_filename: Optional[str] = None,
    project_filename: Optional[str] = None,
    crate_name: Optional[Path] = None,
) -> Tuple[Project, Experiment, Dataset, Path]:
    """Process the input files into a set of dataclasses

    Args:
        filepath (Path): The JSON for the dataset
        sample_filename (Optional[str], optional): The name of the experiment JSON. Defaults to None.
        project_filename (Optional[str], optional): The name of the project JSON. Defaults to None.
        crate_name (Optional[Path], optional): Where the crate should be created. Defaults to None.

    Returns:
        Tuple[Project,Experiment,Dataset,Path]: The resulting Project, Experiment, Dataset and
            crate name. The latter is included for cases where there is no crate name given and
            one needs to be constructed
    """
    if not filepath.is_file():
        raise FileNotFoundError("Please enter the filepath for a dataset JSON file")
    if not sample_filename:
        sample_filename = "experiment.json"
    if not project_filename:
        project_filename = "project.json"
    current_directory = (
        filepath.parent if filepath.parent.as_posix() != "." else Path(os.getcwd())
    )
    sample_directory = current_directory.parent
    project_directory = sample_directory.parent
    file_list = [
        filepath,
        sample_directory / Path(sample_filename),
        project_directory / Path(project_filename),
    ]
    json_dict = combine_json_files(file_list)
    project = process_project(json_dict)
    experiment = process_experiment(json_dict)
    dataset = process_dataset(json_dict, current_directory)
    if not crate_name:
        crate_name = Path(f"rocrate-{dataset.experiment}-{dataset.name}")
    return (project, experiment, dataset, crate_name)


def create_rocrate(
    project: Project,
    experiment: Experiment,
    dataset: Dataset,
    crate_name: Path,
) -> None:
    """Read in a filepath as a directory and process the data for packaging into an RO crate

    Args:
        filepath (Path): The JSON file for a dataset
    """
    crate = ROCrate()
    builder = ROBuilder(crate)
    _ = builder.add_project(project)
    experiment_obj = builder.add_experiment(experiment)
    _ = builder.add_dataset(dataset, experiment_obj)
    crate.datePublished = dataset.created_date  # hack to get the RO-Crate date
    crate.write(crate_name)


def package_rocrate(
    crate_name: Path,
    compress: bool = False,
) -> None:
    """Package up the RO-Crate as a tar file

    Args:
        filepath (Path): The location of the root dir of the ro-crate to package
        compress (bool): A flag to indicate whether or not to compress the box,
            default False
    """
    mode = "w:gz" if compress else "w"

    crate_out_name = (
        Path(f"{crate_name.as_posix()}.tar.gz")
        if compress
        else Path(f"{crate_name.as_posix()}.tar")
    )
    with tarfile.open(crate_out_name, mode) as tar:
        for in_file in crate_name.glob("**/*"):
            if in_file.is_file():
                tar.add(in_file, arcname=in_file.relative_to(crate_name))


parser = argparse.ArgumentParser(description="Package a dataset into an ROCrate")
parser.add_argument(
    "filepath",
    type=str,
    help="The filepath to a JSON file for a dataset",
)
parser.add_argument(
    "-s",
    "--sample-file",
    type=str,
    help="The name of the sample JSON relating to the dataset, default='experiment.json'",
)
parser.add_argument(
    "-p",
    "--project-file",
    type=str,
    help="The name of the project JSON relating to the dataset, default='project.json'",
)
parser.add_argument(
    "-o",
    "--output-path",
    type=str,
    help="The filepath to the location to write the ROCrate",
)
parser.add_argument(
    "-c",
    "--compress",
    action="store_true",
    default=False,
    help="Compress the tar file generated using GZip",
)
parser.add_argument(
    "-d",
    "--directory",
    action="store_true",
    default=False,
    help=(
        "Search all child directories of this directory for datasets to create an RO"
        "-Crate for"
    ),
)

args = parser.parse_args()


if args.output_path:
    args.output_path = Path(args.output_path)
in_files = [Path(args.filepath)]
if args.directory:
    in_files = list(Path(args.filepath).glob("*/*.json"))
print(in_files)
for in_file in in_files:
    (
        ro_project,
        ro_experiment,
        ro_dataset,
        ro_crate_name,
    ) = read_jsons(
        in_file,
        sample_filename=args.sample_file,
        project_filename=args.project_file,
        crate_name=args.output_path,
    )
    create_rocrate(
        ro_project,
        ro_experiment,
        ro_dataset,
        ro_crate_name,
    )
    package_rocrate(
        ro_crate_name,
        args.compress,
    )

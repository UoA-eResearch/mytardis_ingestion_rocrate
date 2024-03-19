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


# def read_jsons(
#     filepath: Path,
#     sample_filename: Optional[str] = None,
#     project_filename: Optional[str] = None,
#     crate_name: Optional[Path] = None,
#     ) -> Tuple[Project, Experiment, Dataset, Path]:
#     """Process the input files into a set of dataclasses

#     Args:
#         filepath (Path): The JSON for the dataset
#         sample_filename (Optional[str], optional):
#             The name of the experiment JSON. Defaults to None.
#         project_filename (Optional[str], optional):
#             he name of the project JSON. Defaults to None.
#         ro_crate_name (Optional[Path], optional):
#             Where the crate should be created. Defaults to None.

#     Returns:
#         Tuple[Project,Experiment,Dataset,Path]:
#         The resulting Project, Experiment, Dataset and
#             crate name. The latter is included for cases
#             where there is no crate name given and
#             one needs to be constructed
#     """
#     if not filepath.is_file():
#         raise FileNotFoundError("Please enter the filepath for a dataset JSON file")
#     if not sample_filename:
#         sample_filename = "experiment.json"
#     if not project_filename:
#         project_filename = "project.json"
#     current_directory = (
#         filepath.parent if filepath.parent.as_posix() != "." else Path(os.getcwd())
#     )
#     print(current_directory)
#     sample_directory = current_directory.parent
#     project_directory = sample_directory.parent
#     file_list = [
#         filepath,
#         sample_directory / Path(sample_filename),
#         project_directory / Path(project_filename),
#     ]
#     print(file_list)
#     json_dict = combine_json_files(file_list)
#     project = process_project(json_dict)
#     experiment = process_experiment(json_dict)
#     dataset = process_dataset(json_dict, current_directory)
#     if not crate_name:
#         crate_name = Path(f"rocrate-{dataset.experiment}-{dataset.name}")
#     return (project, experiment, dataset, crate_name)


#     def extract(self, input_data_source: Any) -> CrateManifest:
#         #if input_data_source is a file get dir
#         #if input_data_source is a dir get children
#         if Path(input_data_source).is_dir():
#             dataset_filename = input_data_source / "dataset.json"
#             project_filename = input_data_source / "project.json"
#             sample_filename = input_data_source / "experiment.json"

#             child_dirs = [
#             child_dir for child_dir in source_path.iterdir if child_dir.is_dir()
#             ]
#             jsons_list = [list(child_dir.glob("*.json")) for child_dir in child_dirs]
#             file_list = [item for in_file in jsons_list for item in in_file]
#             for json_file in file_list:
#                 print(json_file)
#                 crate_name = args.output_path / json_file.relative_to(args.filepath).parent
#                 (
#                     ro_project,
#                     ro_experiment,
#                     ro_dataset,
#                     ro_crate_name,
#                 ) = read_jsons(
#                     json_file,
#                     sample_filename=args.sample_file,
#                     project_filename=args.project_file,
#                     crate_name=crate_name,
#                 )
#         else:
#                 (
#                     ro_project,
#                     ro_experiment,
#                     ro_dataset,
#                     ro_crate_name,
#                 ) = read_jsons(
#                     args.filepath,
#                     sample_filename=args.sample_file,
#                     project_filename=args.project_file,
#                     crate_name=args.output_path,
#                 )


#         return CrateManifest()

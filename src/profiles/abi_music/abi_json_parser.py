"""A class that reads in a dataset, experiment and project JSON file and parses
them into data classes that can be used for creating an RO-crate"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from slugify import slugify

from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    create_metadata_objects,
)
from src.mt_api.mt_consts import MtObject
from src.profiles.abi_music.consts import (  # ZARR_DATASET_NAMESPACE,
    ABI_FACILLITY,
    ABI_MUSIC_MICROSCOPE_INSTRUMENT,
)
from src.profiles.abi_music.filesystem_nodes import DirectoryNode, FileNode
from src.rocrate_builder.rocrate_writer import bagit_crate, write_crate
from src.rocrate_dataclasses.data_class_utils import CrateManifest
from src.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
    Instrument,
    Project,
)
from src.user_lookup.user_lookup import create_person_object

datetime_pattern = re.compile("^[0-9]{6}-[0-9]{6}$")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_timestamp(timestamp: str) -> datetime:
    """
    Parse a timestamp string in the ABI Music format: yymmdd-DDMMSS

    Returns a datetime object or raises a ValueError if the string is ill-formed.
    """
    # strptime is a bit too lenient with its input format, so pre-validate with a regex
    if _ := datetime_pattern.match(timestamp):
        return datetime.strptime(timestamp, r"%y%m%d-%H%M%S")
    raise ValueError("Ill-formed timestamp; expected format 'yymmdd-DDMMSS'")


def combine_json_files(
    files: List[Path],
) -> Dict[str, str | List[str] | Dict[str, str]]:
    """Take a list of file paths for JSON files and combine these into a single dictionary
    to return.

    Note no error handling is done by this function. It is important to ensure that the input
    files are JSON files

    Args:
        files (List[Path]): A list of Path objects to JSON files, typically a dataset, experiment
            and project JSON
    """
    return_dict = {}
    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as json_file:
            json_dict = json.loads(json_file.read())
            return_dict.update(json_dict)
    return return_dict


def process_project(
    project_dir: DirectoryNode,
    metadata_schema: Dict[str, Dict[str, Any]],
    collect_all: bool = False,
) -> Project:
    """Extract the project specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str,str | List[str] | Dict[str,str]]): The dictionary that
            contains the project necessary information

    Returns:
        Project: A project dataclass
    """
    json_dict = read_json(project_dir.file("project.json"))
    metadata_dict = create_metadata_objects(json_dict, metadata_schema, collect_all)
    principal_investigator = create_person_object(
        str(json_dict["principal_investigator"])
    )
    contributors = [
        create_person_object(contributor)
        for contributor in json_dict["contributors"]
        if json_dict["contributors"]
    ]
    identifiers: list[str | int | float] = [
        slugify(identifier) for identifier in json_dict["project_ids"]
    ]
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Project(
        name=str(json_dict["project_name"]),
        description=str(json_dict["project_description"]),
        principal_investigator=principal_investigator,
        contributors=contributors or None,
        identifiers=identifiers,
        metadata=metadata_dict,
        date_created=None,
        date_modified=None,
        mytardis_classification=None,
        ethics_policy=None,
        additional_properties=additional_properties,
        schema_type="Project",
    )


def process_experiment(
    experiment_dir: DirectoryNode,
    metadata_schema: Dict[str, Any],
    project_id: str,
    collect_all: bool = False,
) -> Experiment:
    """Extract the experiment specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            conatins the experiment necessary information

    Returns:
        Experiment: An experiment dataclass
    """
    json_dict = read_json(experiment_dir.file("experiment.json"))
    metadata_dict = create_metadata_objects(json_dict, metadata_schema, collect_all)
    identifiers: list[str | int | float] = [
        slugify(f'{json_dict["project"]}-{identifier}')
        for identifier in json_dict["experiment_ids"]
    ]
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Experiment(
        name=json_dict["experiment_name"],
        description=json_dict["experiment_description"],
        projects=[slugify(json_dict["project"])]
        if json_dict.get("project")
        else [project_id],
        identifiers=identifiers,
        metadata=metadata_dict,
        date_created=None,
        date_modified=None,
        contributors=None,
        mytardis_classification=None,
        participant=None,
        additional_properties=additional_properties,
        schema_type="DataCatalog",
    )


def process_raw_dataset(
    dataset_dir: DirectoryNode,
    metadata_schema: Dict[str, Dict[str, Any]],
    experiment_id: str,
    collect_all: bool = False,
) -> Dataset:
    """Extract the dataset specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            conatins the dataset necessary information

    Returns:
        Dataset: An dataset dataclass
    """
    json_dict = read_json(dataset_dir.file(dataset_dir.name() + ".json"))
    metadata_dict = create_metadata_objects(json_dict, metadata_schema, collect_all)

    named_fields: dict[str, Any] = {
        "full-description": json_dict["Description"],
        "sequence-id": json_dict["SequenceID"],
        "sqrt-offset": json_dict["Offsets"]["SQRT Offset"],
    }
    metadata_dict = metadata_dict | create_metadata_objects(
        named_fields, metadata_schema, False
    )

    identifiers = [
        slugify(
            (
                f'{json_dict["Basename"]["Project"]}-{json_dict["Basename"]["Sample"]}-'
                f'{json_dict["Basename"]["Sequence"]}'
            )
        ),
        json_dict["SequenceID"],
    ]
    updated_dates = []
    for index, session in enumerate(json_dict["Sessions"]):
        if index == 0:
            created_date = parse_timestamp(session["Session"])
        else:
            updated_dates.append(parse_timestamp(session["Session"]))
    if slugify(json_dict["Basename"]["Sample"]) not in experiment_id:
        logger.warning(
            "Experiment ID does not match parent for dataset %s", identifiers[0]
        )
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Dataset(
        name=identifiers[0],
        description=json_dict["Description"],
        identifiers=identifiers,
        experiments=[
            slugify(f'{json_dict["project_ids"][0]}-{json_dict["experiment_ids"][0]}')
        ]
        if json_dict.get("experiment_ids")
        else [experiment_id],
        directory=dataset_dir.path(),
        date_created=created_date,
        date_modified=updated_dates or None,
        metadata=metadata_dict,
        contributors=None,
        instrument=Instrument(
            name=ABI_MUSIC_MICROSCOPE_INSTRUMENT,
            description=ABI_MUSIC_MICROSCOPE_INSTRUMENT,
            identifiers=[ABI_MUSIC_MICROSCOPE_INSTRUMENT, ABI_FACILLITY],
            date_created=None,
            date_modified=None,
            metadata=None,
            location=ABI_FACILLITY,
            additional_properties={},
            schema_type="Thing",
        ),
        additional_properties=additional_properties,
        schema_type="Dataset",
    )


# def process_zarr_dataset()


# def process_datafile()


def read_json(file: FileNode) -> dict[str, Any]:
    """Extract the JSON data hierachy from `file`"""
    file_data = file.path().read_text(encoding="utf-8")
    json_data: dict[str, Any] = json.loads(file_data)
    return json_data


def parse_raw_data(  # pylint: disable=too-many-locals
    raw_dir: DirectoryNode,
    # file_filter: filters.PathFilterSet,
    metadata_handler: MetadataHanlder,
    collect_all: bool = False,
    write_datasets: bool = True,
) -> CrateManifest:
    """
    Parse the directory containing the raw data
    Modified from Mytardis_ingestion code as it expects the same file structures
    """

    crate_manifest = CrateManifest()
    project_metadata_schema = metadata_handler.get_mtobj_schema(MtObject.PROJECT)
    experiment_metadata_schema = metadata_handler.get_mtobj_schema(MtObject.EXPERIMENT)
    raw_dataset_metadata_schema = metadata_handler.get_mtobj_schema(MtObject.DATASET)
    # zarr_dataset_metadata_scheam =
    #  metadata_handler.request_metadata_schema(ZARR_DATASET_NAMESPACE)
    # datafile_metadata_schema = metadata_handler.get_mtobj_schema(MtObject.DATAFILE)
    project_dirs = [
        d for d in raw_dir.iter_dirs(recursive=True) if d.has_file("project.json")
    ]
    for project_dir in project_dirs:
        logging.info("Project directory: %s", project_dir.name())
        project = process_project(
            project_dir=project_dir,
            metadata_schema=project_metadata_schema,
            collect_all=collect_all,
        )
        crate_manifest.add_projects(projcets={str(project.id): project})

        experiment_dirs = [
            d
            for d in project_dir.iter_dirs(recursive=True)
            if d.has_file("experiment.json")
        ]

        for experiment_dir in experiment_dirs:
            logging.info("Experiment directory: %s", experiment_dir.name())

            experiment = process_experiment(
                experiment_dir,
                metadata_schema=experiment_metadata_schema,
                project_id=str(project.id),
                collect_all=collect_all,
            )
            crate_manifest.add_experiments({str(experiment.id): experiment})

            dataset_dirs = [
                d
                for d in experiment_dir.iter_dirs(recursive=True)
                if d.has_file(d.name() + ".json")
            ]
            for dataset_dir in dataset_dirs:
                logging.info("Dataset directory: %s", dataset_dir.name())

                dataset = process_raw_dataset(
                    dataset_dir,
                    raw_dataset_metadata_schema,
                    experiment_id=str(experiment.id),
                    collect_all=collect_all,
                )

                data_dir = next(
                    d
                    for d in dataset_dir.iter_dirs()
                    if datetime_pattern.match(d.path().stem)
                )

                dataset.date_created = parse_timestamp(data_dir.name())

                crate_manifest.add_datasets([dataset])
                if write_datasets:
                    logging.info("Writing Crate for: %s", dataset_dir.name())
                    dataset_manifest = CrateManifest(
                        projcets={str(project.id): project},
                        experiments={str(experiment.id): experiment},
                        datasets=[dataset],
                        datafiles=None,
                    )
                    write_crate(dataset_dir.path(), data_dir.path(), dataset_manifest)
                    logging.info("Bagging Crate for: %s", dataset_dir.name())
                    bagit_crate(
                        data_dir.path(),
                        contact_name=project.principal_investigator.name,
                    )
                    dataset.directory = Path("data") / dataset.directory

                # for file in dataset_dir.iter_files(recursive=True):
                #     if file_filter.exclude(file.path()):
                #         continue

                #     datafile = collate_datafile_info(file, root_dir, dataset_id)
                #     pedd_builder.add_datafile(datafile)

    return crate_manifest

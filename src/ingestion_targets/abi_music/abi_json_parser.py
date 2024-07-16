"""A class that reads in a dataset, experiment and project JSON file and parses
them into data classes that can be used for creating an RO-crate"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mytardis_rocrate_builder.rocrate_builder import ROBuilder
from mytardis_rocrate_builder.rocrate_dataclasses.data_class_utils import (
    convert_to_property_value,
)
from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import CrateManifest
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    ContextObject,
    Dataset,
    Experiment,
    Instrument,
    Project,
)
from mytardis_rocrate_builder.rocrate_writer import bagit_crate, write_crate
from rocrate.rocrate import ROCrate
from slugify import slugify

from src.ingestion_targets.abi_music.consts import (  # ZARR_DATASET_NAMESPACE,
    ABI_FACILLITY,
    ABI_MUSIC_MICROSCOPE_INSTRUMENT,
)
from src.ingestion_targets.abi_music.filesystem_nodes import DirectoryNode, FileNode
from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    create_metadata_objects,
)
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MtObject

datetime_pattern = re.compile("^[0-9]{6}-[0-9]{6}$")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    api_agent: MyTardisRestAgent,
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
    principal_investigator = api_agent.create_person_object(
        str(json_dict["principal_investigator"])
    )
    contributors = [
        api_agent.create_person_object(contributor)
        for contributor in json_dict["contributors"]
        if json_dict["contributors"]
    ]
    identifiers: list[str | int | float] = [
        slugify(identifier) for identifier in json_dict["project_ids"]
    ]
    metadata_dict = create_metadata_objects(
        json_dict, metadata_schema, collect_all, identifiers[0]
    )
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
        additional_properties=additional_properties,
        schema_type="Project",
        acls=None,
    )


def process_experiment(
    experiment_dir: DirectoryNode,
    metadata_schema: Dict[str, Any],
    parent_project_id: str,
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
    project_ids = (
        [json_dict["project"]] if json_dict.get("project") else json_dict["projects"]
    )
    identifiers: list[str | int | float] = [
        slugify(f"{project_ids}-{identifier}")
        for identifier in json_dict["experiment_ids"]
        for project_id in project_ids
    ]
    metadata_dict = create_metadata_objects(
        json_dict, metadata_schema, collect_all, identifiers[0]
    )
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Experiment(
        name=json_dict["experiment_name"],
        description=json_dict["experiment_description"],
        projects=project_ids if project_ids else [parent_project_id],
        identifiers=identifiers,
        metadata=metadata_dict,
        date_created=None,
        date_modified=None,
        contributors=None,
        mytardis_classification=None,
        additional_properties=additional_properties,
        schema_type="DataCatalog",
        acls=None,
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
    identifiers = [
        slugify(
            (
                f'{json_dict["Basename"]["Project"]}-{json_dict["Basename"]["Sample"]}-'
                f'{json_dict["Basename"]["Sequence"]}'
            )
        ),
        json_dict["SequenceID"],
    ]
    metadata_dict = create_metadata_objects(
        json_dict, metadata_schema, collect_all, identifiers[0]
    )
    metadata_dict = metadata_dict | create_metadata_objects(
        {
            "full-description": json_dict["Description"],
            "sequence-id": json_dict["SequenceID"],
            "sqrt-offset": json_dict["Offsets"]["SQRT Offset"],
        },
        metadata_schema,
        False,
        identifiers[0],
    )
    updated_dates = []

    if slugify(json_dict["Basename"]["Sample"]) not in experiment_id:
        logger.warning(
            "Experiment ID does not match parent for dataset %s", identifiers[0]
        )
    additional_properties = {}

    if collect_all:
        additional_properties = json_dict
    created_date = None
    additional_properties["Sessions"] = []
    for index, session in enumerate(json_dict["Sessions"]):
        session_id = slugify(
            f'{identifiers[0]}-"session"-{index}-{session["SessionID"]}'
        )
        session_object = process_nested_contextobj(
            session, session_id, "MedicalImagingTechnique"
        )
        session_date = parse_timestamp(session["Session"])
        session_object.date_created = session_date
        session_object.date_modified = [session_date]
        if index == 0:
            created_date = session_date
        else:
            updated_dates.append(session_date)
        additional_properties["Sessions"].append(session_object)
    additional_properties["Offsets"] = convert_to_property_value(
        json_element=json_dict["Offsets"], name="Offsets"
    )
    additional_properties["Camera Settings"] = convert_to_property_value(
        json_element=json_dict["Camera Settings"], name="Camera Settings"
    )
    additional_properties["data root path"] = dataset_dir.path().as_posix()
    identifiers.append(dataset_dir.path().as_posix())
    return Dataset(
        name=identifiers[0],
        description=json_dict["Description"],
        identifiers=identifiers,
        experiments=(
            [slugify(f'{json_dict["project_ids"][0]}-{json_dict["experiment_ids"][0]}')]
            if json_dict.get("experiment_ids")
            else [experiment_id]
        ),
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
            location=ABI_FACILLITY,
            additional_properties={},
            schema_type="Thing",
        ),
        additional_properties=additional_properties,
        schema_type="Dataset",
        acls=None,
    )


def process_nested_contextobj(
    sub_json: Dict[str, Any], identifier: str, schema_type: str
) -> ContextObject:
    """Create a context object from the subset of the json data

    Args:
        sub_json (Dict[str,Any]): the subset of the json data
        identifier (str): the identifier of this context object
        schema_type (str): the object's type in schema.org

    Returns:
        ContextObject: the restultant context object
    """
    return ContextObject(
        name=identifier,
        description=str(
            sub_json.get("description") if sub_json.get("description") else identifier
        ),
        identifiers=[identifier],
        date_created=None,
        date_modified=None,
        additional_properties=convert_to_property_value(sub_json, identifier),
        schema_type=schema_type,
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
    api_agent: MyTardisRestAgent,
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
    project_dirs = [
        d for d in raw_dir.iter_dirs(recursive=True) if d.has_file("project.json")
    ]
    for project_dir in project_dirs:
        logging.info("Project directory: %s", project_dir.name())
        project = process_project(
            project_dir=project_dir,
            metadata_schema=project_metadata_schema,
            collect_all=collect_all,
            api_agent=api_agent,
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
                parent_project_id=str(project.id),
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
                    (
                        d
                        for d in dataset_dir.iter_dirs()
                        if datetime_pattern.match(d.path().stem)
                    ),
                    None,
                )

                dataset.date_created = (
                    parse_timestamp(data_dir.name()) if data_dir else None
                )

                crate_manifest.add_datasets([dataset])
                if write_datasets and not dataset_dir.has_file("bagit.txt"):
                    logging.info("Writing Crate for: %s", dataset_dir.name())
                    projects = {
                        project_id: crate_manifest.projcets[project_id]
                        for project_id in experiment.projects
                        if crate_manifest.projcets.get(project_id)
                    }
                    projects[str(project.id)] = project
                    dataset_manifest = CrateManifest(
                        projcets={str(project.id): project},
                        experiments={str(experiment.id): experiment},
                        datasets=[dataset],
                        datafiles=None,
                    )
                    dataset.directory = Path("./")
                    dataset.identifiers[0] = "./"
                    crate = ROCrate()
                    crate.source = dataset_dir.path()
                    builder = ROBuilder(crate)
                    write_crate(
                        builder=builder,
                        crate_destination=dataset_dir.path(),
                        crate_source=dataset_dir.path(),
                        crate_contents=dataset_manifest,
                    )
                    logging.info("Bagging Crate for: %s", dataset_dir.name())
                    bagit_crate(
                        dataset_dir.path(),
                        contact_name=project.principal_investigator.name,
                    )
                    dataset.directory = Path("data") / dataset.directory
    return crate_manifest

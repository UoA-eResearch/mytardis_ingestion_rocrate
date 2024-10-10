"""A class that reads in a dataset, experiment and project JSON file and parses
them into data classes that can be used for creating an RO-crate"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mytardis_rocrate_builder.rocrate_dataclasses.crate_manifest import CrateManifest
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    ContextObject,
    Dataset,
    Experiment,
    Instrument,
    MTMetadata,
    Project,
)
from slugify import slugify

from src.ingestion_targets.abi_music.consts import (  # ZARR_DATASET_NAMESPACE,
    ABI_FACILLITY,
    ABI_MUSIC_MICROSCOPE_INSTRUMENT,
)
from src.ingestion_targets.abi_music.filesystem_nodes import DirectoryNode, FileNode
from src.metadata_extraction.metadata_extraction import (
    MetadataHandlder,
    MetadataSchema,
    create_metadata_objects,
)
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import UOA, MtObject

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
    json_file = project_dir.file("project.json")
    json_dict = read_json(json_file)
    principal_investigator = api_agent.create_person_object(
        str(json_dict["principal_investigator"])
    )
    contributors = [
        api_agent.create_person_object(contributor)
        for contributor in json_dict["contributors"]
        if json_dict["contributors"]
    ]
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Project(
        name=str(json_dict["project_name"]),
        description=str(json_dict["project_description"]),
        principal_investigator=principal_investigator,
        contributors=contributors or None,
        date_created=None,
        date_modified=None,
        additional_properties=additional_properties,
        institution=UOA,
        schema_type="Project",
    )


def process_experiment(
    experiment_dir: DirectoryNode,
    parent_project: Project,
    crate_manifest: CrateManifest,
    collect_all: bool = False,
) -> Experiment:
    """Extract the experiment specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            contains the experiment necessary information

    Returns:
        Experiment: An experiment dataclass
    """
    json_file = experiment_dir.file("experiment.json")
    json_dict = read_json(json_file)
    project_ids = (
        [json_dict["project"]] if json_dict.get("project") else json_dict["projects"]
    )
    projects = [parent_project]
    for project_id in project_ids:
        if project_found := crate_manifest.projects.get(project_id):
            if parent_project.id != project_found.id:
                projects.append(project_found)
    additional_properties = {}
    if collect_all:
        additional_properties = json_dict
    return Experiment(
        name=json_dict["experiment_name"],
        description=json_dict["experiment_description"],
        projects=projects,
        date_created=None,
        date_modified=json_file.stat().st_mtime,
        contributors=None,
        mytardis_classification=None,
        additional_properties=additional_properties,
    )


def collect_dataset_metadata(
    dataset_dir: DirectoryNode,
    parent_dataset: Dataset,
    metadata_schema: MetadataSchema,
    collect_all: bool = False,
) -> Dict[str, MTMetadata]:
    """Collect metadata from an abi dataset json

    Args:
        dataset_dir (DirectoryNode): the directory containing the dataset information
        dataset_parent (Dataset): the parent dataset object
        metadata_schema (MetadataSchema): the schema the dataset metadata uses
        collect_all (bool, optional): collect all information from this dataset,
            defaults to False.

    Returns:
        Dict[str,MTMetadata]: the metadata as collected.
    """
    json_dict = read_json(dataset_dir.file(dataset_dir.name() + ".json"))
    metadata_dict = create_metadata_objects(
        json_dict, metadata_schema, collect_all, parent_dataset
    )
    metadata_dict = metadata_dict | create_metadata_objects(
        {
            "full-description": json_dict["Description"],
            "sequence-id": json_dict["SequenceID"],
            "sqrt-offset": json_dict["Offsets"]["SQRT Offset"],
        },
        metadata_schema,
        False,
        parent_dataset,
    )
    return metadata_dict


def process_raw_dataset(  # pylint: disable=too-many-locals
    dataset_dir: DirectoryNode,
    experiment: Experiment,
    crate_manifest: CrateManifest,
) -> Dataset:
    """Extract the dataset specific information out of a JSON dictionary

    Args:
        json_dict (Dict[str, str  |  List[str]  |  Dict[str, str]]): The dictionary that
            contains the dataset necessary information

    Returns:
        Dataset: An dataset dataclass
    """
    json_dict = read_json(dataset_dir.file(dataset_dir.name() + ".json"))
    experiment_id = slugify(json_dict["Basename"]["Sample"])
    identifiers = [
        slugify(
            (
                f'{json_dict["Basename"]["Project"]}-{experiment_id}-'
                f'{json_dict["Basename"]["Sequence"]}'
            )
        ),
        json_dict["SequenceID"],
    ]

    updated_dates: List[datetime] = []

    experiments = [experiment]
    if slugify(experiment_id) not in experiment.id:
        logger.warning(
            "Experiment ID does not match parent for dataset %s", identifiers[0]
        )
        if found_experiment := crate_manifest.experiments.get(experiment_id):
            experiments.append(found_experiment)

    created_date = None
    additional_properties: Dict[str, Any] = {}

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
    additional_properties["Offsets"] = json_dict["Offsets"]
    additional_properties["Camera Settings"] = json_dict["Camera Settings"]
    additional_properties["data root path"] = dataset_dir.path().as_posix()
    identifiers.append(dataset_dir.path().as_posix())
    logger.debug("dataset dir is: %s", dataset_dir.path())
    return Dataset(
        name=identifiers[0],
        description=json_dict["Description"],
        experiments=experiments,
        directory=dataset_dir.path().relative_to(os.getcwd()),
        date_created=created_date,
        date_modified=updated_dates or None,
        contributors=None,
        instrument=Instrument(
            name=ABI_MUSIC_MICROSCOPE_INSTRUMENT,
            description=ABI_MUSIC_MICROSCOPE_INSTRUMENT,
            date_created=None,
            date_modified=None,
            location=ABI_FACILLITY,
            additional_properties=additional_properties,
            schema_type="Thing",
        ),
        additional_properties=None,
        schema_type="Dataset",
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
        date_created=None,
        date_modified=None,
        additional_properties=sub_json,
        schema_type=schema_type,
    )


# def process_zarr_dataset()


# def process_datafile()


def read_json(file: FileNode) -> dict[str, Any]:
    """Extract the JSON data hierarchy from `file`"""
    file_data = file.path().read_text(encoding="utf-8")
    json_data: dict[str, Any] = json.loads(file_data)
    return json_data


def parse_raw_data(  # pylint: disable=too-many-locals,too-many-arguments
    project_source: DirectoryNode,
    metadata_handler: MetadataHandlder,
    api_agent: MyTardisRestAgent,
    collect_all: bool = False,
    experiment_source: DirectoryNode | None = None,
    dataset_source: DirectoryNode | None = None,
) -> CrateManifest:
    """
    Parse the directory containing the raw data
    Modified from Mytardis_ingestion code as it expects the same file structures
    """

    crate_manifest = CrateManifest()
    raw_dataset_metadata_schema = MetadataSchema(
        schema=metadata_handler.get_mtobj_schema(MtObject.DATASET),
        url=metadata_handler.schema_namespaces.get(MtObject.DATASET) or "",
    )
    project_dirs = [
        d
        for d in project_source.iter_dirs(recursive=True)
        if d.has_file("project.json")
    ]
    experiment_dirs: List[tuple[DirectoryNode, Project]] = []
    dataset_dirs: List[tuple[DirectoryNode, Experiment]] = []
    for project_dir in project_dirs:
        logging.info("Project directory: %s", project_dir.name())
        project = process_project(
            project_dir=project_dir,
            collect_all=collect_all,
            api_agent=api_agent,
        )
        crate_manifest.add_projects(projects={str(project.id): project})
        if experiment_source:
            experiment_dirs = [(experiment_source, project)]
        else:
            experiment_dirs.extend(
                [
                    (d, project)
                    for d in project_dir.iter_dirs(recursive=True)
                    if d.has_file("experiment.json")
                ]
            )

    for experiment_dir, project in experiment_dirs:
        logging.info("Experiment directory: %s", experiment_dir.name())

        experiment = process_experiment(
            experiment_dir,
            parent_project=project,
            collect_all=collect_all,
            crate_manifest=crate_manifest,
        )
        crate_manifest.add_experiments({str(experiment.id): experiment})
        if dataset_source:
            dataset_dirs = [(dataset_source, experiment)]
        else:
            dataset_dirs.extend(
                [
                    (d, experiment)
                    for d in experiment_dir.iter_dirs(recursive=True)
                    if d.has_file(d.name() + ".json")
                ]
            )
    for dataset_dir, experiment in dataset_dirs:
        logging.info("Dataset directory: %s", dataset_dir.name())

        dataset = process_raw_dataset(
            dataset_dir,
            experiment=experiment,
            crate_manifest=crate_manifest,
        )
        dataset_metadata = collect_dataset_metadata(
            dataset_dir=dataset_dir,
            parent_dataset=dataset,
            metadata_schema=raw_dataset_metadata_schema,
        )
        data_dir = next(
            (
                d
                for d in dataset_dir.iter_dirs()
                if datetime_pattern.match(d.path().stem)
            ),
            None,
        )

        dataset.date_created = parse_timestamp(data_dir.name()) if data_dir else None

        crate_manifest.add_datasets({dataset.id: dataset})
        crate_manifest.add_metadata(dataset_metadata.values())

    return crate_manifest

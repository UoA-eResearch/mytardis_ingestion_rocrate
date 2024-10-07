"""Constants for the print lab genomics RO-Crate generator
"""

from src.mt_api.mt_consts import MtObject
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import Facility
NAMESPACES = {
    MtObject.PROJECT: "",
    MtObject.EXPERIMENT: "",
    MtObject.DATASET: "http://abi-music.com/dataset-raw/1",
    MtObject.DATAFILE: "http://abi-music.com/dataset-raw/1",
}

ABI_MUSIC_MICROSCOPE_INSTRUMENT = "abi-music-microscope-v1"
ABI_FACILLITY = Facility(name="ABI music", description="ABI music Facillity")
ZARR_DATASET_NAMESPACE = "http://andrew-test.com/datafile/1"

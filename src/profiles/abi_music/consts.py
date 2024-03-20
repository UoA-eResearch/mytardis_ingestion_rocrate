"""Constants for the print lab genomics RO-Crate generator
"""
from src.mt_api.mt_consts import MtObject

NAMESPACES = {
    # MtObject.PROJECT: "",
    # MtObject.EXPERIMENT: "http://print.lab.mockup/experiment/as_sample/v1",
    MtObject.DATASET: "http://abi-music.com/dataset-raw/1",
    MtObject.DATAFILE: "http://abi-music.com/dataset-raw/1",
}

ABI_MUSIC_MICROSCOPE_INSTRUMENT = "abi-music-microscope-v1"
ABI_FACILLITY = "ABI music"
ZARR_DATASET_NAMESPACE = "http://andrew-test.com/datafile/1"

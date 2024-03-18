"""Constants for the print lab genomics RO-Crate generator
"""
from src.mt_api.mt_consts import MtObject

NAMESPACES = {
    MtObject.PROJECT: "http://print.lab.mockup/project/v1",
    MtObject.EXPERIMENT: "http://print.lab.mockup/experiment/as_sample/v1",
    MtObject.DATASET: "http://print.lab.mockup/dataset/as_sample/v1",
    MtObject.DATAFILE: "http://print.lab.mockup/datafile/as_sample/v1",
}

from mytardis_rocrate_builder.rocrate_builder import ROBuilder
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import Dataset
from rocrate.model.contextentity import ContextEntity

class ABIROBuilder(ROBuilder): # type: ignore 
    """A Builder for ABI dataclasses and crate factors

    Args:
        ROBuilder (_type_): the standard RO-Crate builder extended here
    """

    def add_dataset(self, dataset: Dataset) -> ContextEntity:
        """Add a dataset to the RO-Crate accounting for if unlisted children should be added"""
        dataset_entity = super().add_dataset(dataset)
        dataset_entity.source = (
                self.crate.source / dataset.directory
                if self.crate.source
                else dataset.directory
            )
        return dataset_entity

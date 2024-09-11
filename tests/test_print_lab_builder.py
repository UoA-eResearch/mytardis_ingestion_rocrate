
from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)

from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
    Participant,
    SampleExperiment,
    ExtractionDataset
)
from rocrate.model import ContextEntity as ROContextEntity
from rocrate.model import EncryptedContextEntity as ROEncryptedContextEntity


def test_add_medical_condtion(test_print_lab_builder:PrintLabROBuilder, test_medical_condition:MedicalCondition, test_RO_crate_medical_conditon:ROContextEntity):
    assert test_print_lab_builder.add_medical_condition(test_medical_condition).properties() == test_RO_crate_medical_conditon.properties()

def test_add_participant(
    test_print_lab_builder:PrintLabROBuilder,
    test_participant:Participant,
    test_RO_Participant: ROContextEntity,
):
    #test participant sensitive has been written as encrypted
    assert test_print_lab_builder.add_participant(test_participant).properties() == test_RO_Participant.properties()


def test_add_sample_experiment():
    pass

def test_add_extraction_dataset():
    pass
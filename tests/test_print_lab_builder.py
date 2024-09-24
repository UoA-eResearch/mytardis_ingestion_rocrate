"""Test extended features for the Print Lab MyTardis RO-Crate Builder
"""

from rocrate.model import ContextEntity as ROContextEntity

from src.ingestion_targets.print_lab_genomics.print_crate_builder import (
    PrintLabROBuilder,
)
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
    Participant,
    SampleExperiment,
)


def test_add_medical_condtion(
    test_print_lab_builder: PrintLabROBuilder,
    test_medical_condition: MedicalCondition,
    test_ro_crate_medical_conditon: ROContextEntity,
) -> None:
    """Test adding a medical condtion to the RO-Crate

    Args:
        test_print_lab_builder (PrintLabROBuilder): the extended RO-Crate builder
        test_medical_condition (MedicalCondition): the medical condtion to add
        test_RO_crate_medical_conditon (ROContextEntity): the medical condition in the RO-Crate
    """
    assert (
        test_print_lab_builder.add_medical_condition(
            test_medical_condition
        ).properties()
        == test_ro_crate_medical_conditon.properties()
    )


def test_add_participant(
    test_print_lab_builder: PrintLabROBuilder,
    test_participant: Participant,
    test_ro_participant: ROContextEntity,
) -> None:
    """test adding a participant to the RO-Crate

    Args:
        test_print_lab_builder (PrintLabROBuilder): the RO-Crate builder
        test_participant (Participant): the participant to add to the crate
        test_RO_Participant (ROContextEntity): the participant once added
    """
    assert (
        test_print_lab_builder.add_participant(test_participant).properties()
        == test_ro_participant.properties()
    )


def test_add_sample_experiment(
    test_print_lab_builder: PrintLabROBuilder,
    test_sample_experiment: SampleExperiment,
    test_ro_sample_experiment: ROContextEntity,
) -> None:
    """test adding a experiment containing a sample to the RO-Crate

    Args:
        test_print_lab_builder (PrintLabROBuilder): _description_
        test_sample_experiment (SampleExperiment): _description_
        test_RO_Sample_Experiment (ROContextEntity): _description_
    """
    assert (
        test_print_lab_builder.add_experiment(test_sample_experiment).properties()
        == test_ro_sample_experiment.properties()
    )

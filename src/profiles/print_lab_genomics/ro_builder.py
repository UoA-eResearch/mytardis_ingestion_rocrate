"""Print lab specific RO-Crate builder
"""
from typing import Any

from rocrate.model.contextentity import ContextEntity

from src.rocrate_builder.rocrate_builder import ROBuilder
from src.rocrate_dataclasses.rocrate_dataclasses import MedicalCondition, Participant

PARTICIPANT_TYPE = "person"
CONDITION_TYPE = ["MedicalCondition", "MedicalCode"]
CODING_SYSTEM = "ICD11"


class PrintLabROBuilder(ROBuilder):
    """Ro-Crate builder for Print Lab crates to increase interoperablity

    Args:
        ROBuilder: the parent RO-Crate builder class
    """

    def _add_participant(
        self,
        participant: Participant,
    ) -> Any:
        participant_id = participant.id
        if new_participant := self.crate.dereference(participant_id):
            return new_participant
        new_participant = ContextEntity(
            self.crate,
            participant_id,
            properties={
                "@type": PARTICIPANT_TYPE,
                "name": participant.name,
                "sex": participant.sex,
                "memberOf": participant.project,
                "birthDate": participant.date_of_birth,
                "ethnicity": participant.ethnicity,
            },
        )
        self.crate.add(new_participant)
        return new_participant

    def _add_medical_condtion(
        self, medical_condition: MedicalCondition
    ) -> ContextEntity:
        condition_id = medical_condition.name
        if new_condition := self.crate.dereference(condition_id):
            return new_condition
        new_condition = ContextEntity(
            self.crate,
            condition_id,
            properties={
                "@type": CONDITION_TYPE,
                "name": MedicalCondition.name,
                "codeValue": MedicalCondition.code_text,
                "codingSystem": CODING_SYSTEM,
                "inCodeSet": MedicalCondition.code_source,
            },
        )
        self.crate.add(new_condition)
        return new_condition

    # def add_sample_experiment(self, experiment: SampleExperiment) -> ContextEntity:
    #     """Add an experiment to the RO crate

    #     Args:
    #         experiment (Experiment): The experiment to be added to the crate
    #     """
    #     # Note that this is being created as a data catalog object as there are no better
    #     # fits
    #     added_experiment = self.add_experiment(experiment)
    #     associated_diseases = [
    #         self._add_medical_condtion(disease)
    #         for disease in experiment.associated_disease
    #     ]
    #     identifier = experiment.id
    #     properties = {
    #         "@type": ["DataCatalog", "BioSample"],
    #         "name": experiment.name,
    #         "description": experiment.description,
    #         "project": experiment.project,
    #     }

    #     if experiment.metadata:
    #         properties = self._add_metadata(
    # identifier, properties, experiment.metadata)  # type: ignore
    #     if experiment.date_created:
    #         properties = self._add_dates(  # type: ignore
    #             properties,  # type: ignore
    #             experiment.date_created,
    #             experiment.date_modified,
    #         )
    #     if experiment.participant:
    #         properties = self._add_participant(properties, experiment.participant)
    #     experiment_obj = ContextEntity(
    #         self.crate,
    #         identifier,
    #         properties=properties,
    #     )
    #     return self._add_identifiers(experiment, experiment_obj)

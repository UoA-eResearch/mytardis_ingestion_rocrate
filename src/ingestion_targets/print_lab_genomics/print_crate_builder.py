"""Builder class and functions for translating RO-Crate dataclasses into RO-Crate Entities
"""
import logging
from typing import Any, Dict

from rocrate.model.contextentity import ContextEntity
from rocrate.model.encryptedcontextentity import (  # pylint: disable=import-error, no-name-in-module
    EncryptedContextEntity,
)
from slugify import slugify

from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
    Participant,
    SampleExperiment,
)
from src.rocrate_builder.rocrate_builder import ROBuilder
from src.rocrate_dataclasses.rocrate_dataclasses import Experiment  # BaseObject,

logger = logging.getLogger(__name__)


class PrintLabROBuilder(ROBuilder):
    """Specific RO-Crate builder for print-lab dataclasses

    Args:
        ROBuilder: extends ROBuilder
    """

    def _add_participant_sensitve(
        self, participant: Participant, participant_id: str
    ) -> Any:
        """Add sensitive data object to crate from a participant's info

        Args:
            participant (Participant): participant holding senstive info
            participant_id (str): patient's ID in the RO-Crete

        Returns:
            str: id of the sensitive info
        """
        name = slugify(f"{participant.id}-sensitive")
        sensitive_data = EncryptedContextEntity(
            self.crate,
            name,
            properties={
                "@type": "MedicalEntity",
                "name": name,
                "NHI number": participant.nhi_number or "N/A",
                "date_of_birth": participant.date_of_birth,
                "parents": [participant_id],
            },
            pubkey_fingerprints=[],
        )
        return self.crate.add(sensitive_data).id

    def add_medical_condition(
        self, medical_condition: MedicalCondition
    ) -> ContextEntity:
        """Add a medical condition, usually associated with a sample

        Args:
            medical_condition (MedicalCondition): medical condtion object read in from source

        Returns:
            ContextEntity: a context entity representing the medical condition
        """
        identifier = slugify(f"{medical_condition.code_type}-{medical_condition.name}")
        if condition := self.crate.dereference(identifier):
            return condition
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": "MedicalCondition",
            "name": medical_condition.name,
            "code_type": medical_condition.code_type,
            "code_source": medical_condition.code_source.as_posix(),
            "code_text": medical_condition.code_text,
        }
        medical_condition_obj = ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )
        self.crate.add(medical_condition_obj)
        return medical_condition_obj

    def add_participant(
        self, participant: Participant, sensitive: bool = False
    ) -> ContextEntity:
        """Add a participant to the RO-Crate as a context entitiy

        Args:
            participant (Participant): the participant data
            sensitive (bool, optional): is this participant data sensitive, should it be encrypted?.
                Defaults to False.

        Returns:
            ContextEntity: A (poteintally encryped) context entity for this participant.
                now stored in the RO-Crate
        """
        identifier = participant.id
        if participant_obj := self.crate.dereference(identifier):
            return participant_obj
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": ["Person", "MedicalEntity", "Patient"],
            "name": participant.name,
            "description": participant.description,
            "project": participant.project,
            "sex": participant.sex,
            "ethnicity": participant.ethnicity,
        }

        properties = self._update_properties(
            data_object=participant, properties=properties
        )
        if self.crate.pubkey_fingerprints and (
            participant.date_of_birth or participant.nhi_number
        ):
            properties["sensitive"] = self._add_participant_sensitve(
                participant, str(participant.id)
            )
        if sensitive:
            participant_obj = EncryptedContextEntity(
                self.crate,
                identifier,
                properties=properties,
            )
            return self._add_identifiers(participant, participant_obj)
        participant_obj = ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )
        return self._add_identifiers(participant, participant_obj)

    def add_experiment(self, experiment: Experiment) -> ContextEntity:
        """Add a sample experiment to the RO crate

        Args:
            experiment (Experiment): The experiment to be added to the crate
        """
        # Note that this is being created as a data catalog object as there are no better
        # fits
        if not isinstance(experiment, SampleExperiment):
            return super().add_experiment(experiment)
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": "DataCatalog",
            "participant": self.add_participant(experiment.participant).id
            if experiment.participant
            else "",
            "sex": experiment.sex if experiment.sex else "",
            "name": experiment.name,
            "associated_disease": [
                self.add_medical_condition(condition).id
                for condition in experiment.associated_disease
            ]
            if experiment.associated_disease
            else [],
            "project": experiment.projects,
            "body_location": self.add_medical_condition(experiment.body_location).id
            if experiment.body_location
            else "",
            "tissue_processing_method": experiment.tissue_processing_method
            if experiment.tissue_processing_method
            else "",
            "analyate": experiment.analyate if experiment.analyate else "",
            "description": experiment.description,
        }
        experiment_obj = self._update_experiment_meta(
            experiment=experiment, properties=properties
        )
        return self._add_identifiers(experiment, experiment_obj)

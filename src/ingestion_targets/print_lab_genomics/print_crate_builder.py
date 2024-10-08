"""Builder class and functions for translating RO-Crate dataclasses into RO-Crate Entities
"""

import logging
from typing import Any, Dict

from mytardis_rocrate_builder.rocrate_builder import ROBuilder
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    Dataset,
    Experiment,
)
from rocrate.model.contextentity import ContextEntity
from rocrate.model.encryptedcontextentity import (  # pylint: disable=import-error, no-name-in-module
    EncryptedContextEntity,
)
from slugify import slugify

from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    ExtractionDataset,
    MedicalCondition,
    Participant,
    SampleExperiment,
)

logger = logging.getLogger(__name__)


class PrintLabROBuilder(ROBuilder):  # type: ignore
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
        )
        if participant.recipients:
            recipients = [
                self.crate.dereference(user.roc_id) or self.add_user(user)
                for user in participant.recipients
                if user
            ]
            sensitive_data.append_to("encryptedTo", recipients)
        return self.crate.add(sensitive_data)

    def add_medical_condition(
        self, medical_condition: MedicalCondition
    ) -> ContextEntity:
        """Add a medical condition, usually associated with a sample

        Args:
            medical_condition (MedicalCondition): medical condtion object read in from source

        Returns:
            ContextEntity: a context entity representing the medical condition
        """
        identifier = medical_condition.roc_id
        if condition := self.crate.dereference(identifier):
            return condition
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": "MedicalCondition",
            "name": medical_condition.code,
            "code_type": medical_condition.code_type,
            "code_source": medical_condition.code_source,
        }
        medical_condition_obj = ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )
        if medical_condition.code_text:
            medical_condition_obj.append_to("code_text", medical_condition.code_text)
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

        identifier = participant.roc_id
        if participant_obj := self.crate.dereference(identifier):
            return participant_obj
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": ["Person", "MedicalEntity", "Patient"],
            "name": participant.name,
            "description": participant.description,
            "project": participant.project,
            "gender": participant.gender,
            "ethnicity": participant.ethnicity,
        }

        properties = self._update_properties(
            data_object=participant, properties=properties
        )
        if sensitive and participant.recipients:
            participant_obj = EncryptedContextEntity(
                self.crate,
                identifier,
                properties=properties,
            )
            recipients = [
                self.crate.dereference(participant.user.rocid) or self.add_user(user)
                for user in participant.recipients
            ]
            participant_obj.append_to("encryptedTo", recipients)
            return self.crate.add(participant_obj)

        participant_obj = ContextEntity(
            self.crate,
            identifier,
            properties=properties,
        )
        if participant.recipients and (
            participant.date_of_birth or participant.nhi_number
        ):
            participant_obj.append_to(
                "sensitive",
                self._add_participant_sensitve(participant, str(participant.id)),
            )
        return self.crate.add(participant_obj)

    def add_experiment(self, experiment: Experiment) -> ContextEntity:
        """Add a sample experiment to the RO crate

        Args:
            experiment (Experiment): The experiment to be added to the crate
        """
        if not isinstance(experiment, SampleExperiment):
            return super().add_experiment(experiment)
        properties: Dict[str, str | list[str] | dict[str, Any]] = {
            "@type": "DataCatalog",
            "gender": experiment.gender if experiment.gender else "",
            "name": experiment.name,
            "analyate": experiment.analyate if experiment.analyate else "",
            "description": experiment.description,
        }
        projects = []
        for project in experiment.projects:
            if crate_project := self.crate.dereference(project.roc_id):
                projects.append(crate_project)
            else:
                projects.append(self.add_project(project))
        experiment_obj = self._update_experiment_meta(
            experiment=experiment, properties=properties, projects=projects
        )

        associated_diseases = (
            [
                self.add_medical_condition(condition)
                for condition in experiment.associated_disease
            ]
            if experiment.associated_disease
            else []
        )
        experiment_obj.append_to("associated_disease", associated_diseases)
        body_location = (
            self.add_medical_condition(experiment.body_location)
            if experiment.body_location
            else None
        )
        experiment_obj.append_to(
            "tissue_processing_method", experiment.tissue_processing_method
        )
        experiment_obj.append_to("body_location", body_location)
        participant = self.add_participant(experiment.participant)
        experiment_obj.append_to("participant", participant)
        return self.crate.add(experiment_obj)

    def add_dataset(self, dataset: Dataset) -> ContextEntity:
        """Add a dataset to the RO-Crate accounting for if unlisted cildren should be added"""
        datset_entity = super().add_dataset(dataset)
        if not isinstance(dataset, ExtractionDataset):
            return datset_entity
        if (
            dataset.copy_unlisted
        ):  # update source so dataset directory and all children are added
            datset_entity.source = (
                self.crate.source / dataset.directory
                if self.crate.source
                else dataset.directory
            )
        return datset_entity

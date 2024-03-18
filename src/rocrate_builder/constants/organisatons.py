"""Define The University of Auckland as an Organisation."""

from src.rocrate_dataclasses.rocrate_dataclasses import Organisation

UOA = Organisation(
    identifiers=["https://ror.org/03b94tp07"],
    name="The University of Auckland | Waipapa Taumata Rau",
    url="https://auckland.ac.nz",
    location="Auckland, New Zealand",
    research_org=True,
)

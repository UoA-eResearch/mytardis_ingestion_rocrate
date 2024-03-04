# pylint: disable=all
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class Metadata_csv_parser:
    def __init__(self, sheet_location: Path):
        self.sheet_location = sheet_location
        pass

    def extract_metadata(self) -> None:
        pass

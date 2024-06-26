"""
Helpers for configuring and formatting logging outputs
Taken wholesale from the MyTardis ingestion logging utils, thanks Andrew.
"""

import logging
import sys
from typing import Optional


def init_logging(file_name: Optional[str] = None, level: int = logging.DEBUG) -> None:
    """
    Configure a basic default logging setup. Logs to the console, and optionally
    to a file, if a filename is passed.
    """
    root = logging.getLogger()
    root.setLevel(level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))
    root.addHandler(console_handler)

    if file_name:
        file_handler = logging.FileHandler(filename=file_name, mode="w")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))
        root.addHandler(file_handler)

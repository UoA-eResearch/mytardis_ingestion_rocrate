"""
    Utitlites for file handling and checking
"""

from pathlib import Path


def is_xslx(filename: Path) -> bool:
    """magic number check for xls files"""
    with open(filename, "rb") as f:
        first_four_bytes = f.read()[:4]
    return first_four_bytes == b"PK\x03\x04"

"""Tar compatible ROCrate with build_json only capability"""

from typing import Optional
from rocrate.rocrate import ROCrate
import tarfile
from pathlib import Path

class TaROCrate(ROCrate):

    def write_tar(self, out_path: str| Path, dry_un: bool = False, compress: Optional[str]=None) -> None:
        if isinstance(out_path, str):
            out_path = Path(out_path)
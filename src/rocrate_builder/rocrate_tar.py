# pylint: disable=missing-docstring
"""Tar compatible ROCrate with build_json only capability"""

from pathlib import Path

from rocrate.rocrate import ROCrate


class TaROCrate(ROCrate):  # type: ignore
    def write_tar(
        self,
        out_path: str | Path,
        # dry_un: bool = False, compress: Optional[str] = None
    ) -> None:
        if isinstance(out_path, str):
            out_path = Path(out_path)

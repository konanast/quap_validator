from __future__ import annotations
import io
import os
import tarfile
import zipfile
import tempfile
from pathlib import Path
import shutil
import gzip, bz2, lzma
from typing import Callable, Optional, Tuple, List

BASIC_ARCHIVE_EXTS = {
    ".zip",
    ".tar",
    ".tgz",
    ".tar.gz",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".gz",
    ".bz2",
    ".xz",
}


class UnpackError(Exception):
    pass


def _is_archive(path: Path) -> bool:
    p = path.name.lower()
    return any(p.endswith(ext) for ext in BASIC_ARCHIVE_EXTS)


def _safe_members_tar(tf: tarfile.TarFile) -> List[tarfile.TarInfo]:
    safe = []
    for m in tf.getmembers():
        name = m.name
        if name.startswith("/") or name.startswith("\\"):
            raise UnpackError(f"Unsafe member path: {name}")
        norm = os.path.normpath(name)
        if norm.startswith("..") or "/../" in norm.replace("\\", "/"):
            raise UnpackError(f"Unsafe member path: {name}")
        safe.append(m)
    return safe


def _safe_namelist_zip(zf: zipfile.ZipFile) -> List[str]:
    safe = []
    for name in zf.namelist():
        if name.startswith("/") or name.startswith("\\"):
            raise UnpackError(f"Unsafe member path: {name}")
        norm = os.path.normpath(name)
        if norm.startswith("..") or "/../" in norm.replace("\\", "/"):
            raise UnpackError(f"Unsafe member path: {name}")
        safe.append(name)
    return safe


def _shapefile_dataset_root(paths: List[Path]) -> Optional[Path]:
    """Return the stem if files constitute exactly one shapefile dataset; else None."""
    shp_files = [p for p in paths if p.suffix.lower() == ".shp"]
    if len(shp_files) != 1:
        return None
    stem = shp_files[0].with_suffix("")
    siblings = [p for p in paths if p.with_suffix("") == stem]
    if not any(p.suffix.lower() == ".dbf" for p in siblings):
        return None
    if not any(p.suffix.lower() == ".shx" for p in siblings):
        return None
    return shp_files[0]


def _single_file_stream_decompress(src: Path, tmpdir: Path) -> Path:
    suffix = src.suffix.lower()
    if suffix == ".gz":
        opener, strip = gzip.open, True
    elif suffix == ".bz2":
        opener, strip = bz2.open, True
    elif suffix == ".xz":
        opener, strip = lzma.open, True
    else:
        raise UnpackError(f"Unsupported single-file compression: {src}")
    out_name = src.name[: -len(suffix)] if strip else src.name + ".out"
    if not out_name:
        out_name = "decompressed.bin"
    out_path = tmpdir / out_name
    with opener(src, "rb") as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return out_path


def maybe_decompress(input_path: str) -> Tuple[Path, Callable[[], None]]:
    """
    Returns (dataset_path, cleanup_fn).
    - dataset_path: either the original path, or a path inside a temp dir (e.g., .gpkg, .csv, .parquet, or .shp for shapefiles)
    - cleanup_fn(): delete temp directory if created.
    Raises UnpackError for multi-root archives or unsafe paths.
    """
    p = Path(input_path)
    if not p.exists():
        raise UnpackError(f"File not found: {input_path}")

    if not _is_archive(p):
        return p, (lambda: None)

    tmpdir_obj = tempfile.TemporaryDirectory(prefix="validator_unpack_")
    tmpdir = Path(tmpdir_obj.name)

    if p.suffix.lower() in {".gz", ".bz2", ".xz"} and not p.name.lower().endswith(
        (".tar.gz", ".tar.bz2", ".tar.xz", ".tgz", ".tbz2", ".txz")
    ):
        out_file = _single_file_stream_decompress(p, tmpdir)
        return out_file, tmpdir_obj.cleanup

    if p.name.lower().endswith(".zip"):
        with zipfile.ZipFile(p, "r") as zf:
            names = _safe_namelist_zip(zf)
            zf.extractall(tmpdir, members=names)

    elif p.name.lower().endswith(
        (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")
    ):
        with tarfile.open(p, "r:*") as tf:
            members = _safe_members_tar(tf)
            tf.extractall(tmpdir, members=members)

    else:
        tmpdir_obj.cleanup()
        raise UnpackError(f"Unsupported archive: {p.name}")

    files = [q for q in tmpdir.rglob("*") if q.is_file()]
    if not files:
        tmpdir_obj.cleanup()
        raise UnpackError("Archive contained no files.")

    if len(files) == 1:
        return files[0], tmpdir_obj.cleanup

    shp_root = _shapefile_dataset_root(files)
    if shp_root is not None:
        return shp_root, tmpdir_obj.cleanup

    stems = sorted(set([f.name for f in files if f.parent == tmpdir]))
    raise UnpackError(
        "Archive contains multiple files and does not represent a single Shapefile bundle. "
        "Please provide an archive with exactly one dataset (e.g., a single .csv/.gpkg/.parquet, "
        "or a zipped Shapefile with one stem)."
    )

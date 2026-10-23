#!/usr/bin/env python3
"""Download, verify and safely extract pinned telecom datasets."""

import argparse
import hashlib
import json
import os
import shutil
import stat
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data/manifests/datasets.json"
CHUNK = 1024 * 1024


def safe_path(relative):
    """Resolve a manifest destination without leaving the repository."""
    result = (ROOT / relative).resolve()
    if not result.is_relative_to(ROOT):
        raise ValueError(f"Unsafe destination: {relative}")
    return result


def md5sum(path):
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_archive(dataset):
    archive = dataset["archive"]
    destination = safe_path(archive["path"])
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        print(f"Using existing archive: {destination.name}")
    else:
        print(f"Downloading: {destination.name}")
        temporary = destination.with_name(destination.name + ".part")

        request = urllib.request.Request(
            archive["url"],
            headers={"User-Agent": "TelecomDataPlatform/1.0"}
        )

        try:
            with urllib.request.urlopen(request, timeout=90) as source:
                with temporary.open("wb") as output:
                    shutil.copyfileobj(source, output, CHUNK)
            os.replace(temporary, destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

    actual = md5sum(destination)
    expected = archive["md5"].lower()

    if actual != expected:
        raise RuntimeError(
            f"Checksum mismatch for {destination.name}\n"
            f"Expected: {expected}\n"
            f"Actual:   {actual}\n"
            "Archive not trusted. Remove the corrupt archive manually "
            "before retrying."
        )

    print(f"MD5 verified: {destination.name}")
    return destination


def source_destination(name, dataset):
    """Map original ZIP paths into the documented raw data layout."""
    parts = PurePosixPath(name)

    if parts.is_absolute() or ".." in parts.parts:
        raise ValueError(f"Unsafe archive path: {name}")

    if any(part.startswith(".") for part in parts.parts):
        return None

    exact = dataset.get("exact_files", {})
    if name in exact:
        return safe_path(exact[name])

    for mapping in dataset["mappings"]:
        prefix = mapping["source_prefix"]

        if name.startswith(prefix):
            relative = name[len(prefix):]
            if relative:
                return safe_path(
                    Path(mapping["destination"]) / relative
                )

    return None


def extract_dataset(dataset, archive_path, force=False):
    print(f"Extracting / checking: {dataset['id']}")
    extracted = 0
    skipped = 0
    matched = 0

    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()

        if len(members) > 150000:
            raise RuntimeError("Unexpectedly large archive inventory")

        total_size = sum(item.file_size for item in members)
        if total_size > 4 * 1024**3:
            raise RuntimeError("Unexpected uncompressed archive size")

        for item in members:
            if item.is_dir():
                continue

            mode = (item.external_attr >> 16) & 0o170000
            if mode == stat.S_IFLNK:
                raise RuntimeError(f"Symlink not allowed: {item.filename}")

            target = source_destination(item.filename, dataset)
            if target is None:
                continue

            matched += 1

            if target.exists():
                if not target.is_file():
                    raise RuntimeError(f"Not a regular file: {target}")

                if target.stat().st_size == item.file_size and not force:
                    skipped += 1
                    continue

                if not force:
                    raise RuntimeError(
                        f"Local file conflicts with archive: {target}\n"
                        "Inspect the file, or use --force to restore it."
                    )

            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".part")

            try:
                with archive.open(item) as source:
                    with temporary.open("wb") as output:
                        shutil.copyfileobj(source, output, CHUNK)

                if temporary.stat().st_size != item.file_size:
                    raise RuntimeError(
                        f"Extracted size mismatch: {item.filename}"
                    )

                os.replace(temporary, target)
                extracted += 1
            finally:
                temporary.unlink(missing_ok=True)

    if matched == 0:
        raise RuntimeError(
            f"No matching data files found in {archive_path}"
        )

    for expected in dataset.get("expected_files", []):
        if not safe_path(expected).is_file():
            raise RuntimeError(f"Missing required file: {expected}")

    print(
        f"Completed {dataset['id']}: "
        f"{extracted} extracted, {skipped} already present, "
        f"{matched} matched archive files"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Prepare pinned telecom datasets from Zenodo"
    )
    parser.add_argument(
        "--dataset",
        choices=["all", "network", "fraudzen"],
        default="all"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing extracted files from verified archives"
    )
    args = parser.parse_args()

    with MANIFEST.open(encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("manifest_version") != 1:
        raise RuntimeError("Unsupported dataset manifest version")

    for dataset in manifest["datasets"]:
        if args.dataset not in ("all", dataset["id"]):
            continue

        print(f"\n=== {dataset['name']} ===")
        archive = download_archive(dataset)
        extract_dataset(dataset, archive, force=args.force)

    print("\nDataset setup completed successfully.")


if __name__ == "__main__":
    main()

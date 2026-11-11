"""Where the dataset lives: the HuggingFace repo, or a local directory for dry runs.

Both stores expose the same two calls: read the manifest, and commit a set of
files plus the new manifest in one step. A sync run reads nothing else and
writes once, so an unchanged day costs one manifest download and no commit.

``huggingface_hub`` errors are wrapped as domain errors at this boundary.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import structlog
from huggingface_hub import CommitOperationAdd, HfApi, create_repo, hf_hub_download
from huggingface_hub.errors import (
    EntryNotFoundError,
    HfHubHTTPError,
    RepositoryNotFoundError,
)
from pydantic import ValidationError

from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.manifest import MANIFEST_PATH, Manifest

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]


class Store(Protocol):
    """A dataset destination."""

    def read_manifest(self) -> Manifest:
        """The held manifest; empty only when the dataset does not exist yet."""
        ...

    def commit(self, files: dict[str, Path], manifest: Manifest, message: str) -> None:
        """Write ``files`` (repo path -> local file) and ``manifest`` in one commit."""
        ...


def _parse(text: str, where: str) -> Manifest:
    try:
        return Manifest.model_validate_json(text)
    except ValidationError as exc:
        raise ExternalServiceError(f"drifted manifest in {where}: {exc}") from exc


class LocalStore:
    """A directory laid out like the HF repo; counts its commits."""

    def __init__(self, root: Path) -> None:
        """Use ``root`` as the dataset root."""
        self.root = root
        self.commits = 0

    def read_manifest(self) -> Manifest:
        """The manifest under ``root``, or empty."""
        path = self.root / MANIFEST_PATH
        if not path.exists():
            return Manifest()
        return _parse(path.read_text(encoding="utf-8"), str(path))

    def commit(self, files: dict[str, Path], manifest: Manifest, message: str) -> None:
        """Copy ``files`` into ``root`` and write the manifest."""
        for repo_path, local in files.items():
            dest = self.root / repo_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            if local.resolve() != dest.resolve():
                _ = shutil.copyfile(local, dest)
        dest = self.root / MANIFEST_PATH
        dest.parent.mkdir(parents=True, exist_ok=True)
        _ = dest.write_text(manifest.to_json(), encoding="utf-8")
        self.commits += 1
        log.info("commit.local", root=str(self.root), files=len(files), message=message)


class HfStore:
    """The HuggingFace dataset repo."""

    def __init__(self, repo_id: str, token: str | None) -> None:
        """Target ``repo_id`` with ``token``."""
        self.repo_id = repo_id
        self.token = token

    def read_manifest(self) -> Manifest:
        """Download ``_state/manifest.json``.

        A missing manifest in an existing repo raises: treating it as empty would
        write a manifest that forgets every held file.

        Raises:
            ExternalServiceError: on transport failure, a drifted manifest, or a
                repo that exists without one.

        """
        try:
            local = hf_hub_download(
                self.repo_id, MANIFEST_PATH, repo_type="dataset", token=self.token
            )
        except RepositoryNotFoundError:
            return Manifest()
        except EntryNotFoundError as exc:
            raise ExternalServiceError(
                f"{self.repo_id} exists but has no {MANIFEST_PATH}; run `dtcc-sdr seed` first"
            ) from exc
        except HfHubHTTPError as exc:
            raise ExternalServiceError(
                f"failed to read the manifest of {self.repo_id}: {exc}"
            ) from exc
        return _parse(Path(local).read_text(encoding="utf-8"), self.repo_id)

    def ensure_repo(self) -> None:
        """Create the dataset repo if it does not already exist."""
        try:
            _ = create_repo(self.repo_id, repo_type="dataset", exist_ok=True, token=self.token)
        except HfHubHTTPError as exc:
            raise ExternalServiceError(
                f"failed to ensure dataset repo {self.repo_id}: {exc}"
            ) from exc

    def commit(self, files: dict[str, Path], manifest: Manifest, message: str) -> None:
        """One ``create_commit`` carrying every new partition and the manifest."""
        ops = [
            CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=str(local))
            for repo_path, local in sorted(files.items())
        ]
        ops.append(
            CommitOperationAdd(
                path_in_repo=MANIFEST_PATH, path_or_fileobj=manifest.to_json().encode()
            )
        )
        try:
            _ = HfApi(token=self.token).create_commit(
                repo_id=self.repo_id, repo_type="dataset", operations=ops, commit_message=message
            )
        except HfHubHTTPError as exc:
            raise ExternalServiceError(f"failed to commit to {self.repo_id}: {exc}") from exc
        log.info("commit.hf", repo_id=self.repo_id, files=len(files), message=message)

    def upload_tree(self, local_dir: Path) -> None:
        """Resumable bulk upload of a Parquet tree (the one-off seed).

        ``upload_large_folder`` hashes each file and skips those already present,
        so an interrupted seed resumes where it stopped.
        """
        try:
            HfApi(token=self.token).upload_large_folder(
                repo_id=self.repo_id,
                repo_type="dataset",
                folder_path=str(local_dir),
                allow_patterns=["*.parquet"],
                ignore_patterns=["._*", ".tmp-*"],
            )
        except HfHubHTTPError as exc:
            raise ExternalServiceError(f"failed to upload to {self.repo_id}: {exc}") from exc

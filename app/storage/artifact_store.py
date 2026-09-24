"""Artifact storage (spec section 9).

Distinguishes logical identity from immutable filing identity:

    reports/{ticker}/{form}/{year}/
        current                       <- pointer info (kept in Redis too)
        accession-{accession}/
            filing.*                  <- preserved SEC source
            report.pdf
            metadata.json

The physical backend is intentionally abstracted behind this module so it
can be swapped (local disk here; S3/GCS in production) without touching
callers. Accession-scoped directories are treated as immutable: once
written, they are never overwritten in place.

Because the backend is unspecified by the spec, physical paths are treated
as internal. Callers that need to name an artifact externally use
`ref_for()`, which yields a stable, backend-independent reference built
only from spec section 27 identifiers (logical path + accession number).
`resolve_pdf_path()` is the inverse, and is the single place where a
reference is turned back into a location.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

from app.config import settings
from app.reports.identity import ReportIdentity, decode_form, encode_form

# SEC accession numbers are a 10-digit filer prefix and a 2-digit year,
# followed by a sequence number. The 10-2 prefix is documented, but the
# sequence length is not guaranteed anywhere normative (it is conventionally
# 6), so only digits are required here rather than a fixed width — a stricter
# rule risks rejecting legitimate accessions.
#
# This is defence in depth, not the traversal barrier: `_accession_dir`
# rebuilds the path from validated components instead of joining raw input,
# so `..` segments are structurally discarded even without this check.
_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d+$")


class InvalidArtifactRefError(ValueError):
    pass


@dataclass(frozen=True)
class ArtifactPaths:
    root: str
    source_path: str
    pdf_path: str
    metadata_path: str


class ArtifactStore:
    def __init__(self, storage_root: str = settings.storage_root) -> None:
        self._root = storage_root

    # -- public references --------------------------------------------------

    @staticmethod
    def ref_for(identity: ReportIdentity, accession_number: str) -> str:
        """Stable public reference to a generated artifact.

        Composed only of identifiers the spec already guarantees
        (section 27): the logical path plus the immutable accession number.
        It deliberately carries no information about the storage backend, so
        the same reference stays valid if local disk is swapped for S3.

        It is also URL-shaped, so a future report-serving endpoint
        (`GET /artifacts/{ref}`, deferred by spec section 26) can adopt it
        without changing the response contract.
        """
        segments = [identity.ticker, encode_form(identity.form), str(identity.year)]
        if identity.quarter:
            segments.append(identity.quarter)
        segments.append(accession_number)
        return "/".join(segments)

    @staticmethod
    def parse_ref(ref: str) -> tuple[ReportIdentity, str]:
        """Inverse of `ref_for`. Raises `InvalidArtifactRefError` if malformed."""
        segments = [s for s in ref.split("/") if s]
        # The accession is always last; everything before it is the logical
        # path, which may or may not carry a trailing quarter segment.
        if len(segments) not in (4, 5):
            raise InvalidArtifactRefError(f"Invalid artifact ref: {ref!r}")
        *logical, accession = segments
        if not _ACCESSION_RE.match(accession):
            raise InvalidArtifactRefError(f"Invalid accession in ref: {ref!r}")
        try:
            identity = ReportIdentity(
                ticker=logical[0],
                form=decode_form(logical[1]),
                year=int(logical[2]),
                quarter=logical[3] if len(logical) == 4 else None,
            )
        except ValueError as exc:
            raise InvalidArtifactRefError(f"Invalid artifact ref: {ref!r}") from exc
        return identity, accession

    def resolve_pdf_path(self, ref: str) -> str:
        """Turn a public reference back into a physical location.

        The only place in the codebase that performs this mapping.
        """
        identity, accession = self.parse_ref(ref)
        return os.path.join(self._accession_dir(identity, accession), "report.pdf")

    # -- physical layout ----------------------------------------------------

    def _accession_dir(self, identity: ReportIdentity, accession_number: str) -> str:
        return os.path.join(
            self._root,
            identity.ticker,
            encode_form(identity.form),
            str(identity.year),
            f"accession-{accession_number}",
        )

    def paths_for(
        self, identity: ReportIdentity, accession_number: str, source_extension: str
    ) -> ArtifactPaths:
        accession_dir = self._accession_dir(identity, accession_number)
        return ArtifactPaths(
            root=accession_dir,
            source_path=os.path.join(accession_dir, f"filing{source_extension}"),
            pdf_path=os.path.join(accession_dir, "report.pdf"),
            metadata_path=os.path.join(accession_dir, "metadata.json"),
        )

    def artifact_exists(self, paths: ArtifactPaths) -> bool:
        """Idempotency check (spec 8/13): skip re-download/convert if the
        accession-scoped artifact is already present."""
        return os.path.exists(paths.pdf_path)

    def write(
        self,
        paths: ArtifactPaths,
        source_bytes: bytes,
        pdf_bytes: bytes,
        metadata: dict,
    ) -> None:
        os.makedirs(paths.root, exist_ok=True)
        # Write to temp files then atomically rename so partially-written
        # artifacts are never observed by other processes.
        self._atomic_write(paths.source_path, source_bytes)
        self._atomic_write(paths.pdf_path, pdf_bytes)
        self._atomic_write(paths.metadata_path, json.dumps(metadata, indent=2).encode())

    @staticmethod
    def _atomic_write(path: str, data: bytes) -> None:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)



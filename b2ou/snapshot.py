"""
VaultSnapshot — efficient single-pass directory walk.

Instead of walking the same folder three times (newest mtime, content
hashes, junk collection), ``VaultSnapshot`` does it once and caches
everything.  A cheap ``refresh_mtimes()`` re-stats known files without
a new walk.

ChangeDetector uses VaultSnapshot data plus the Bear DB signature to
decide whether a sync cycle needs to run.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

try:
    import xxhash
    _USE_XXHASH = True
except ImportError:
    import hashlib
    _USE_XXHASH = False

from b2ou.constants import (
    CLOUD_JUNK_DIRS,
    CLOUD_JUNK_RE,
    SENTINEL_FILES,
)
from b2ou.db import bear_db_signature

log = logging.getLogger(__name__)

_NOTE_EXTS = frozenset((".md", ".txt", ".markdown"))


def _is_note_file(fname: str) -> bool:
    return (
        any(fname.endswith(ext) for ext in _NOTE_EXTS)
        and fname not in SENTINEL_FILES
    )


# ---------------------------------------------------------------------------
# Content hashing
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    """Return a content hash (xxhash or SHA-256) for *path*."""
    try:
        if _USE_XXHASH:
            h = xxhash.xxh3_128()
        else:
            h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


# ---------------------------------------------------------------------------
# NoteEntry
# ---------------------------------------------------------------------------

class NoteEntry:
    """One note file discovered during a VaultSnapshot walk."""
    __slots__ = ("abs_path", "mtime", "size")

    def __init__(self, abs_path: Path, mtime: float, size: int) -> None:
        self.abs_path = abs_path
        self.mtime = mtime
        self.size = size


# ---------------------------------------------------------------------------
# VaultSnapshot
# ---------------------------------------------------------------------------

class VaultSnapshot:
    """
    Single ``os.walk`` of a vault folder.  Collects:

    * ``notes``         – ``{rel_path: NoteEntry}`` for every note file.
    * ``newest_mtime``  – max mtime across all notes (Layer 2 fast-path).
    * ``junk_files``    – cloud-junk files to remove.
    * ``junk_dirs``     – cloud-junk directories to remove.
    """

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.notes: dict[str, NoteEntry] = {}
        self.newest_mtime: float = 0.0
        self.junk_files: list[Path] = []
        self.junk_dirs: list[Path] = []
        if folder.is_dir():
            self._walk()

    # ── core walk ────────────────────────────────────────────────────────

    def _walk(self) -> None:
        for root, dirs, files in os.walk(self.folder):
            root_path = Path(root)
            keep: list[str] = []
            for d in dirs:
                if d in CLOUD_JUNK_DIRS:
                    self.junk_dirs.append(root_path / d)
                elif d in ("BearImages", ".obsidian"):
                    pass  # skip, don't descend
                elif d.endswith(".textbundle"):
                    self._add_textbundle(root_path, d)
                else:
                    keep.append(d)
            dirs[:] = keep

            for fname in files:
                fpath = root_path / fname
                if CLOUD_JUNK_RE.match(fname):
                    self.junk_files.append(fpath)
                    continue
                if not _is_note_file(fname):
                    continue
                try:
                    st = fpath.stat()
                    if st.st_size == 0:
                        self.junk_files.append(fpath)
                        continue
                    rel = str(fpath.relative_to(self.folder))
                    self.notes[rel] = NoteEntry(fpath, st.st_mtime, st.st_size)
                    if st.st_mtime > self.newest_mtime:
                        self.newest_mtime = st.st_mtime
                except OSError:
                    pass

    def _add_textbundle(self, root: Path, dirname: str) -> None:
        tb_text = root / dirname / "text.md"
        try:
            st = tb_text.stat()
            rel = str((root / dirname).relative_to(self.folder))
            self.notes[rel] = NoteEntry(tb_text, st.st_mtime, st.st_size)
            if st.st_mtime > self.newest_mtime:
                self.newest_mtime = st.st_mtime
        except OSError:
            pass

    # ── refresh (re-stat without re-walking) ─────────────────────────────

    def refresh_mtimes(self) -> None:
        """Re-stat every known note; update ``newest_mtime``.  O(n) stat calls."""
        self.newest_mtime = 0.0
        dead: list[str] = []
        for rel, entry in self.notes.items():
            try:
                st = entry.abs_path.stat()
                entry.mtime = st.st_mtime
                entry.size = st.st_size
                if st.st_mtime > self.newest_mtime:
                    self.newest_mtime = st.st_mtime
            except OSError:
                dead.append(rel)
        for rel in dead:
            del self.notes[rel]

    # ── content hashes ───────────────────────────────────────────────────

    def compute_hashes(
        self, prev_hashes: Optional[dict] = None
    ) -> dict[str, tuple[int, float, str]]:
        """
        Return ``{rel_path: (size, mtime, hash)}`` for all notes.

        When *prev_hashes* is supplied files whose size and mtime are unchanged
        reuse the cached hash.
        """
        result: dict[str, tuple[int, float, str]] = {}
        for rel, entry in self.notes.items():
            if prev_hashes and rel in prev_hashes:
                cached = prev_hashes[rel]
                if isinstance(cached, (list, tuple)) and len(cached) >= 3:
                    prev_sz, prev_mtime, prev_hash = cached[0], cached[1], cached[2]
                    if entry.size == prev_sz and abs(entry.mtime - float(prev_mtime)) < 0.001 and prev_hash:
                        result[rel] = tuple(cached)
                        continue
            result[rel] = (entry.size, entry.mtime, _hash_file(entry.abs_path))
        return result

    # ── junk removal ─────────────────────────────────────────────────────

    def clean_junk(self) -> int:
        """Remove all cloud-junk files/dirs found during the walk."""
        removed = 0
        for d in self.junk_dirs:
            try:
                shutil.rmtree(d)
                removed += 1
            except OSError:
                pass
        for f in self.junk_files:
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass
        return removed


def build_snapshots(folders: list[Path]) -> dict[Path, VaultSnapshot]:
    """Return ``{folder: VaultSnapshot}`` for each folder."""
    return {f: VaultSnapshot(f) for f in folders}


# ---------------------------------------------------------------------------
# ChangeDetector
# ---------------------------------------------------------------------------

class ChangeDetector:
    """
    Detects whether Bear content or exported files have changed since the
    last saved state.
    """

    def __init__(
        self,
        state: dict,
        folder_md: Path,
        folder_tb: Path,
        bear_db: Path,
        snapshots: Optional[dict[Path, VaultSnapshot]] = None,
    ) -> None:
        self._hash_state = state.get("hashes", {})
        self.folder_md = folder_md
        self.folder_tb = folder_tb
        self._bear_db = bear_db
        self._snapshots = snapshots

    # ── Bear DB ──────────────────────────────────────────────────────────

    def bear_changed(self) -> bool:
        current_mod, current_count = bear_db_signature(self._bear_db)
        prev_mod = float(self._hash_state.get("bear_max_mod", 0.0) or 0.0)
        prev_count_raw = self._hash_state.get("bear_note_count", None)

        if current_mod <= 0 and current_count < 0:
            return True

        if prev_count_raw is None:
            if current_mod > prev_mod:
                log.debug("Bear content changed (+%.0fs)", current_mod - prev_mod)
                return True
            return False

        try:
            prev_count = int(prev_count_raw)
        except (TypeError, ValueError):
            prev_count = -1

        if current_mod != prev_mod or current_count != prev_count:
            parts = []
            if current_mod != prev_mod:
                parts.append(f"mod {prev_mod:.0f}→{current_mod:.0f}")
            if current_count != prev_count:
                parts.append(f"count {prev_count}→{current_count}")
            log.debug("Bear content changed (%s)", ", ".join(parts))
            return True
        return False

    # ── Folder files ─────────────────────────────────────────────────────

    def files_changed(self) -> tuple[bool, bool]:
        """Return ``(md_changed, tb_changed)``."""
        return (
            self._folder_changed("md_hashes", self.folder_md),
            self._folder_changed("tb_hashes", self.folder_tb),
        )

    def _folder_changed(self, key: str, folder: Path) -> bool:
        prev_raw = self._hash_state.get(key, {})
        prev = {k: tuple(v) if isinstance(v, list) else v
                for k, v in prev_raw.items()}
        snap = self._snapshots.get(folder) if self._snapshots else None
        current = snap.compute_hashes(prev) if snap else _hash_folder(folder, prev)
        if current == prev:
            return False
        added = set(current) - set(prev)
        removed = set(prev) - set(current)
        modified = {k for k in set(current) & set(prev) if current[k] != prev[k]}
        if added or removed or modified:
            parts = []
            if added:    parts.append(f"+{len(added)}")
            if removed:  parts.append(f"-{len(removed)}")
            if modified: parts.append(f"~{len(modified)}")
            log.debug("%s: %s", folder.name, " ".join(parts))
            return True
        return False

    # ── Snapshot saving ──────────────────────────────────────────────────

    def snapshot(
        self,
        state: dict,
        post_snapshots: Optional[dict[Path, VaultSnapshot]] = None,
    ) -> None:
        """Persist current file hashes into *state*."""
        h = state.setdefault("hashes", {})
        mod, count = bear_db_signature(self._bear_db)
        h["bear_max_mod"] = mod
        h["bear_note_count"] = count
        snap_md = post_snapshots.get(self.folder_md) if post_snapshots else None
        snap_tb = post_snapshots.get(self.folder_tb) if post_snapshots else None
        h["md_hashes"] = (
            snap_md.compute_hashes() if snap_md else _hash_folder(self.folder_md)
        )
        h["tb_hashes"] = (
            snap_tb.compute_hashes() if snap_tb else _hash_folder(self.folder_tb)
        )


# ---------------------------------------------------------------------------
# Fallback folder hashing (when no VaultSnapshot is available)
# ---------------------------------------------------------------------------

def _hash_folder(
    folder: Path, prev: Optional[dict] = None
) -> dict[str, tuple[int, float, str]]:
    """Walk *folder* and return ``{rel_path: (size, mtime, hash)}`` for all notes."""
    result: dict[str, tuple[int, float, str]] = {}
    if not folder.is_dir():
        return result

    for root, dirs, files in os.walk(folder):
        dirs[:] = [
            d for d in dirs
            if d not in CLOUD_JUNK_DIRS
            and d not in ("BearImages", ".obsidian")
            and not d.endswith(".textbundle")
        ]

        root_path = Path(root)
        # Handle .textbundle dirs
        try:
            for entry in os.listdir(root):
                if entry.endswith(".textbundle"):
                    tb_text = root_path / entry / "text.md"
                    if tb_text.is_file():
                        rel = str((root_path / entry).relative_to(folder))
                        result[rel] = _stat_hash_cached(tb_text, rel, prev)
        except OSError:
            pass

        for fname in files:
            if CLOUD_JUNK_RE.match(fname) or not _is_note_file(fname):
                continue
            fpath = root_path / fname
            rel = str(fpath.relative_to(folder))
            result[rel] = _stat_hash_cached(fpath, rel, prev)

    return result


def _stat_hash_cached(
    path: Path, rel: str, prev: Optional[dict]
) -> tuple[int, float, str]:
    try:
        st = path.stat()
        sz = st.st_size
        mtime = st.st_mtime
    except OSError:
        return (0, 0.0, "")

    if prev and rel in prev:
        cached = prev[rel]
        if isinstance(cached, (list, tuple)) and len(cached) >= 3:
            prev_sz, prev_mtime, prev_hash = cached[0], cached[1], cached[2]
            if sz == prev_sz and abs(mtime - float(prev_mtime)) < 0.001 and prev_hash:
                return tuple(cached)

    return (sz, mtime, _hash_file(path))


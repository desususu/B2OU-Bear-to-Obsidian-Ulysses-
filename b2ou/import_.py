"""
Import phase: Markdown / TextBundle files on disk → Bear (via x-callback-url).

Entry point: ``import_changed_notes(config)``

Only notes modified after the last sync timestamp are processed.  For each
changed note the function:
  1. Backs up the current disk file to *config.backup_path*.
  2. Detects the note's BearID (or recovers it by title lookup).
  3. Checks for a sync conflict (Bear modified since last export).
  4. Uploads new images to Bear and rewrites the markdown.
  5. Sends the updated text to Bear via ``bear://x-callback-url/add-text``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import time
import urllib.parse
from pathlib import Path
from typing import Callable, Optional

from b2ou.config import ExportConfig
from b2ou.constants import IMPORT_SKIP_DIRS, NOTE_EXTENSIONS
from b2ou.db import (
    core_data_to_unix,
    get_note_by_title,
    get_note_by_uuid,
    get_note_files_by_uuid,
    get_note_modification,
    open_readonly,
)
from b2ou.images import process_import_images
from b2ou.markdown import (
    extract_bear_id,
    first_heading,
    html_img_to_markdown,
    insert_link_top_note,
    normalize_local_image_ref,
    ref_links_to_inline,
    restore_image_links_md,
    restore_image_links_tb,
    strip_bear_id,
    tag_from_path,
)
from b2ou.platform_macos import bear_x_callback, open_bear_url

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sync timestamp helpers
# ---------------------------------------------------------------------------

def read_sync_timestamps(config: ExportConfig) -> tuple[float, float]:
    """Return ``(ts_last_sync, ts_last_export)`` from sentinel files, or (0, 0)."""
    try:
        ts_sync = config.sync_ts_file.stat().st_mtime
    except OSError:
        ts_sync = 0.0
    try:
        ts_export = config.export_ts_file.stat().st_mtime
    except OSError:
        ts_export = 0.0
    return ts_sync, ts_export


def update_sync_timestamp(config: ExportConfig, ts: float) -> None:
    """Touch the sync-time sentinel file with timestamp *ts*."""
    import datetime
    config.sync_ts_file.parent.mkdir(parents=True, exist_ok=True)
    config.sync_ts_file.write_text(
        "Checked for Markdown updates to sync at: "
        + datetime.datetime.now().strftime("%Y-%m-%d at %H:%M:%S"),
        encoding="utf-8",
    )
    os.utime(config.sync_ts_file, (-1, ts))


# ---------------------------------------------------------------------------
# Backup helpers
# ---------------------------------------------------------------------------

def backup_disk_note(md_file: Path, backup_path: Path) -> None:
    """Copy *md_file* (or its containing textbundle) to *backup_path*."""
    backup_path.mkdir(parents=True, exist_ok=True)
    if ".textbundle" in str(md_file):
        bundle = md_file.parent
        target_base = backup_path / bundle.name
        target = Path(str(target_base))
        stem = target_base.stem
        ext = target_base.suffix
        count = 2
        while target.exists():
            target = backup_path / f"{stem} - {str(count).zfill(2)}{ext}"
            count += 1
        shutil.copytree(bundle, target)
    else:
        shutil.copy2(md_file, backup_path)


def backup_bear_note(uuid: str, config: ExportConfig, conn: sqlite3.Connection) -> str:
    """
    Write the current Bear note text to *config.backup_path* as a backup.
    Returns the note title (empty string on failure).
    """
    import datetime

    from b2ou.markdown import insert_link_top_note

    title = ""
    try:
        note = get_note_by_uuid(conn, uuid)
        if not note:
            return title

        title = note.title
        text = insert_link_top_note(note.text, "Link to updated note: ", uuid)
        mod_unix = core_data_to_unix(note.modified_date)
        cre_dt = datetime.datetime.fromtimestamp(core_data_to_unix(note.creation_date))
        filename = _clean_title(title) + cre_dt.strftime(" - %Y-%m-%d_%H%M")

        config.backup_path.mkdir(parents=True, exist_ok=True)
        base = config.backup_path / filename
        dest = Path(str(base) + ".txt")
        count = 2
        while dest.exists():
            dest = Path(f"{base} - {str(count).zfill(2)}.txt")
            count += 1

        dest.write_text(text, encoding="utf-8")
        os.utime(dest, (-1, mod_unix))
        log.debug("Backed up Bear note: %s", dest.name)
    except Exception as exc:
        log.warning("backup_bear_note failed for %s: %s", uuid, exc)
    return title


def _clean_title(title: str) -> str:
    import re
    title = title[:225].strip() or "Untitled"
    title = re.sub(r"[\/\\:]", "-", title)
    title = re.sub(r"-$", "", title)
    return title.strip()


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

def check_sync_conflict(
    uuid: str,
    ts_last_export: float,
    conn: sqlite3.Connection,
) -> bool:
    """Return True if Bear has modified the note since the last export."""
    try:
        mod = get_note_modification(conn, uuid)
        if mod is not None:
            return core_data_to_unix(mod) > ts_last_export
    except Exception as exc:
        log.warning("check_sync_conflict error for %s: %s", uuid, exc)
    return False


# ---------------------------------------------------------------------------
# Vault index
# ---------------------------------------------------------------------------

def build_vault_index(root_path: Path) -> dict[str, Path]:
    """Return ``{filename: absolute_path}`` for every file in the vault."""
    index: dict[str, Path] = {}
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirnames[:] = [
            d for d in dirnames
            if d not in (".obsidian", ".git", "__pycache__")
        ]
        for fname in filenames:
            if fname not in index:
                index[fname] = Path(dirpath) / fname
    return index


# ---------------------------------------------------------------------------
# Iter changed notes
# ---------------------------------------------------------------------------

def iter_changed_note_files(
    root_path: Path, ts_last_sync: float, config: ExportConfig
) -> list[tuple[Path, float]]:
    """Return list of ``(abs_path, mtime)`` for notes changed since last sync."""
    results: list[tuple[Path, float]] = []

    # 1. Attempt to load previous hashes from daemon state
    state_file = Path.cwd() / ".b2ou_state.json"
    prev_hashes: dict[str, tuple] = {}
    if state_file.is_file():
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
            key = "tb_hashes" if config.export_as_textbundles else "md_hashes"
            prev_hashes = state.get("hashes", {}).get(key, {})
        except Exception as exc:
            log.warning("Could not read state for import hash comparison: %s", exc)

    def _check_add(fpath: Path, rel: str) -> None:
        try:
            st = fpath.stat()
            sz, mtime = st.st_size, st.st_mtime
        except OSError:
            return

        if not prev_hashes:
            if mtime > ts_last_sync:
                results.append((fpath, mtime))
            return

        cached = prev_hashes.get(rel)
        if not cached:
            results.append((fpath, mtime))
            return

        from b2ou.snapshot import _hash_file
        if isinstance(cached, (list, tuple)) and len(cached) >= 3:
            prev_sz, prev_mtime, prev_hash = cached[0], cached[1], cached[2]
            if sz == prev_sz and abs(mtime - float(prev_mtime)) < 0.001:
                return

        curr_hash = _hash_file(fpath)
        cached_hash = cached[2] if len(cached) >= 3 else (cached[1] if len(cached) >= 2 else "")
        if curr_hash == cached_hash:
            return

        results.append((fpath, mtime))

    for root, dirnames, filenames in os.walk(root_path):
        dirnames[:] = [
            d for d in dirnames
            if d not in IMPORT_SKIP_DIRS and not d.startswith(".Ulysses")
        ]
        root_p = Path(root)

        for d in list(dirnames):
            if d.endswith(".textbundle"):
                tb_dir = root_p / d
                tb_text = tb_dir / "text.md"
                if tb_text.is_file():
                    _check_add(tb_text, str(tb_dir.relative_to(root_path)))
                dirnames.remove(d)

        for fname in filenames:
            if not any(fname.endswith(ext) for ext in NOTE_EXTENSIONS):
                continue
            if fname in (".sync-time.log", ".export-time.log", ".DS_Store"):
                continue
            fpath = root_p / fname
            _check_add(fpath, str(fpath.relative_to(root_path)))

    return results


# ---------------------------------------------------------------------------
# Update Bear note (plain .md)
# ---------------------------------------------------------------------------

def update_bear_note(
    text: str,
    md_file: Path,
    ts: float,
    ts_last_export: float,
    config: ExportConfig,
    conn: sqlite3.Connection,
    vault_index_fn: Optional[Callable[[], dict[str, Path]]] = None,
) -> None:
    """Send an updated Markdown file back to Bear."""
    text = _restore_for_import(text, config)

    uuid = extract_bear_id(text)
    if uuid:
        text = strip_bear_id(text).lstrip() + "\n"

        conflict = check_sync_conflict(uuid, ts_last_export, conn)
        text = process_import_images(
            text, md_file, config.export_path, config.assets_path,
            note_uuid=uuid, vault_index=vault_index_fn() if vault_index_fn else None,
        )

        if conflict:
            link_original = "bear://x-callback-url/open-note?id=" + uuid
            message = (
                f"::Sync conflict! External update: "
                f"{_fmt_ts(ts)}::"
                f"\n[Click here to see original Bear note]({link_original})"
            )
            x_create = "bear://x-callback-url/create?show_window=no&open_note=no"
            bear_x_callback(x_create, text, message, "")
        else:
            backup_bear_note(uuid, config, conn)
            x_replace = (
                "bear://x-callback-url/add-text?show_window=no&open_note=no"
                "&mode=replace_all&id=" + uuid
            )
            bear_x_callback(x_replace, text, "", "")
    else:
        # No BearID — try to recover by title lookup
        title = first_heading(text)
        note = get_note_by_title(conn, title)
        recovered_uuid = note.uuid if note else None

        vi = vault_index_fn() if vault_index_fn else None
        if recovered_uuid:
            text = process_import_images(
                text, md_file, config.export_path, config.assets_path,
                note_uuid=recovered_uuid, vault_index=vi,
            )
            backup_bear_note(recovered_uuid, config, conn)
            x_replace = (
                "bear://x-callback-url/add-text?show_window=no&open_note=no"
                "&mode=replace_all&id=" + recovered_uuid
            )
            bear_x_callback(x_replace, text, "", "")
        else:
            # Brand-new note created outside Bear
            tagged = tag_from_path(
                text, str(md_file), str(config.export_path),
                file_tags=_get_file_tags(md_file),
            )
            x_create = "bear://x-callback-url/create?show_window=no"
            bear_x_callback(x_create, tagged, "", "")
            time.sleep(1.0)

            new_note = get_note_by_title(conn, title)
            new_uuid = new_note.uuid if new_note else None
            final_text = process_import_images(
                tagged, md_file, config.export_path, config.assets_path,
                note_uuid=new_uuid, note_title=title, vault_index=vi,
            )

            if final_text != tagged:
                if new_uuid:
                    x_replace = (
                        "bear://x-callback-url/add-text?show_window=no"
                        "&open_note=no&mode=replace_all&id=" + new_uuid
                    )
                else:
                    safe_title = urllib.parse.quote(title)
                    x_replace = (
                        "bear://x-callback-url/add-text?show_window=no"
                        "&open_note=no&mode=replace_all&title=" + safe_title
                    )
                bear_x_callback(x_replace, final_text, "", "")


# ---------------------------------------------------------------------------
# TextBundle import
# ---------------------------------------------------------------------------

def textbundle_to_bear(
    text: str,
    md_file: Path,
    mod_dt: float,
    config: ExportConfig,
    conn: sqlite3.Connection,
) -> None:
    """Import a ``.textbundle`` note back into Bear."""
    from b2ou.constants import RE_MD_IMAGE

    text = _restore_for_import_tb(text)
    bundle = md_file.parent

    # Resolve UUID via multiple strategies
    uuid: Optional[str] = None
    bearid_path = bundle / ".bearid"
    if bearid_path.exists():
        uuid = bearid_path.read_text(encoding="utf-8").strip() or None

    if not uuid:
        uuid = extract_bear_id(text)

    if not uuid:
        info_path = bundle / "info.json"
        if info_path.exists():
            try:
                uuid = json.loads(
                    info_path.read_text(encoding="utf-8")
                ).get("bear_uuid") or None
            except Exception as exc:
                log.warning("Could not read info.json: %s", exc)

    if not uuid:
        title = first_heading(text)
        note = get_note_by_title(conn, title)
        uuid = note.uuid if note else None

    if uuid:
        clean = strip_bear_id(text)
        clean = RE_MD_IMAGE.sub(_fix_tb_image_path, clean)
        id_tag = f"\n\n[//]: # ({{BearID:{uuid}}})\n"

        # Write updated textbundle file on disk
        from b2ou.export import write_note_file
        write_note_file(md_file, clean.rstrip() + id_tag, mod_dt, 0)
        write_note_file(bundle / ".bearid", uuid, mod_dt, 0)

        # Keep info.json in sync
        info_path = bundle / "info.json"
        if info_path.exists():
            try:
                data = json.loads(info_path.read_text(encoding="utf-8"))
                data["bear_uuid"] = uuid
                write_note_file(info_path, json.dumps(data, indent=4), mod_dt, 0)
            except Exception:
                pass

        # Copy and upload new images
        assets_dir = bundle / "assets"
        assets_dir.mkdir(exist_ok=True)
        _upload_tb_images(clean, bundle, assets_dir, uuid, config, conn)

        # Send final text to Bear
        bear_md = RE_MD_IMAGE.sub(_restore_img_format_fn(conn, uuid), clean)
        x_replace = (
            f"bear://x-callback-url/add-text?show_window=no&open_note=no"
            f"&mode=replace_all&id={uuid}"
            f"&text={urllib.parse.quote(bear_md, safe='')}"
        )
        open_bear_url(x_replace)
        time.sleep(0.5)
    else:
        # No UUID found — open the bundle directly in Bear
        text = tag_from_path(text, str(bundle), str(config.export_path))
        from b2ou.export import write_note_file
        write_note_file(md_file, text, mod_dt, 0)
        os.utime(bundle, (-1, mod_dt))
        import subprocess
        subprocess.call(["open", "-a", "Bear", str(bundle)])
        time.sleep(0.5)


def _fix_tb_image_path(m: "re.Match") -> str:
    """Normalise image URL to ``assets/filename`` inside a textbundle."""
    image_url = m.group(2)
    if image_url.startswith("http"):
        return m.group(0)
    filename = urllib.parse.unquote(image_url).split("/")[-1]
    return f"![{m.group(1)}](assets/{urllib.parse.quote(filename)})"


def _restore_img_format_fn(conn: sqlite3.Connection, uuid: str):
    """Factory: return a regex-sub function that strips UUID prefixes for Bear import."""
    from b2ou.constants import RE_MD_IMAGE

    existing_prefixed_names: set[str] = set()
    try:
        for nf in get_note_files_by_uuid(conn, uuid):
            if nf.filename and nf.uuid:
                existing_prefixed_names.add(f"{nf.uuid}_{nf.filename}")
    except Exception:
        pass

    def _fn(m: "re.Match") -> str:
        image_url = m.group(2)
        if image_url.startswith("http"):
            return m.group(0)
        filename = urllib.parse.unquote(image_url).split("/")[-1]
        if filename in existing_prefixed_names and "_" in filename:
            clean_name = filename.split("_", 1)[1]
        else:
            clean_name = filename
        return f"![{m.group(1)}]({urllib.parse.quote(clean_name)})"

    return _fn


def _upload_tb_images(
    text: str,
    bundle: Path,
    assets_dir: Path,
    uuid: str,
    config: ExportConfig,
    conn: sqlite3.Connection,
) -> None:
    """Copy new images into the bundle's assets/ folder and upload to Bear."""
    import base64
    from b2ou.constants import RE_MD_IMAGE
    from b2ou.platform_macos import open_bear_url

    existing_bear_filenames: set[str] = set()
    existing_prefixed_names: set[str] = set()
    try:
        for nf in get_note_files_by_uuid(conn, uuid):
            if nf.filename:
                existing_bear_filenames.add(nf.filename)
            if nf.filename and nf.uuid:
                existing_prefixed_names.add(f"{nf.uuid}_{nf.filename}")
    except Exception as exc:
        log.warning("Could not read Bear attachments for %s: %s", uuid, exc)

    new_images: dict[str, Path] = {}

    for m in RE_MD_IMAGE.finditer(text):
        img_url = m.group(2)
        if img_url.startswith(("http://", "https://")):
            continue
        source_path, img_filename = _resolve_tb_image_source(
            img_url, bundle, assets_dir, config
        )
        if not img_filename:
            continue
        asset_path = assets_dir / img_filename
        if source_path and source_path != asset_path and not asset_path.exists():
            shutil.copy2(source_path, asset_path)
        already = (
            img_filename in existing_bear_filenames
            or img_filename in existing_prefixed_names
        )
        if not already and asset_path.exists():
            new_images.setdefault(img_filename, asset_path)
        elif source_path is None:
            log.debug("TB image missing: %s  in %s", img_url, bundle)

    for filename, filepath in new_images.items():
        try:
            encoded = base64.b64encode(filepath.read_bytes()).decode("utf-8")
            safe_filename = urllib.parse.quote(filename)
            safe_file = urllib.parse.quote(encoded, safe="")
            x_add = (
                f"bear://x-callback-url/add-file?show_window=no&open_note=no"
                f"&id={uuid}&filename={safe_filename}&mode=append&file={safe_file}"
            )
            open_bear_url(x_add)
            time.sleep(0.3)
            existing_bear_filenames.add(filename)
        except Exception as exc:
            log.warning("Image upload failed for %s: %s", filename, exc)


def _resolve_tb_image_source(
    img_url: str,
    bundle: Path,
    assets_dir: Path,
    config: ExportConfig,
) -> tuple[Optional[Path], str]:
    """Resolve a local image source path for a textbundle note."""
    ref = normalize_local_image_ref(img_url)
    if not ref:
        return None, ""
    basename = os.path.basename(ref)
    candidates = []
    if os.path.isabs(ref):
        candidates.append(Path(ref))
    else:
        candidates.extend([
            (bundle / ref).resolve(),
            bundle / basename,
            assets_dir / basename,
            config.export_path / basename,
            config.assets_path / basename,
        ])
    for c in candidates:
        if c and c.exists():
            return c, basename
    return None, basename


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _restore_for_import(text: str, config: ExportConfig) -> str:
    """Pre-process note text before sending it back to Bear (plain .md)."""
    text = html_img_to_markdown(text)
    text = ref_links_to_inline(text)
    if config.export_image_repository:
        text = restore_image_links_md(
            text,
            str(config.assets_path),
            str(config.export_path),
        )
    return text


def _restore_for_import_tb(text: str) -> str:
    """Pre-process textbundle text before sending it back to Bear."""
    text = html_img_to_markdown(text)
    text = restore_image_links_tb(text)
    return text


def _fmt_ts(ts: float) -> str:
    import datetime
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d at %H:%M")


def _get_file_tags(md_file: Path) -> list[str]:
    try:
        from b2ou.platform_macos import get_file_tags
        return get_file_tags(md_file)
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Main import entry point
# ---------------------------------------------------------------------------

def import_changed_notes(config: ExportConfig) -> bool:
    """
    Scan *config.export_path* for notes changed since the last sync and
    push them back to Bear.  Returns True if any updates were sent.
    """
    if (not config.sync_ts_file.exists()
            or not config.export_ts_file.exists()):
        return False

    ts_last_sync, ts_last_export = read_sync_timestamps(config)
    current_sync_ts = time.time()
    update_sync_timestamp(config, current_sync_ts)

    changed = iter_changed_note_files(config.export_path, ts_last_sync, config)
    if not changed:
        return False

    # Lazy vault index — built only when first needed
    _vault_index: Optional[dict[str, Path]] = None

    def get_vault_index() -> dict[str, Path]:
        nonlocal _vault_index
        if _vault_index is None:
            _vault_index = build_vault_index(config.export_path)
        return _vault_index

    conn = None
    try:
        conn = open_readonly(config.bear_db)
    except Exception as exc:
        log.warning("Could not open Bear DB read-only: %s", exc)

    updates_found = False
    try:
        for md_file, ts in changed:
            if not updates_found:
                time.sleep(1)
            updates_found = True
            try:
                text = md_file.read_text(encoding="utf-8")
                backup_disk_note(md_file, config.backup_path)
                if ".textbundle" in str(md_file):
                    textbundle_to_bear(text, md_file, ts, config, conn)
                    log.info("Imported to Bear: %s", md_file)
                else:
                    update_bear_note(
                        text, md_file, ts, ts_last_export, config,
                        conn, vault_index_fn=get_vault_index,
                    )
                    log.info("Bear note updated: %s", md_file)
            except Exception as exc:
                log.error("Failed to import %s: %s", md_file, exc)
    finally:
        if conn is not None:
            conn.close()

    return updates_found

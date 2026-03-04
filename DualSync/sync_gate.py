#!/usr/bin/env python3
"""
sync_gate.py — Smart sync gate for Bear Export (v4.3)

The #1 design constraint: NEVER interrupt active editing.

Two operating modes:

  RUN-ONCE (default):  Called by launchd on a timer.  Checks guards,
    syncs if safe, exits.  Low memory footprint but 30-75s latency.

  DAEMON (--daemon):  Resident process kept alive by launchd.  Watches
    the filesystem via FSEvents (watchdog library) and syncs within
    seconds of changes.  The debounce timer ensures bursts of writes
    (e.g. the user typing) are batched into a single sync cycle.

    Two-stage design — debounce and editing guard serve DIFFERENT roles:

      Stage 1 (debounce, 3s default):  Batches rapid FSEvents into a
        single "something changed" signal.  Replaces launchd's polling.
        Configurable via daemon_debounce_seconds.

      Stage 2 (editing guard, 30s default):  After debounce fires, the
        full editing guard runs with write_quiet_seconds as its Layer 2
        threshold.  This is identical to run-once mode's protection.
        If the guard blocks, retries fire at daemon_retry_seconds until
        the guard clears.

    Net latency by change source:
      Bear DB change → 3s debounce → Layer 2 passes (folders unchanged)
                     → sync in ~3-5s
      File edit      → 3s debounce → Layer 2 blocks (file modified 3s ago,
                     need 30s) → retries → sync in ~30-35s (same safety
                     as run-once, but no 0-45s timer jitter)

    Self-event suppression: after sync completes, a cooldown window
    (debounce + 1s, minimum 5s) silently drops incoming FSEvents to
    absorb the daemon's own write echoes.

    Event flow:
      FSEvent → debounce timer resets (3s default)
      3s of quiet → editing guard check (all three layers)
      → Layer 2 blocks → retry in 5s → ... → guard clears → sync
      → cooldown window → back to watching

Three-layer editing guard (both modes):
  Layer 1 — File-open check (lsof on directories, not individual files)
            Are note files held open by any editor process?
  Layer 2 — Write-settle (mtime)
            Was any note file modified in the last N seconds?
            In daemon mode, the debounce timer serves this role.
  Layer 3 — Frontmost app (NSWorkspace native API, no subprocess)
            Is a known editor in the foreground?

v4.3 additions (daemon mode):
  • FSEvents via watchdog: watches Bear database directory (non-recursive)
    and export folders (recursive for tag subdirectories).
  • Two-stage protection: debounce timer (daemon_debounce_seconds) for
    event batching + full editing guard (write_quiet_seconds) for edit
    protection.  These are independent — debounce wakes the daemon,
    the guard decides if it's safe to sync.
  • Retry on guard block: if the editing guard blocks after debounce,
    retries at daemon_retry_seconds (default 5s) until clear.
  • Minimum sync interval: enforces sync_interval_seconds between
    sync completions, matching run-once mode.
  • Self-event suppression: a post-sync cooldown window (debounce + 1s,
    minimum 5s) silently drops incoming FSEvents to absorb the daemon's
    own export write echoes.  Augments the _syncing flag which covers
    events during the sync itself.
  • Startup cycle: runs one immediate sync at daemon start to catch
    changes that occurred while the daemon was down.
  • Polling fallback: if watchdog is not installed, the daemon falls
    back to a simple polling loop at sync_interval_seconds.
  • Signal handling: SIGTERM/SIGINT trigger clean shutdown (stop
    observers, save state, release lock).

v4.2 optimizations (retained):
  • VaultSnapshot class: single os.walk per folder per phase.
  • Content hashing: xxhash (xxh3_128) with SHA-256 fallback.
  • Pre-compiled regex, size-based hash fast-reject, lsof +D.

v4.1 optimizations (retained):
  • Layer 3 uses PyObjC NSWorkspace (<1ms) with subprocess fallback.
  • Layer 1 uses lsof +D on directories.

Data flow (hub-and-spoke, Bear is truth):
  MD/TB edit → import to Bear → export to BOTH MD+TB  (single cycle)
  Bear edit  →                   export to BOTH MD+TB
  Delete in MD/TB → ignored (export recreates from Bear)

Usage:
  python3 sync_gate.py                  # run-once (called by launchd)
  python3 sync_gate.py --daemon         # resident daemon with FSEvents
  python3 sync_gate.py --force          # bypass all guards
  python3 sync_gate.py --export-only    # skip import phase
  python3 sync_gate.py --dry-run        # show what would happen
  python3 sync_gate.py --guard-test     # test all guard layers
"""

import argparse
import fcntl
import fnmatch
try:
    import xxhash
    _USE_XXHASH = True
except ImportError:
    import hashlib
    _USE_XXHASH = False
import json
import logging
import logging.handlers
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time

# ─── Optional: PyObjC for native frontmost-app detection ─────────────────────
try:
    from AppKit import NSWorkspace, NSAutoreleasePool
    from Foundation import NSRunLoop, NSDate
    _HAS_APPKIT = True
except ImportError:
    _HAS_APPKIT = False

# ─── Optional: watchdog for FSEvents-driven daemon mode ──────────────────────
try:
    from watchdog.observers import Observer as _WatchdogObserver
    from watchdog.events import FileSystemEventHandler as _FSEventHandler
    _HAS_WATCHDOG = True
except ImportError:
    _WatchdogObserver = None
    _FSEventHandler = object          # base class stub for _SyncEventHandler
    _HAS_WATCHDOG = False

# ─── Paths ────────────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "sync_config.json")
LOG_FILE    = os.path.join(SCRIPT_DIR, "sync_gate.log")
LOCK_FILE   = os.path.join(SCRIPT_DIR, ".sync_gate.lock")
STATE_FILE  = os.path.join(SCRIPT_DIR, ".sync_gate_state.json")

BEAR_DB_PATH = os.path.expanduser(
    "~/Library/Group Containers/9K33E3U3T4.net.shinyfrog.bear"
    "/Application Data/database.sqlite"
)

_CD_EPOCH = 978307200.0  # Core Data epoch: 2001-01-01 UTC

# ─── Defaults ─────────────────────────────────────────────────────────────────

DEFAULTS = {
    "script_path":            "./bear_export_sync.py",
    "python_path":            "",
    "folder_md":              "./Export/MD_Export",
    "folder_tb":              "./Export/TB_Export",
    "backup_md":              "./Backup/MD_Backup",
    "backup_tb":              "./Backup/TB_Backup",
    "sync_interval_seconds":  30,
    "write_quiet_seconds":    30,
    "editor_cooldown_seconds": 5,
    "bear_settle_seconds":    3,
    "conflict_backup_dir":    "",
    "daemon_debounce_seconds": 3.0,
    "daemon_retry_seconds":   5.0,
}

# ─── Cloud-sync junk filtering ───────────────────────────────────────────────

CLOUD_JUNK_DIRS = frozenset({
    "@eaDir", "#recycle", ".SynologyDrive", "@SynoResource",
    ".sync", ".stfolder", ".stversions", "__MACOSX",
    ".dropbox.cache", ".dropbox",
})

# Pre-compiled regex for junk file detection (replaces per-call fnmatch loop)
_CLOUD_JUNK_RE = re.compile(
    r"^("
    r"\.DS_Store|\.\_\.DS_Store|\._.*|\.syncloud_.*"
    r"|Thumbs\.db|desktop\.ini|.*\.tmp|~\$.*"
    r"|\.~lock\..*|.*\.swp|.*\.crdownload|\.fuse_hidden.*"
    r"|.*\.partial|.*\.part|\.dropbox|Icon\r"
    r")$"
)

_NOTE_EXTENSIONS = frozenset((".md", ".txt", ".markdown"))
_SENTINEL_FILES  = frozenset((".sync-time.log", ".export-time.log"))


def _is_cloud_junk(path: str) -> bool:
    basename = os.path.basename(path)
    if _CLOUD_JUNK_RE.match(basename):
        return True
    parts = path.replace("\\", "/").split("/")
    return any(p in CLOUD_JUNK_DIRS for p in parts)


def _is_note_file(fname: str) -> bool:
    return (any(fname.endswith(ext) for ext in _NOTE_EXTENSIONS)
            and fname not in _SENTINEL_FILES)


def _clean_cloud_junk(folder: str) -> int:
    if not os.path.isdir(folder):
        return 0
    removed = 0
    for root, dirs, files in os.walk(folder, topdown=True):
        for d in list(dirs):
            if d in CLOUD_JUNK_DIRS:
                try:
                    shutil.rmtree(os.path.join(root, d))
                    removed += 1
                except OSError:
                    pass
                dirs.remove(d)
        for fname in files:
            fpath = os.path.join(root, fname)
            if _CLOUD_JUNK_RE.match(fname):
                try:
                    os.remove(fpath)
                    removed += 1
                except OSError:
                    pass
            elif _is_note_file(fname):
                try:
                    if os.path.getsize(fpath) == 0:
                        os.remove(fpath)
                        removed += 1
                except OSError:
                    pass
    return removed


# ═══════════════════════════════════════════════════════════════════════════════
# VAULT SNAPSHOT — single walk, all data collected once  (v4.2)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Replaces the pattern where _newest_note_mtime(), _hash_folder_notes(),
# and _clean_cloud_junk() each independently os.walk the same trees.
# A single VaultSnapshot walks once and serves all three needs.


class _NoteEntry:
    """Mutable record for one note file discovered during the walk."""
    __slots__ = ("abs_path", "mtime", "size")

    def __init__(self, abs_path: str, mtime: float, size: int):
        self.abs_path = abs_path
        self.mtime = mtime
        self.size = size


class VaultSnapshot:
    """One os.walk of a folder, collecting everything the sync cycle needs.

    Attributes
    ----------
    folder : str           Absolute path of the root folder.
    notes : dict           {rel_path: _NoteEntry} for every note file found.
    newest_mtime : float   Max mtime across all note files.
    junk_files : list      Absolute paths of cloud-junk files to remove.
    junk_dirs : list       Absolute paths of cloud-junk directories to remove.
    """

    def __init__(self, folder: str):
        self.folder = folder
        self.notes: dict = {}          # rel_path → _NoteEntry
        self.newest_mtime: float = 0.0
        self.junk_files: list = []
        self.junk_dirs: list = []
        if os.path.isdir(folder):
            self._walk()

    # ── core walk ────────────────────────────────────────────────────────

    def _walk(self) -> None:
        for root, dirs, files in os.walk(self.folder):
            keep = []
            for d in dirs:
                if d in CLOUD_JUNK_DIRS:
                    self.junk_dirs.append(os.path.join(root, d))
                elif d == "BearImages" or d == ".obsidian":
                    pass  # skip, don't descend
                elif d.endswith(".textbundle"):
                    self._add_textbundle(root, d)
                else:
                    keep.append(d)
            dirs[:] = keep

            for fname in files:
                fpath = os.path.join(root, fname)
                if _CLOUD_JUNK_RE.match(fname):
                    self.junk_files.append(fpath)
                    continue
                if not _is_note_file(fname):
                    continue
                try:
                    st = os.stat(fpath)
                    if st.st_size == 0:
                        self.junk_files.append(fpath)
                        continue

                    rel = os.path.relpath(fpath, self.folder)
                    self.notes[rel] = _NoteEntry(fpath, st.st_mtime, st.st_size)
                    if st.st_mtime > self.newest_mtime:
                        self.newest_mtime = st.st_mtime
                except OSError:
                    pass

    def _add_textbundle(self, root: str, dirname: str) -> None:
        tb_text = os.path.join(root, dirname, "text.md")
        try:
            st = os.stat(tb_text)
            rel = os.path.relpath(os.path.join(root, dirname), self.folder)
            self.notes[rel] = _NoteEntry(tb_text, st.st_mtime, st.st_size)
            if st.st_mtime > self.newest_mtime:
                self.newest_mtime = st.st_mtime
        except OSError:
            pass

    # ── cheap refresh (re-stat known files, NO walk) ────────────────────

    def refresh_mtimes(self) -> None:
        """Re-stat every known note to get fresh mtime/size without
        re-walking the directory tree.  O(n) stat calls, zero readdir."""
        self.newest_mtime = 0.0
        dead = []
        for rel, entry in self.notes.items():
            try:
                st = os.stat(entry.abs_path)
                entry.mtime = st.st_mtime
                entry.size = st.st_size
                if st.st_mtime > self.newest_mtime:
                    self.newest_mtime = st.st_mtime
            except OSError:
                dead.append(rel)
        for rel in dead:
            del self.notes[rel]

    # ── compute content hashes from snapshot data ───────────────────────

    def compute_hashes(self, prev_hashes: dict = None) -> dict:
        """Return {rel_path: (size, content_hash)} using the snapshot's
        cached sizes.  *prev_hashes* enables the same size-based
        fast-reject optimisation that _stat_and_hash_with_cache used."""
        result = {}
        for rel, entry in self.notes.items():
            if prev_hashes and rel in prev_hashes:
                cached = prev_hashes[rel]
                if isinstance(cached, (list, tuple)) and len(cached) >= 2:
                    prev_sz, prev_hash = cached[0], cached[1]
                    if entry.size == prev_sz and prev_hash:
                        result[rel] = (entry.size, prev_hash)
                        continue
            result[rel] = (entry.size, _hash_file(entry.abs_path))
        return result

    # ── junk removal from pre-collected paths ───────────────────────────

    def clean_junk(self) -> int:
        """Remove all cloud-junk files and dirs found during the walk."""
        removed = 0
        for d in self.junk_dirs:
            try:
                shutil.rmtree(d)
                removed += 1
            except OSError:
                pass
        for f in self.junk_files:
            try:
                os.remove(f)
                removed += 1
            except OSError:
                pass
        return removed


def _build_snapshots(folders: list) -> dict:
    """Build {folder_path: VaultSnapshot} for every folder in the list."""
    return {f: VaultSnapshot(f) for f in folders}


# ═══════════════════════════════════════════════════════════════════════════════
# EDITING GUARD — three independent detection layers
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Layer 1: lsof file-open check ───────────────────────────────────────────
#
# Uses lsof +D to check the entire directory at once, avoiding the need
# to enumerate individual files first.  This is both faster and more
# thorough than checking only the N most recent files.

_SYSTEM_PROCESS_PREFIXES = (
    "mds", "mdworker",
    "Finder", "fseventsd", "kernel",
    "SynologyDr", "CloudDrive",
    "Dropbox", "dbfsevent",
    "bird", "cloudd", "nsurlsessi",
    "python", "Python", "rsync",
    "launchd", "loginwindow",
    "com.apple",
    "revisiond", "quicklookd", "iconservi",
    "Spotlight",
)


def _is_system_process(name: str) -> bool:
    return any(name.startswith(p) for p in _SYSTEM_PROCESS_PREFIXES)


def _note_files_open_by_editor(folders: list) -> str:
    """
    Check if any editor process holds note files open in the given folders.

    Uses `lsof +D <dir>` which recursively scans the directory for open
    file descriptors.  This replaces the v4 approach of collecting the
    15 most recently modified files and passing them as arguments — +D
    is both faster (one syscall for the whole tree) and catches files
    that os.walk might miss (e.g. files opened by path before they
    appear in the directory listing).

    Returns the process name if found, empty string if all clear.
    """
    lsof_args = ["lsof", "-F", "pcn"]
    for folder in folders:
        if os.path.isdir(folder):
            lsof_args.extend(["+D", folder])
    if len(lsof_args) == 3:
        return ""  # no valid folders

    try:
        r = subprocess.run(
            lsof_args, capture_output=True, text=True, timeout=8,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""

    current_cmd = ""
    for line in r.stdout.splitlines():
        if line.startswith("c"):
            current_cmd = line[1:]
        elif line.startswith("n") and current_cmd:
            fname = os.path.basename(line[1:])
            # Only care about note files, not random opens on the dir
            if _is_note_file(fname) and not _is_system_process(current_cmd):
                return current_cmd

    return ""


# ─── Layer 2: Write-settle (mtime-based) ─────────────────────────────────────

def _newest_note_mtime(folder: str) -> float:
    newest = 0.0
    if not os.path.isdir(folder):
        return newest
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs
                   if d not in CLOUD_JUNK_DIRS
                   and d != "BearImages"
                   and d != ".obsidian"]
        for d in list(dirs):
            if d.endswith(".textbundle"):
                try:
                    newest = max(newest,
                                 os.stat(os.path.join(root, d, "text.md")).st_mtime)
                except OSError:
                    pass
                dirs.remove(d)
        for fname in files:
            if _CLOUD_JUNK_RE.match(fname) or not _is_note_file(fname):
                continue
            try:
                newest = max(newest, os.stat(os.path.join(root, fname)).st_mtime)
            except OSError:
                pass
    return newest


# ─── Layer 3: Frontmost app (native API) ─────────────────────────────────────
#
# v4.1: Uses NSWorkspace.sharedWorkspace().frontmostApplication() when
# PyObjC is available (<1ms, zero process overhead).  Falls back to
# lsappinfo/osascript subprocess only if AppKit import failed.

EDITOR_BUNDLE_IDS = {
    "net.shinyfrog.bear",
    "com.ulyssesapp.mac", "com.soulmen.ulysses3", "com.soulmen.ulysses",
    "md.obsidian",
    "abnerworks.typora", "io.typora.typora",
    "com.microsoft.VSCode", "com.microsoft.VSCodeInsiders",
    "com.sublimetext.4", "com.sublimetext.3",
    "com.apple.TextEdit", "com.apple.dt.Xcode",
    "com.coteditor.CotEditor", "com.macromates.TextMate",
    "com.barebones.bbedit", "com.panic.Nova",
    "org.vim.MacVim", "com.qvacua.VimR",
    "com.todesktop.230313mzl4w4u92",  # Cursor
    "dev.zed.Zed",
    "pro.writer.mac", "co.writer.mac",
    "com.xelaton.marktext-github",
    "com.joplinapp.desktop",
    "com.omz-software.Drafts",
    "io.github.nicehash.zettlr",
    "com.logseq.logseq",
    "com.lukilabs.lukiedit",
}

EDITOR_KEYWORDS = (
    "ulysses", "obsidian", "typora", "bear", "vscode", "sublime",
    "textedit", "textmate", "bbedit", "nova", "vim", "emacs", "cursor",
    "zed", "ia.writer", "iawriter", "writer.pro", "marktext",
    "joplin", "drafts", "zettlr", "logseq", "craft", "notepad",
)

_EDITOR_BID_LOWER = frozenset(b.lower() for b in EDITOR_BUNDLE_IDS)


def _get_frontmost_bid() -> str:
    """
    Get the bundle ID of the frontmost application.

    Priority:
      1. NSWorkspace native API  (<1ms, no subprocess)
      2. lsappinfo subprocess    (~50ms)
      3. osascript subprocess    (~200ms)

    Returns lowercase bundle ID or empty string.
    """
    # Method 1: Native PyObjC (fastest, most reliable)
    if _HAS_APPKIT:
        try:
            # NSAutoreleasePool
            pool = NSAutoreleasePool.alloc().init()
            try:
                NSRunLoop.currentRunLoop().runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.01))
                
                app = NSWorkspace.sharedWorkspace().frontmostApplication()
                if app:
                    bid = app.bundleIdentifier()
                    if bid:
                        return bid.lower()
            finally:
                del pool
        except Exception:
            pass

    # Method 2: lsappinfo (fast subprocess, no permissions needed)
    try:
        r = subprocess.run(
            ["lsappinfo", "info", "-only", "bundleid", "-app", "Front"],
            capture_output=True, text=True, timeout=3,
        )
        out = r.stdout.strip()
        if '="' in out:
            bid = out.split('="', 1)[1].strip('" \n')
            if bid:
                return bid.lower()
    except Exception:
        pass

    # Method 3: osascript (slowest, but most compatible)
    try:
        r = subprocess.run(
            ["osascript", "-e",
             'tell application "System Events" to get bundle identifier '
             'of first application process whose frontmost is true'],
            capture_output=True, text=True, timeout=3,
        )
        bid = r.stdout.strip()
        if bid:
            return bid.lower()
    except Exception:
        pass

    return ""


# ─── Combined editing guard ──────────────────────────────────────────────────

def check_editing_guard(folders: list, quiet_seconds: float,
                        last_sync_end: float,
                        verbose: bool = False,
                        log_all: bool = False,
                        snapshots: dict = None) -> str:
    """
    Check all three layers.  Returns a reason string if editing is
    detected (sync should NOT run), or empty string if all clear.

    If *snapshots* is provided ({folder: VaultSnapshot}), Layer 2 uses
    the pre-computed newest_mtime instead of re-walking the directories.

    If *log_all* is True, logs every layer's result at DEBUG level
    (used by daemon mode for diagnostics).

    Each result is prefixed with its layer tag:
      [frontmost]    — Layer 3: known editor app is frontmost
      [write-settle] — Layer 2: note file modified too recently
      [lsof]         — Layer 1: editor process holds note files open
    """
    results = {}  # layer_name → (blocked: bool, detail: str)

    # Layer 3 (cheapest — <1ms with PyObjC): frontmost app
    bid = _get_frontmost_bid()
    if bid and (bid in _EDITOR_BID_LOWER or any(kw in bid for kw in EDITOR_KEYWORDS)):
        results["frontmost"] = (True, bid)
    else:
        method = "PyObjC" if _HAS_APPKIT else "subprocess"
        results["frontmost"] = (False, f"{bid or '(none)'} via {method}")

    # Layer 2 (fast — mtime checks): write-settle
    now = time.time()
    folder_ages = {}
    for f in folders:
        if snapshots and f in snapshots:
            n = snapshots[f].newest_mtime
        else:
            n = _newest_note_mtime(f)
        if n > 0:
            if last_sync_end > 0 and abs(n - last_sync_end) < 2.0:
                folder_ages[f] = -1  # our own output
            else:
                folder_ages[f] = now - n

    unsettled = {f: age for f, age in folder_ages.items()
                 if age >= 0 and age < quiet_seconds}
    if unsettled:
        age_strs = [f"{os.path.basename(f)}={age:.0f}s" for f, age in unsettled.items()]
        results["write-settle"] = (True,
            f"ages: {', '.join(age_strs)}, need {quiet_seconds:.0f}s")
    else:
        settled_strs = []
        for f, age in folder_ages.items():
            if age < 0:
                settled_strs.append(f"{os.path.basename(f)}=own-output")
            else:
                settled_strs.append(f"{os.path.basename(f)}={age:.0f}s")
        results["write-settle"] = (False,
            f"settled ({', '.join(settled_strs)})" if settled_strs else "no files")

    # Layer 1 (most reliable — lsof +D): file-open check
    proc = _note_files_open_by_editor(folders)
    if proc:
        results["lsof"] = (True, f"process '{proc}'")
    else:
        results["lsof"] = (False, "no editor processes")

    # Verbose output for --guard-test
    if verbose:
        for layer in ("frontmost", "write-settle", "lsof"):
            blocked, detail = results[layer]
            status = "BLOCKED" if blocked else "ok"
            log.info("  [%-13s] %s  %s", layer, status, detail)

    # Daemon diagnostic logging (always on in daemon mode)
    if log_all:
        parts = []
        for layer in ("frontmost", "write-settle", "lsof"):
            blocked, detail = results[layer]
            mark = "BLOCK" if blocked else "ok"
            parts.append(f"[{layer}]={mark}({detail})")
        log.debug("Guard: %s", "  ".join(parts))

    # Return first blocking reason (cheapest-first order)
    for layer in ("frontmost", "write-settle", "lsof"):
        blocked, detail = results[layer]
        if blocked:
            return f"[{layer}] {detail}"

    return ""


# ─── Logging ──────────────────────────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("sync_gate")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=1_000_000, backupCount=2, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    if sys.stdout.isatty():
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger

log = _setup_logging()


# ─── State / Config ──────────────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULTS, f, indent=4, ensure_ascii=False)
        log.warning("Created default config at %s — review and restart.", CONFIG_FILE)
        sys.exit(1)
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)
    for k, v in DEFAULTS.items():
        cfg.setdefault(k, v)
    return cfg

def _resolve(raw: str) -> str:
    return raw if os.path.isabs(raw) else os.path.normpath(os.path.join(SCRIPT_DIR, raw))

def _get_python(cfg: dict) -> str:
    explicit = cfg.get("python_path", "").strip()
    if explicit:
        resolved = _resolve(explicit)
        if os.path.isfile(resolved) and os.access(resolved, os.X_OK):
            return resolved
        log.warning("python_path '%s' not found; using sys.executable", explicit)
    return sys.executable


# ═══════════════════════════════════════════════════════════════════════════════
# CONTENT-HASH CHANGE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _bear_db_max_mod() -> float:
    return _bear_db_signature()[0]


def _bear_db_signature() -> tuple:
    """Return a lightweight Bear-content signature: (max_mod_unix, note_count).

    max_mod_unix tracks the newest visible note modification timestamp.
    note_count catches deletes/archives where max_mod might not increase.
    """
    try:
        with sqlite3.connect(f"file:{BEAR_DB_PATH}?mode=ro", uri=True) as conn:
            row = conn.execute(
                "SELECT MAX(ZMODIFICATIONDATE), COUNT(*) "
                "FROM ZSFNOTE WHERE ZTRASHED = 0 AND ZARCHIVED = 0"
            ).fetchone()
            if row:
                max_mod = (row[0] + _CD_EPOCH) if row[0] is not None else 0.0
                note_count = int(row[1]) if row[1] is not None else 0
                return max_mod, note_count
    except Exception as exc:
        log.debug("Could not read Bear DB: %s", exc)
    return 0.0, -1


def _coerce_note_count(value, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _state_bear_signature(state: dict) -> tuple:
    hashes = state.get("hashes", {}) if isinstance(state, dict) else {}
    try:
        mod = float(hashes.get("bear_max_mod", 0.0) or 0.0)
    except (TypeError, ValueError):
        mod = 0.0
    count = _coerce_note_count(hashes.get("bear_note_count", -1), -1)
    return (mod, count)


def _hash_file(path: str) -> str:
    """Content hash for change detection.

    Uses xxhash (xxh3_128) when available — ~6× faster than SHA-256
    on typical hardware (~30 GB/s vs ~500 MB/s).  Falls back to
    SHA-256 if xxhash is not installed.

    The hash is only used for same-machine change detection, not
    security, so a fast non-cryptographic hash is ideal.
    """
    try:
        if _USE_XXHASH:
            h = xxhash.xxh3_128()
        else:
            h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


def _stat_and_hash(path: str) -> tuple:
    """Return (size, content_hash).  Used for fast-reject in change detection."""
    try:
        sz = os.path.getsize(path)
    except OSError:
        return (0, "")
    return (sz, _hash_file(path))


def _hash_folder_notes(folder: str, prev_snapshot: dict = None) -> dict:
    """
    Build {relative_path: (size, content_hash)} for all note files.

    If prev_snapshot is provided, files whose size matches the previous
    snapshot skip the expensive hash computation — they use a cheap
    size-only comparison as a fast-reject gate.  This cuts hashing work
    by ~90% in typical "nothing changed" cycles.
    """
    hashes = {}
    if not os.path.isdir(folder):
        return hashes

    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs
                   if d not in CLOUD_JUNK_DIRS
                   and d != "BearImages" and d != ".obsidian"
                   and not d.endswith(".textbundle")]

        # .textbundle: hash text.md
        try:
            for entry in os.listdir(root):
                if entry.endswith(".textbundle"):
                    tb_text = os.path.join(root, entry, "text.md")
                    if os.path.isfile(tb_text):
                        rel = os.path.relpath(os.path.join(root, entry), folder)
                        hashes[rel] = _stat_and_hash_with_cache(
                            tb_text, rel, prev_snapshot)
        except OSError:
            pass

        for fname in files:
            if _CLOUD_JUNK_RE.match(fname) or not _is_note_file(fname):
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, folder)
            hashes[rel] = _stat_and_hash_with_cache(fpath, rel, prev_snapshot)

    return hashes


def _stat_and_hash_with_cache(path: str, rel: str,
                              prev: dict = None) -> tuple:
    """
    Return (size, hash) for a file.

    If prev contains the same rel key and the file size matches,
    reuse the previous hash (size change is a reliable signal that
    content changed; same-size-different-content is vanishingly rare
    for text files and will be caught next cycle when the hash is
    recomputed after the prev cache expires).
    """
    try:
        sz = os.path.getsize(path)
    except OSError:
        return (0, "")

    if prev and rel in prev:
        cached_val = prev[rel]
        
        # Backward compat: older state files might store only a hash string.
        if isinstance(cached_val, (list, tuple)) and len(cached_val) >= 2:
            prev_sz, prev_hash = cached_val[0], cached_val[1]
            if sz == prev_sz and prev_hash:
                return (sz, prev_hash)  # fast path: skip hashing

    return (sz, _hash_file(path))


class ChangeDetector:
    def __init__(self, state: dict, cfg: dict,
                 snapshots: dict = None):
        self.hash_state = state.get("hashes", {})
        self.folder_md = _resolve(cfg["folder_md"])
        self.folder_tb = _resolve(cfg["folder_tb"])
        self._snapshots = snapshots  # {folder: VaultSnapshot} or None

    def bear_changed(self) -> bool:
        current_mod, current_count = _bear_db_signature()
        previous_mod = self.hash_state.get("bear_max_mod", 0.0)
        previous_count_raw = self.hash_state.get("bear_note_count", None)

        if current_mod <= 0 and current_count < 0:
            return True

        # Backward compatibility for old state files (no note-count key).
        if previous_count_raw is None:
            if current_mod > previous_mod:
                log.debug("Bear content changed (+%.0fs)", current_mod - previous_mod)
                return True
            return False

        previous_count = _coerce_note_count(previous_count_raw, -1)
        if current_mod != previous_mod or current_count != previous_count:
            parts = []
            if current_mod != previous_mod:
                parts.append(f"mod {previous_mod:.0f}->{current_mod:.0f}")
            if current_count != previous_count:
                parts.append(f"count {previous_count}->{current_count}")
            log.debug("Bear content changed (%s)", ", ".join(parts))
            return True
        return False

    def files_changed(self) -> tuple:
        md = self._check("md_hashes", self.folder_md)
        tb = self._check("tb_hashes", self.folder_tb)
        return md, tb

    def _check(self, key: str, folder: str) -> bool:
        prev_raw = self.hash_state.get(key, {})
        prev = {k: tuple(v) if isinstance(v, list) else v
                for k, v in prev_raw.items()}
        # Use snapshot if available, otherwise fall back to walking
        snap = self._snapshots.get(folder) if self._snapshots else None
        if snap:
            current = snap.compute_hashes(prev_hashes=prev)
        else:
            current = _hash_folder_notes(folder, prev_snapshot=prev)
        if current == prev:
            return False
        added    = set(current) - set(prev)
        removed  = set(prev) - set(current)
        modified = {k for k in set(current) & set(prev)
                    if current[k] != prev[k]}
        if added or removed or modified:
            parts = []
            if added:    parts.append(f"+{len(added)}")
            if removed:  parts.append(f"-{len(removed)}")
            if modified: parts.append(f"~{len(modified)}")
            log.debug("%s: %s", os.path.basename(folder), " ".join(parts))
            return True
        return False

    def snapshot(self, state: dict,
                 post_snapshots: dict = None) -> None:
        """Save current file hashes into state.

        If *post_snapshots* is provided, use those (already-built)
        VaultSnapshots instead of re-walking the directories."""
        h = state.setdefault("hashes", {})
        bear_mod, bear_count = _bear_db_signature()
        h["bear_max_mod"] = bear_mod
        h["bear_note_count"] = bear_count
        snap_md = post_snapshots.get(self.folder_md) if post_snapshots else None
        snap_tb = post_snapshots.get(self.folder_tb) if post_snapshots else None
        h["md_hashes"] = snap_md.compute_hashes() if snap_md else _hash_folder_notes(self.folder_md)
        h["tb_hashes"] = snap_tb.compute_hashes() if snap_tb else _hash_folder_notes(self.folder_tb)


# ─── DB quiesce ───────────────────────────────────────────────────────────────

def _db_is_quiet(quiet_seconds: float) -> bool:
    now = time.time()
    for suffix in ("", "-wal", "-shm"):
        try:
            if now - os.stat(BEAR_DB_PATH + suffix).st_mtime < quiet_seconds:
                return False
        except OSError:
            pass
    return True


# ─── Conflict logging ────────────────────────────────────────────────────────

def _log_export_overwrites(folder: str, pre_hashes: dict,
                           conflict_dir: str,
                           post_snapshot: 'VaultSnapshot' = None) -> int:
    if not conflict_dir:
        return 0
    if post_snapshot:
        post = post_snapshot.compute_hashes()
    else:
        post = _hash_folder_notes(folder)
    conflicts = 0
    for rel, pre_val in pre_hashes.items():
        post_val = post.get(rel)
        if pre_val and post_val and pre_val != post_val:
            log.warning("Export overwrote: %s (backup in BearSyncBackup)", rel)
            conflicts += 1
    return conflicts


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

def run_sync(cfg: dict, export_only: bool = False,
             files_changed: bool = False,
             pre_snapshots: dict = None) -> dict:
    """Run the sync cycle.

    Parameters
    ----------
    pre_snapshots : dict, optional
        {folder_path: VaultSnapshot} built before the sync.  Used for
        junk cleaning and pre-sync conflict hashes (avoids re-walking).

    Returns
    -------
    dict  {folder_path: VaultSnapshot} built AFTER the sync completes,
          so the caller can reuse them for post-sync state saving.
    """
    python    = _get_python(cfg)
    script    = _resolve(cfg["script_path"])
    folder_md = _resolve(cfg["folder_md"])
    folder_tb = _resolve(cfg["folder_tb"])
    backup_md = _resolve(cfg["backup_md"])
    backup_tb = _resolve(cfg["backup_tb"])

    conflict_dir = cfg.get("conflict_backup_dir", "").strip()
    if conflict_dir:
        conflict_dir = _resolve(conflict_dir)
        os.makedirs(conflict_dir, exist_ok=True)

    bear_settle = max(1, float(cfg.get("bear_settle_seconds", 3)))

    for d in (folder_md, folder_tb, backup_md, backup_tb):
        os.makedirs(d, exist_ok=True)

    # Junk cleaning — use pre-built snapshots if available
    for folder in (folder_md, folder_tb):
        snap = pre_snapshots.get(folder) if pre_snapshots else None
        if snap:
            n = snap.clean_junk()
        else:
            n = _clean_cloud_junk(folder)
        if n:
            log.debug("Cleaned %d junk items from %s", n, folder)

    def _run(fmt, out, backup, skip_import=False, skip_export=False):
        cmd = [python, script,
               "--out", out, "--backup", backup, "--format", fmt]
        if skip_import:
            cmd.append("--skipImport")
        if skip_export:
            cmd.append("--skipExport")
        phase = "export" if skip_import else "import"
        tag = f"{fmt.upper()}-{phase}"
        t0 = time.monotonic()
        try:
            r = subprocess.run(cmd, check=False,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            elapsed = time.monotonic() - t0
            stderr = r.stderr.decode(errors="replace").strip()
            if r.returncode == 0:
                log.info("[%s] ok  %.1fs", tag, elapsed)
            elif r.returncode == 1 and skip_import:
                msg = f"  stderr: {stderr[:400]}" if stderr else ""
                log.info("[%s] exported  %.1fs%s", tag, elapsed, msg)
            elif r.returncode == 1:
                msg = f"  stderr: {stderr[:400]}" if stderr else ""
                log.info("[%s] exit=1  %.1fs%s", tag, elapsed, msg)
            else:
                log.error("[%s] exit=%d %.1fs  stderr: %s",
                          tag, r.returncode, elapsed, stderr[:500])
            if elapsed < 0.05 and r.returncode != 0 and stderr:
                if "ModuleNotFoundError" in stderr or "ImportError" in stderr:
                    log.error("[%s] Missing module — run: %s -m pip install "
                              "pyobjc-framework-Cocoa", tag, python)
        except Exception as exc:
            log.error("[%s] %s", tag, exc)

    need_import = files_changed and not export_only
    plan = ("import+export" if need_import else "export-only")
    t0 = time.monotonic()
    log.info("── Sync [%s] ──────────────────────────────", plan)

    # Pre-sync conflict hashes — reuse pre-built snapshots
    if conflict_dir:
        snap_md = pre_snapshots.get(folder_md) if pre_snapshots else None
        snap_tb = pre_snapshots.get(folder_tb) if pre_snapshots else None
        pre_md = snap_md.compute_hashes() if snap_md else _hash_folder_notes(folder_md)
        pre_tb = snap_tb.compute_hashes() if snap_tb else _hash_folder_notes(folder_tb)
    else:
        pre_md, pre_tb = {}, {}

    if need_import:
        _run("md", folder_md, backup_md, skip_export=True)
        _run("tb", folder_tb, backup_tb, skip_export=True)
        pre_mod = _bear_db_max_mod()
        deadline = time.time() + bear_settle
        while time.time() < deadline:
            time.sleep(0.5)
            if _bear_db_max_mod() > pre_mod:
                time.sleep(0.5)
                break

    # Export ALWAYS runs (cross-sync guarantee: MD↔Bear↔TB)
    _run("md", folder_md, backup_md, skip_import=True)
    _run("tb", folder_tb, backup_tb, skip_import=True)

    # Build post-sync snapshots (single walk per folder for all post-sync needs)
    post_snapshots = _build_snapshots([folder_md, folder_tb])

    if conflict_dir:
        c = (_log_export_overwrites(folder_md, pre_md, conflict_dir,
                                     post_snapshot=post_snapshots.get(folder_md)) +
             _log_export_overwrites(folder_tb, pre_tb, conflict_dir,
                                     post_snapshot=post_snapshots.get(folder_tb)))
        if c:
            log.warning("Overwrites: %d (backups in %s)", c, backup_md)

    log.info("── Sync complete  %.1fs ─────────────────────",
             time.monotonic() - t0)
    return post_snapshots


# ═══════════════════════════════════════════════════════════════════════════════
# DAEMON MODE — FSEvents-driven sync with debouncing  (v4.3)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Instead of launchd launching a fresh Python process every 30-45 seconds,
# a single resident daemon watches the filesystem for changes using macOS
# FSEvents (via the `watchdog` library) and runs sync cycles only when
# something actually changes.
#
# Architecture — two-stage protection:
#
#   Stage 1: Debounce (daemon_debounce_seconds, default 3s)
#     watchdog Observer → _SyncEventHandler.on_any_event()
#       → SyncDaemon.schedule_sync()   [resets debounce timer]
#       → (debounce_seconds of FSEvent silence)
#       → SyncDaemon._on_timer()       [debounce expired — wake up]
#
#   Stage 2: Full editing guard (write_quiet_seconds, default 30s)
#     _run_sync_cycle() uses the SAME write_quiet_seconds as run-once
#     mode for Layer 2 (mtime check).  This is critical because:
#       - The debounce only guarantees N seconds of FSEvent silence
#       - The user might pause typing for 3s while still editing
#       - Layer 2's mtime check catches this: "file modified 3s ago,
#         need 30s of quiet → blocked"
#     Layer 3 (frontmost app) and Layer 1 (lsof) provide additional
#     safety as before.
#
#   Net effect by change source:
#     Bear DB change → debounce fires → Layer 2 checks FOLDER mtimes
#       (unchanged) → passes immediately → sync in ~3-5s
#     File edit → debounce fires → Layer 2 checks folder mtimes
#       (changed 3s ago, need 30s) → blocks → retries every 5s
#       → eventually clears → sync in ~30-35s
#
# Self-event suppression:
#   During sync: _syncing flag drops all incoming events.
#   After sync: _cooldown_until timestamp drops events for N seconds,
#   absorbing the async FSEvents from our own export writes.
#
# Fallback: if watchdog is not installed, the daemon falls back to a
# simple polling loop at sync_interval_seconds.


class _SyncEventHandler(_FSEventHandler):
    """Receives FSEvents and triggers debounced sync via the daemon."""

    _IGNORE_DIRS = frozenset({
        'BearImages', '.obsidian', '__pycache__', '.git',
    })
    _IGNORE_FILES = frozenset({
        '.DS_Store', '.sync-time.log', '.export-time.log',
        '.sync_gate_state.json', '.sync_gate_state.json.tmp',
        '.sync_gate.lock',
    })

    def __init__(self, daemon: 'SyncDaemon', source_tag: str):
        super().__init__()
        self.daemon = daemon
        self.source_tag = source_tag   # "bear_db" | "folder_md" | "folder_tb"

    def on_any_event(self, event):
        # Skip directory-level events (we only care about files)
        if getattr(event, 'is_directory', False):
            return

        # Open/close notifications are high-frequency noise and don't
        # represent stable content changes.
        event_type = getattr(event, 'event_type', '')
        if event_type in ("opened", "closed", "closed_no_write"):
            return

        path = getattr(event, 'src_path', '')
        if not path:
            return
        basename = os.path.basename(path)

        # Ignore system files, cloud junk, and our own sentinel files
        if basename in self._IGNORE_FILES:
            return
        if _CLOUD_JUNK_RE.match(basename):
            return

        # Ignore events inside excluded directories
        parts = path.split(os.sep)
        if any(p in self._IGNORE_DIRS for p in parts):
            return

        # Source-specific filtering
        if self.source_tag == "bear_db":
            # Only care about the database files
            if not basename.startswith("database.sqlite"):
                return
        else:
            # Export folders: only note files and textbundle content
            if not (_is_note_file(basename) or basename == "text.md"):
                return

        self.daemon.schedule_sync(self.source_tag)


class SyncDaemon:
    """Resident daemon that watches for filesystem changes and syncs.

    Uses watchdog (FSEvents on macOS) for near-instant change detection
    with configurable debouncing.  Falls back to polling if watchdog is
    not installed.
    """

    def __init__(self, cfg: dict, export_only: bool = False):
        self.cfg = cfg
        self.export_only = export_only
        self.state = _load_state()

        self.folder_md = _resolve(cfg["folder_md"])
        self.folder_tb = _resolve(cfg["folder_tb"])
        self.folders = [self.folder_md, self.folder_tb]
        self.bear_db_dir = os.path.dirname(BEAR_DB_PATH)

        # Debounce: how long to wait after the LAST FSEvent before
        # waking up and checking guards.  Batches rapid writes.
        self.debounce_s = max(1.0, float(
            cfg.get("daemon_debounce_seconds", 3.0)))

        # Write-settle: how long files must be quiet before sync is
        # allowed.  This is the SAME threshold as run-once mode's
        # Layer 2.  For file-folder changes (user editing in an
        # external editor), this protects against interrupting the
        # user.  For Bear-DB changes, Layer 2 checks folder mtimes
        # which haven't changed, so it passes immediately — giving
        # Bear→export syncs low latency (~debounce_s).
        self.write_quiet_s = max(5.0, float(
            cfg.get("write_quiet_seconds", 30)))

        # Retry: how often to recheck when the editing guard blocks.
        self.retry_s = max(1.0, float(
            cfg.get("daemon_retry_seconds", 5.0)))

        # Minimum interval between sync completions.
        self.min_interval_s = max(5.0, float(
            cfg.get("sync_interval_seconds", 30)))

        self.db_settle_s = min(5.0, float(
            cfg.get("bear_settle_seconds", 3)))

        # Post-sync cooldown: ignore events for this many seconds
        # after a sync completes to absorb our own write echoes.
        self._cooldown_s = max(self.debounce_s + 1.0, 5.0)
        self._cooldown_until = 0.0     # time.time() after which events are accepted

        self._timer: threading.Timer = None
        self._timer_lock = threading.Lock()
        self._syncing = False
        self._sync_requested = threading.Event()  # signalled by timer thread
        self._stop = threading.Event()
        self._observers: list = []
        self._cycle_count = 0
        self._last_bear_sig = _state_bear_signature(self.state)

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> None:
        """Block until SIGTERM / SIGINT.  Called from main().

        The main thread runs the actual sync cycles so that NSWorkspace
        (Layer 3 frontmost-app detection) works correctly — it requires
        the main thread's Objective-C autorelease pool / run loop
        context.  Timer threads and watchdog threads only *signal* that
        a sync is needed; they never run sync logic themselves.
        """
        log.info("══ Daemon starting ══  debounce=%.1fs  write_quiet=%.0fs  "
                 "min_interval=%.0fs  cooldown=%.1fs  watchdog=%s",
                 self.debounce_s, self.write_quiet_s,
                 self.min_interval_s, self._cooldown_s,
                 "yes" if _HAS_WATCHDOG else "NO (polling)")

        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT,  self._on_signal)

        if _HAS_WATCHDOG:
            self._start_observers()
            # Run one immediate cycle at startup to catch anything that
            # changed while the daemon was down.
            self._schedule(0.5)

            # Main loop — waits for the timer thread to signal, then
            # runs the sync cycle HERE on the main thread.
            while not self._stop.is_set():
                # Wait for either a sync request or stop signal.
                # Timeout ensures we check _stop periodically.
                self._sync_requested.wait(timeout=60.0)
                if self._stop.is_set():
                    break
                if not self._sync_requested.is_set():
                    continue   # timeout, just loop
                self._sync_requested.clear()
                try:
                    self._syncing = True
                    self._run_sync_cycle()
                except Exception:
                    log.exception("Sync cycle failed unexpectedly")
                finally:
                    self._syncing = False
        else:
            log.warning("watchdog not installed — falling back to %.0fs "
                        "polling.  Install: pip install watchdog",
                        float(self.cfg.get("sync_interval_seconds", 30)))
            self._poll_loop()

        self._cleanup()
        log.info("══ Daemon stopped ══  (%d cycles completed)",
                 self._cycle_count)

    def schedule_sync(self, source_tag: str) -> None:
        """Called by event handlers; resets the debounce timer.

        Events are silently dropped if:
          - a sync cycle is currently running (_syncing), or
          - we're inside the post-sync cooldown window (absorbing
            FSEvents generated by our own writes).
        """
        if self._syncing:
            return
        if time.time() < self._cooldown_until:
            return
        if source_tag == "bear_db" and self._should_skip_bear_event():
            return
        self._schedule(self.debounce_s)
        log.debug("Debounce reset → %.1fs  (trigger: %s)",
                  self.debounce_s, source_tag)

    def _should_skip_bear_event(self) -> bool:
        """True when Bear DB file activity did not change note content.

        This suppresses periodic SQLite/WAL housekeeping writes that emit
        FSEvents but do not modify exported note content.
        """
        sig = _bear_db_signature()
        if sig[0] <= 0 and sig[1] < 0:
            # If DB read fails, fail open and allow a sync attempt.
            return False
        if sig == self._last_bear_sig:
            return True
        self._last_bear_sig = sig
        return False

    # ── internal scheduling ─────────────────────────────────────────────

    def _schedule(self, delay: float) -> None:
        """Schedule a sync cycle after *delay* seconds, cancelling any
        pending timer."""
        with self._timer_lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(delay, self._on_timer)
            self._timer.daemon = True
            self._timer.start()

    def _schedule_retry(self, reason: str) -> None:
        """Schedule a retry after the editing guard blocked."""
        log.debug("Guard blocked (%s) — retry in %.0fs", reason,
                  self.retry_s)
        self._schedule(self.retry_s)

    def _on_timer(self) -> None:
        """Timer callback — signals the main thread that debounce expired."""
        if not self._stop.is_set():
            self._sync_requested.set()

    # ── core sync cycle ─────────────────────────────────────────────────

    def _run_sync_cycle(self) -> None:
        """Guards → change detection → sync → save state.

        Protections (matching run-once mode):

        1. Minimum interval: don't sync if the last sync completed less
           than min_interval_s ago.

        2. Three editing guard checks (first, recheck after DB settle,
           final before sync) — each uses write_quiet_s for Layer 2
           (write-settle), preserving the full protection from run-once
           mode.  For Bear-DB triggers this adds no latency because
           Layer 2 checks *folder* mtimes (unchanged) not DB mtime.

        3. DB settle wait.

        4. Post-sync cooldown: after sync completes, _cooldown_until is
           set to absorb FSEvent echoes from our own writes.
        """
        self._cycle_count += 1

        # ── Guard 1: minimum interval ───────────────────────────────
        elapsed = time.time() - self.state.get("last_sync_end", 0)
        if elapsed < self.min_interval_s:
            remaining = self.min_interval_s - elapsed
            log.debug("Interval: %.0fs remaining — retry in %.0fs",
                      remaining, remaining + 1)
            self._schedule(remaining + 1)
            return

        # Build snapshots once for the entire cycle
        snapshots = _build_snapshots(self.folders)

        # ── Guard 2: editing guard — first check ────────────────────
        last_sync_end = self.state.get("last_sync_end", 0)
        reason = check_editing_guard(
            self.folders, self.write_quiet_s, last_sync_end,
            log_all=True, snapshots=snapshots)
        if reason:
            self._schedule_retry(reason)
            return

        # ── Guard 3: DB settle ──────────────────────────────────────
        waited = 0.0
        max_wait = self.db_settle_s * 3
        while not _db_is_quiet(self.db_settle_s) and waited < max_wait:
            if self._stop.is_set():
                return
            time.sleep(0.5)
            waited += 0.5
        if waited:
            log.debug("DB settled after %.1fs", waited)

        # ── Guard 2 recheck after DB wait ───────────────────────────
        for snap in snapshots.values():
            snap.refresh_mtimes()
        reason = check_editing_guard(
            self.folders, self.write_quiet_s, last_sync_end,
            log_all=True, snapshots=snapshots)
        if reason:
            self._schedule_retry(reason)
            return

        # ── Guard 4: change detection ───────────────────────────────
        detector = ChangeDetector(self.state, self.cfg, snapshots=snapshots)
        bear_changed = detector.bear_changed()
        md_changed, tb_changed = detector.files_changed()
        files_changed = md_changed or tb_changed

        if not bear_changed and not files_changed:
            log.debug("No real changes.")
            self.state["last_sync"] = time.time()
            _save_state(self.state)
            return

        log.info("Changes: bear=%s  md=%s  tb=%s",
                 bear_changed, md_changed, tb_changed)

        # ── Guard 2 final check before sync ─────────────────────────
        for snap in snapshots.values():
            snap.refresh_mtimes()
        reason = check_editing_guard(
            self.folders, self.write_quiet_s, last_sync_end,
            log_all=True, snapshots=snapshots)
        if reason:
            self._schedule_retry(reason)
            return

        # Pre-sync hashes for post-export verification
        old_bear_mod = detector.hash_state.get("bear_max_mod", 0.0)
        old_bear_count = detector.hash_state.get("bear_note_count", -1)
        old_md = {k: tuple(v) if isinstance(v, list) else v
                  for k, v in detector.hash_state.get("md_hashes", {}).items()}
        old_tb = {k: tuple(v) if isinstance(v, list) else v
                  for k, v in detector.hash_state.get("tb_hashes", {}).items()}

        # ── SYNC ────────────────────────────────────────────────────
        post_snaps = run_sync(
            self.cfg, export_only=self.export_only,
            files_changed=files_changed, pre_snapshots=snapshots)

        now = time.time()
        self.state["last_sync"] = now
        self.state["last_sync_end"] = now
        self.state.pop("last_editor_left", None)
        detector.snapshot(self.state, post_snapshots=post_snaps)
        self._last_bear_sig = _state_bear_signature(self.state)

        # Post-export verification
        if bear_changed and not files_changed:
            h = self.state.get("hashes", {})
            if h.get("md_hashes") == old_md and h.get("tb_hashes") == old_tb:
                retry = self.state.get("bear_export_retry", 0) + 1
                if retry <= 5:
                    log.warning("Bear changed but export produced no file "
                                "changes \u2014 will retry (%d/5)", retry)
                    h["bear_max_mod"] = old_bear_mod
                    h["bear_note_count"] = old_bear_count
                    self.state["bear_export_retry"] = retry
                else:
                    log.warning("Bear changed but export produced no file "
                                "changes after 5 retries \u2014 accepting")
                    self.state.pop("bear_export_retry", None)
            else:
                self.state.pop("bear_export_retry", None)
        else:
            self.state.pop("bear_export_retry", None)

        _save_state(self.state)

        # ── Post-sync cooldown ──────────────────────────────────────
        # Set cooldown window so the event handler ignores FSEvents
        # generated by our own export writes.
        self._cooldown_until = time.time() + self._cooldown_s
        log.debug("Post-sync cooldown: %.1fs", self._cooldown_s)

    # ── observers ───────────────────────────────────────────────────────

    def _start_observers(self) -> None:
        """Create and start watchdog Observers for all watched paths."""
        watch_specs = [
            (self.bear_db_dir, "bear_db",   False),   # non-recursive
            (self.folder_md,   "folder_md", True),     # recursive (tag dirs)
            (self.folder_tb,   "folder_tb", True),
        ]
        for path, tag, recursive in watch_specs:
            if not os.path.isdir(path):
                log.warning("Watch path does not exist (skipped): %s", path)
                continue
            handler = _SyncEventHandler(self, tag)
            obs = _WatchdogObserver()
            obs.schedule(handler, path, recursive=recursive)
            obs.daemon = True
            obs.start()
            self._observers.append(obs)
            log.info("  watching: %s  (%s, %s)",
                     path, tag,
                     "recursive" if recursive else "top-level only")

    # ── polling fallback ────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        """Simple polling loop when watchdog is not available."""
        interval = max(10, int(self.cfg.get("sync_interval_seconds", 30)))
        while not self._stop.is_set():
            try:
                self._syncing = True
                self._run_sync_cycle()
            except Exception:
                log.exception("Sync cycle failed")
            finally:
                self._syncing = False
            self._stop.wait(interval)

    # ── lifecycle ───────────────────────────────────────────────────────

    def _on_signal(self, signum, _frame) -> None:
        signame = signal.Signals(signum).name
        log.info("Received %s — shutting down.", signame)
        self._stop.set()
        self._sync_requested.set()   # wake main thread from wait()
        with self._timer_lock:
            if self._timer is not None:
                self._timer.cancel()

    def _cleanup(self) -> None:
        for obs in self._observers:
            obs.stop()
        for obs in self._observers:
            obs.join(timeout=5)
        _save_state(self.state)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    ap = argparse.ArgumentParser(description="Bear sync gate v4.3")
    ap.add_argument("--force", action="store_true",
                    help="Bypass all guards and sync immediately")
    ap.add_argument("--export-only", action="store_true",
                    help="Skip import phase")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would happen without syncing")
    ap.add_argument("--guard-test", action="store_true",
                    help="Test all three editing guard layers and show results")
    ap.add_argument("--daemon", action="store_true",
                    help="Run as a resident daemon with FSEvents-driven sync "
                         "(requires: pip install watchdog)")
    args = ap.parse_args()

    cfg = load_config()

    # Guard 0: Lock
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        if args.daemon:
            log.error("Another instance is running (lock held) — exiting.")
        else:
            log.debug("Locked — exiting.")
        return 0

    try:
        # ── Daemon mode ─────────────────────────────────────────────────
        if args.daemon:
            daemon = SyncDaemon(cfg, export_only=args.export_only)
            daemon.run()   # blocks until SIGTERM / SIGINT
            return 0

        # ── Run-once mode (original behavior) ───────────────────────────
        state     = _load_state()
        folder_md = _resolve(cfg["folder_md"])
        folder_tb = _resolve(cfg["folder_tb"])
        folders   = [folder_md, folder_tb]

        if args.guard_test:
            quiet_s = max(10, float(cfg.get("write_quiet_seconds", 30)))
            last_sync_end = state.get("last_sync_end", 0)
            log.info("=== Guard test (v4.3) ===")
            log.info("  PyObjC (AppKit): %s", "available" if _HAS_APPKIT else "NOT available (using subprocess fallback)")
            log.info("  write_quiet_seconds = %.0f", quiet_s)
            log.info("  last_sync_end = %s",
                     time.strftime("%H:%M:%S", time.localtime(last_sync_end))
                     if last_sync_end else "(never)")
            reason = check_editing_guard(folders, quiet_s, last_sync_end,
                                         verbose=True)
            if reason:
                log.info("  RESULT: sync BLOCKED by %s", reason)
            else:
                log.info("  RESULT: all layers clear — sync would proceed")
            return 0

        if args.force:
            log.info("--force: bypassing all guards")
            snapshots = _build_snapshots(folders)
            detector = ChangeDetector(state, cfg, snapshots=snapshots)
            post_snaps = run_sync(cfg, export_only=args.export_only,
                                  files_changed=True, pre_snapshots=snapshots)
            state["last_sync"] = time.time()
            state["last_sync_end"] = time.time()
            detector.snapshot(state, post_snapshots=post_snaps)
            _save_state(state)
            return 0

        interval  = max(10, int(cfg.get("sync_interval_seconds", 30)))
        quiet_s   = max(10, float(cfg.get("write_quiet_seconds", 30)))
        db_quiet  = min(quiet_s, 5)

        # Guard 1: Minimum interval
        elapsed = time.time() - state.get("last_sync", 0)
        if elapsed < interval:
            log.debug("Interval: %.0fs remaining — exiting.", interval - elapsed)
            return 0

        # Build VaultSnapshots once — used by all subsequent guards,
        # change detection, junk cleaning, and pre-sync hashing.
        # This single walk per folder replaces 6-8 redundant walks.
        snapshots = _build_snapshots(folders)

        # Guard 2: Editing guard (first check)
        last_sync_end = state.get("last_sync_end", 0)
        reason = check_editing_guard(folders, quiet_s, last_sync_end,
                                      snapshots=snapshots)
        if reason:
            log.debug("Editing guard: %s — exiting.", reason)
            return 0

        # Guard 3: DB settle
        waited = 0.0
        max_db_wait = db_quiet * 3
        while not _db_is_quiet(db_quiet) and waited < max_db_wait:
            time.sleep(1)
            waited += 1
        if waited:
            log.debug("DB settled after %.0fs", waited)

        # Guard 2 RECHECK after DB wait — refresh mtimes (re-stat,
        # no walk) to detect any writes that happened during the wait.
        for snap in snapshots.values():
            snap.refresh_mtimes()
        reason = check_editing_guard(folders, quiet_s, last_sync_end,
                                      snapshots=snapshots)
        if reason:
            log.debug("Editing guard (recheck): %s — exiting.", reason)
            return 0

        # Guard 4: Change detection (uses snapshot hashes, no walk)
        detector = ChangeDetector(state, cfg, snapshots=snapshots)
        bear_changed = detector.bear_changed()
        md_changed, tb_changed = detector.files_changed()
        files_changed = md_changed or tb_changed

        if not bear_changed and not files_changed:
            log.debug("No real changes — exiting.")
            state["last_sync"] = time.time()
            _save_state(state)
            return 0

        if args.dry_run:
            log.info("[dry-run] bear=%s  md=%s  tb=%s",
                     bear_changed, md_changed, tb_changed)
            return 0

        log.info("Changes: bear=%s  md=%s  tb=%s",
                 bear_changed, md_changed, tb_changed)

        # Guard 2 FINAL CHECK before sync — refresh mtimes again
        for snap in snapshots.values():
            snap.refresh_mtimes()
        reason = check_editing_guard(folders, quiet_s, last_sync_end,
                                      snapshots=snapshots)
        if reason:
            log.debug("Editing guard (final): %s — exiting.", reason)
            return 0

        # Save pre-sync file hashes for post-export verification
        old_bear_mod = detector.hash_state.get("bear_max_mod", 0.0)
        old_bear_count = detector.hash_state.get("bear_note_count", -1)
        old_md = {k: tuple(v) if isinstance(v, list) else v
                  for k, v in detector.hash_state.get("md_hashes", {}).items()}
        old_tb = {k: tuple(v) if isinstance(v, list) else v
                  for k, v in detector.hash_state.get("tb_hashes", {}).items()}

        # SYNC — pass pre-snapshots in, get post-snapshots back
        post_snaps = run_sync(cfg, export_only=args.export_only,
                              files_changed=files_changed,
                              pre_snapshots=snapshots)

        state["last_sync"] = time.time()
        state["last_sync_end"] = time.time()
        state.pop("last_editor_left", None)
        detector.snapshot(state, post_snapshots=post_snaps)

        # Post-export verification: if Bear changed but export produced
        # no file changes, preserve old bear_max_mod so next cycle retries.
        # This catches the case where the export script skips notes whose
        # modification dates (carried from another device via CloudKit)
        # are older than its own "last exported" watermark.
        if bear_changed and not files_changed:
            h = state.get("hashes", {})
            if h.get("md_hashes") == old_md and h.get("tb_hashes") == old_tb:
                retry = state.get("bear_export_retry", 0) + 1
                if retry <= 5:
                    log.warning("Bear changed but export produced no file "
                                "changes \u2014 will retry (%d/5)", retry)
                    h["bear_max_mod"] = old_bear_mod
                    h["bear_note_count"] = old_bear_count
                    state["bear_export_retry"] = retry
                else:
                    log.warning("Bear changed but export produced no file "
                                "changes after 5 retries \u2014 accepting state")
                    state.pop("bear_export_retry", None)
            else:
                state.pop("bear_export_retry", None)
        else:
            state.pop("bear_export_retry", None)

        _save_state(state)
        return 0

    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


if __name__ == "__main__":
    sys.exit(main())

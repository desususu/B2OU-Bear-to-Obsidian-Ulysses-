# Bear Markdown Export & Sync

> A deep refactor of [andymatuschak's fork](https://github.com/andymatuschak/Bear-Markdown-Export) of the original [rovest/Bear-Markdown-Export](https://github.com/rovest/Bear-Markdown-Export).
> Updated for Bear 2.0 · Overhauled performance · Real-time sync daemon · Beginner-friendly launcher.

**中文文档请见 [README_zh.md](README_zh.md)**

---

## ⚠️ Back Up First

Before running these scripts for the first time, back up your Bear notes:
**Bear → File → Backup Notes…**

Also back up your Mac with Time Machine or equivalent. Both `rsync` and `shutil.rmtree` are used internally — an incorrectly configured path can overwrite or delete files.

---

## What's New vs the Original

The original `bear_export_sync.py` (rovest → andymatuschak) is a solid foundation. This refactor addresses every known performance bottleneck and adds a production-grade sync daemon that did not previously exist.

### bear_export_sync.py — Performance Overhaul

| Area | Original | This Refactor |
|---|---|---|
| **Regex** | Compiled inline on every note call | 29 patterns compiled once at module load |
| **File creation dates** | `SetFile -d` subprocess (~50 ms per file) | `NSFileManager` native API (< 1 ms per file) |
| **Export pipeline** | Write all notes to `~/Temp/BearExportTemp` → rsync to destination | Direct in-place write — no temp folder, no rsync |
| **Image sync** | `copy_bear_images()` runs an `rsync -r` over the entire Bear image store every cycle | Incremental copy inside the export loop — only images referenced by changed notes |
| **Stale file cleanup** | Not implemented | `_cleanup_stale_notes()` uses the expected-path set built during export — no extra walk |
| **Orphan image cleanup** | Not implemented | `_cleanup_root_orphan_images()` cross-references every image ref in every note |
| **Image syntax support** | Bear `[image:…]` only | Also handles HTML `<img src=…>`, `![[wikilink]]`, and reference-style links |
| **Run modes** | Export + import only | `--skipExport`, `--skipImport`, `--excludeTag`, `--hideTags`, `--format md/tb` |
| **Code size** | 763 lines, 38 functions | 1 430 lines, 60 functions |

**Concrete impact (vault of ~500 notes, 200 images):**

| Operation | Original | This Refactor |
|---|---|---|
| Creation-date stamp for 20 new files | ~1 000 ms (20 × 50 ms subprocess) | < 20 ms (native API) |
| Image sync pass | Full rsync of entire image store every cycle | Zero cost if no changed notes reference new images |
| Stale note removal | Manual or not done | Automatic, zero extra I/O — piggybacks on the export pass |

### DualSync/sync_gate.py — New Real-Time Daemon

The original project had no real-time daemon. The old workaround was a cron/launchd timer polling every 5–15 minutes, with no editing protection. `sync_gate.py` is a purpose-built sync gate written from scratch:

- **Three-layer editing guard** — never interrupts active editing
  - **Layer 1 — file-open check:** `lsof +D` on note directories detects open file handles
  - **Layer 2 — write-settle:** no note may have been modified within the last N seconds (`write_quiet_seconds`)
  - **Layer 3 — frontmost app:** `NSWorkspace.frontmostApplication()` (< 1 ms, no subprocess) detects Bear, Obsidian, Typora, or Ulysses in the foreground
- **FSEvents daemon mode** (`--daemon`) — `watchdog` watches Bear's SQLite WAL and the export folders; syncs within ~3–5 s of a Bear save
- **Two-stage debounce + retry** — rapid write bursts are batched by a debounce timer; if the guard blocks after debounce, retries fire at `daemon_retry_seconds` until the coast is clear
- **Self-event suppression** — a post-sync cooldown window silently drops the daemon's own write echoes
- **VaultSnapshot** — one `os.walk` per folder per cycle feeds change detection, cloud-junk removal, and content hashing simultaneously (replaces three independent walks)
- **Content hashing** — `xxhash` (xxh3_128) with SHA-256 fallback; a size pre-filter short-circuits unchanged files before any hash is computed
- **Lock file** — prevents concurrent instances
- **Cloud junk filter** — auto-removes `.DS_Store`, Synology `@eaDir`, Dropbox temp files, empty note files, and other cloud-sync debris
- **launchd-compatible run-once mode** (default) — check guards → sync → exit; minimal memory footprint, 30–75 s latency depending on timer alignment
- **Signal handling** — `SIGTERM` / `SIGINT` trigger clean shutdown (stop observers, save state, release lock)

### run.sh — New Interactive Launcher

The original project included a single hardcoded shell script with the developer's personal paths baked in. `run.sh` is a full bilingual interactive launcher:

- Language selection at startup (English / 中文)
- Dependency checker: Python version, venv, `pyobjc-framework-Cocoa`, `watchdog`, `xxhash`
- One-click venv creation and package installation via pip
- Guided configuration wizard — writes `sync_config.json` through Python for guaranteed valid JSON even with spaces or special characters in paths
- Accepts drag-and-dropped paths from Finder (auto-strips quotes and surrounding whitespace)
- Covers all run modes of both scripts
- Log viewer (tail + open in editor)
- No hardcoded paths anywhere

---

## Architecture

```
Bear-Markdown-Export/
├── bear_export_sync.py       # Core export/import engine
├── run.sh                    # Interactive bilingual launcher
├── DualSync/
│   ├── sync_gate.py          # Smart sync daemon
│   └── sync_config.json      # Your local config (not committed to git)
├── LICENSE
├── README.md                 # English documentation (this file)
└── README_zh.md              # Chinese documentation
```

Data flow (hub-and-spoke — Bear is always the source of truth):

```
External editor edits an MD/TB file
  └→ sync_gate detects change (FSEvents or poll)
     └→ bear_export_sync imports to Bear (--skipExport)
        └→ Bear DB modified → FSEvents fires
           └→ bear_export_sync exports to ALL configured folders
```

---

## Requirements

- **macOS 12 Monterey or later** (uses `AppKit`, `NSWorkspace`, `NSFileManager`, FSEvents)
- **Bear 2.0** installed and signed in
- **Python 3.9+** (3.6+ minimum)

### Python packages

| Package | Used by | Required? |
|---|---|---|
| `pyobjc-framework-Cocoa` | Both scripts — `AppKit`, `NSWorkspace`, `NSFileManager` | **Required** |
| `watchdog` | `sync_gate.py` — FSEvents daemon mode | Strongly recommended |
| `xxhash` | `sync_gate.py` — fast content hashing | Optional (falls back to SHA-256) |

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/desususu/Bear-Markdown-Export.git
cd Bear-Markdown-Export

# 2. Launch the interactive setup
bash run.sh
```

From the menu:
1. **Option 5** — check and install dependencies
2. **Option 3** — configure your export paths (choose "Guided setup")
3. **Option 1** for a one-time sync, or **Option 2 → Daemon mode** to start real-time syncing

---

## bear_export_sync.py

The core engine. Can be run standalone or driven by `sync_gate.py`.

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--out PATH` | `~/Work/BearNotes` | Destination for exported notes |
| `--backup PATH` | `~/Work/BearSyncBackup` | Conflict backup folder (must be outside `--out`) |
| `--format md\|tb` | `md` | Output format: plain Markdown or Textbundle |
| `--images PATH` | `<out>/BearImages` | Custom image repository path |
| `--skipImport` | off | Export only — skip the import phase |
| `--skipExport` | off | Import only — skip the export phase |
| `--excludeTag TAG` | — | Exclude notes tagged with TAG (repeatable) |
| `--hideTags` | off | Wrap tags in HTML comments on export |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | No changes — nothing to export |
| `1` | Notes exported successfully |

### Examples

```bash
source venv/bin/activate

# Full sync — Markdown
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup

# Textbundle format
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --format tb

# Export only
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --skipImport

# Import only
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --skipExport

# Exclude a tag
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --excludeTag private

# Custom image folder
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --images ~/Notes/Images
```

---

## DualSync/sync_gate.py

The smart sync daemon. Reads configuration from `DualSync/sync_config.json`.

### Run modes

| Command | Description |
|---|---|
| `python3 DualSync/sync_gate.py` | **Run-once** — check guards, sync if safe, exit. Designed for launchd. |
| `python3 DualSync/sync_gate.py --daemon` | **Daemon** — stay resident, watch FSEvents, sync within seconds of changes |
| `python3 DualSync/sync_gate.py --force` | Bypass all editing guards and sync immediately |
| `python3 DualSync/sync_gate.py --export-only` | Skip the import phase |
| `python3 DualSync/sync_gate.py --dry-run` | Show what would happen without writing anything |
| `python3 DualSync/sync_gate.py --guard-test` | Diagnose all three guard layers and exit |

### Sync latency by change source

| Change source | Mode | Latency |
|---|---|---|
| Bear saves a note | Daemon | ~3–5 s (debounce → Layer 2 passes → sync) |
| External editor saves a file | Daemon | ~30–35 s (debounce → Layer 2 settle → retry → sync) |
| Any change | Run-once (launchd) | 0 – `sync_interval_seconds` jitter |

### sync_config.json reference

```json
{
    "script_path":              "./bear_export_sync.py",
    "python_path":              "./venv/bin/python3",
    "folder_md":                "/your/path/to/MD_Export",
    "folder_tb":                "/your/path/to/TB_Export",
    "backup_md":                "/your/path/to/MD_Backup",
    "backup_tb":                "/your/path/to/TB_Backup",
    "sync_interval_seconds":    30,
    "write_quiet_seconds":      30,
    "editor_cooldown_seconds":  5,
    "bear_settle_seconds":      3,
    "conflict_backup_dir":      "",
    "daemon_debounce_seconds":  3.0,
    "daemon_retry_seconds":     5.0
}
```

| Key | Description |
|---|---|
| `script_path` | Path to `bear_export_sync.py` (relative to `DualSync/` or absolute) |
| `python_path` | Python interpreter (leave `""` to auto-detect) |
| `folder_md` | Markdown export destination |
| `folder_tb` | Textbundle export destination |
| `backup_md` / `backup_tb` | Conflict backup folders — must be outside export folders |
| `sync_interval_seconds` | Polling interval for run-once / daemon fallback (minimum 30) |
| `write_quiet_seconds` | Settle window before sync is allowed — Layer 2 guard |
| `editor_cooldown_seconds` | Seconds after editor goes to background before Layer 3 clears |
| `bear_settle_seconds` | Seconds to wait after Bear DB changes before syncing |
| `conflict_backup_dir` | Extra directory for conflict copies (optional) |
| `daemon_debounce_seconds` | FSEvents debounce window in daemon mode |
| `daemon_retry_seconds` | Retry interval when the editing guard blocks in daemon mode |

---

## run.sh

```bash
bash run.sh
```

### Menu

```
Language selection  (English / 中文)
│
├── 1  Quick sync      — run bear_export_sync.py once with interactive prompts
├── 2  DualSync menu   — run-once / daemon / force / dry-run / export-only / guard-test
├── 3  Configure paths — guided wizard or open sync_config.json in text editor
├── 4  View logs       — tail sync_gate.log or open in editor
├── 5  Dependencies    — check Python / venv / packages; install missing
└── q  Quit
```

---

## How Sync Works

### Export (Bear → disk)

1. Check `database.sqlite` mtime — exit immediately if unchanged
2. Query all notes from Bear's SQLite database
3. For each changed note: write `.md` or `.textbundle` directly to the export folder
4. Copy only images referenced by changed notes (incremental — no full rsync pass)
5. Strip Bear-specific syntax; append `BearID` footer for round-trip matching
6. `_cleanup_stale_notes()` — remove files for notes deleted in Bear (uses the expected-path set, no extra walk)
7. `_cleanup_root_orphan_images()` — remove images no longer referenced by any note

### Import (disk → Bear)

1. Scan export folder for `.md` / `.textbundle` files modified since last sync
2. Match each file to its Bear note via the embedded `BearID`
3. Update via `bear://x-callback-url/add-text?mode=replace` (preserves creation date and note ID)
4. Conflict: keep both versions in Bear with a conflict notice
5. Files without a `BearID` are created as new Bear notes

---

## Notes & Caveats

**Obsidian users** — Every note must start with a `# Heading` on line 1. Obsidian uses this line to derive the filename; without it, file links break across the vault.

**sync_config.json is git-ignored** — It contains your local paths. Never commit it.

**Large vaults** — First export may take a minute or two. Subsequent syncs process only changed notes.

**watchdog not installed** — `sync_gate.py` falls back to polling. Daemon mode still works but responds on a polling interval rather than via FSEvents.

**launchd setup** — Run-once mode is designed for launchd. A sample plist is not included because paths are machine-specific. Point launchd at `DualSync/sync_gate.py` with your venv's Python and a `StartInterval` of 30–60 seconds.

---

## Credits

- Original author: [rovest](https://github.com/rovest) ([@rorves](https://twitter.com/rorves))
- Modified by: [andymatuschak](https://github.com/andymatuschak) ([@andy_matuschak](https://twitter.com/andy_matuschak))
- Further refactored and maintained by: [desususu](https://github.com/desususu)

---

## License

MIT — see [LICENSE](LICENSE).

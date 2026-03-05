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

### b2ou cli — Performance & Reliability

| Area | Status |
|---|---|
| **Architecture** | Modern modular Python package (`b2ou/`) |
| **Logic** | Decoupled core engine + FSEvents daemon |
| **Type Safety** | Fully typed (PEP 484/585/604) |
| **Regex** | Pre-compiled static patterns |
| **File creation dates** | Native `AppKit` / `NSFileManager` APIs |
| **Editing Guard** | 3-layer protection (lsof, mtime, active app) |

### Architecture

```
B2OU-Bear-to-Obsidian-Ulysses/
├── b2ou/                  # Package source
│   ├── cli.py             # Entry points
│   ├── daemon.py          # Sync logic & daemon
│   ├── export.py          # Bear → Disk
│   ├── import_.py         # Disk → Bear
│   ├── guard.py           # Editing protection
│   └── ...
├── pyproject.toml         # Build & dependency metadata
└── b2ou_config.json       # Local sync configuration
```

---

## Quick Start (macOS only)

```bash
# 1. Clone
git clone https://github.com/desususu/B2OU-Bear-to-Obsidian-Ulysses-.git
cd B2OU-Bear-to-Obsidian-Ulysses-

# 2. Setup environment
python3 -m venv venv
source venv/bin/activate
pip install -e ".[all]"

# 3. Initialize config
# This will create a default b2ou_config.json in the current folder
b2ou gate
```

After editing `b2ou_config.json` with your paths:

```bash
# One-time sync
b2ou sync

# Run as background daemon
b2ou daemon
```

---

## CLI Reference

| Command | Description |
|---|---|
| `b2ou export` | Export Bear notes to disk (one-way) |
| `b2ou import` | Import changed notes from disk back into Bear |
| `b2ou sync` | Run a full import + export cycle |
| `b2ou gate` | Run-once smart sync (safe for cron/launchd) |
| `b2ou daemon` | FSEvents-driven daemon mode (real-time) |
| `b2ou guard-test` | Diagnose editing-guard layers |

### Arguments

Most commands accept:
- `--out PATH`: Destination for exported notes
- `--backup PATH`: Conflict backup folder
- `--format md|tb`: Output format (Markdown or TextBundle)
- `--exclude-tag TAG`: Skip notes with specific tags

For `gate` and `daemon`:
- `--config FILE`: Path to your config JSON (default: `b2ou_config.json`)
- `--force`: Bypass guards (gate only)
- `--export-only`: Skip the import phase

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

"""
Configuration dataclasses for b2ou.

``ExportConfig``   — core engine (export + import phases).
``SyncGateConfig`` — daemon orchestrator.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Default Bear paths (macOS only)
# ---------------------------------------------------------------------------

_HOME = Path.home()

DEFAULT_BEAR_DB = _HOME / (
    "Library/Group Containers/9K33E3U3T4.net.shinyfrog.bear"
    "/Application Data/database.sqlite"
)

DEFAULT_BEAR_IMAGE_PATH = _HOME / (
    "Library/Group Containers/9K33E3U3T4.net.shinyfrog.bear"
    "/Application Data/Local Files/Note Images"
)


# ---------------------------------------------------------------------------
# Core-engine config
# ---------------------------------------------------------------------------

@dataclass
class ExportConfig:
    """All settings for the export + import engine (bear_export_sync)."""

    # ── Required paths ───────────────────────────────────────────────────
    export_path: Path
    backup_path: Path

    # ── Optional / defaulted paths ───────────────────────────────────────
    bear_db: Path = field(default_factory=lambda: DEFAULT_BEAR_DB)
    bear_image_path: Path = field(default_factory=lambda: DEFAULT_BEAR_IMAGE_PATH)
    assets_path: Optional[Path] = None  # defaults to export_path/BearImages

    # ── Export format ────────────────────────────────────────────────────
    # 'md'  → plain Markdown + separate BearImages folder
    # 'tb'  → TextBundle (.textbundle) with embedded assets
    export_format: str = "md"

    # ── Tag / folder options ──────────────────────────────────────────────
    make_tag_folders: bool = False
    multi_tag_folders: bool = True
    hide_tags: bool = False
    only_export_tags: list[str] = field(default_factory=list)
    exclude_tags: list[str] = field(default_factory=list)

    # ── Phase flags ───────────────────────────────────────────────────────
    skip_import: bool = False
    skip_export: bool = False

    # ── Misc ─────────────────────────────────────────────────────────────
    logging_enabled: bool = True

    def __post_init__(self) -> None:
        self.export_path = Path(self.export_path)
        self.backup_path = Path(self.backup_path)
        self.bear_db = Path(self.bear_db)
        self.bear_image_path = Path(self.bear_image_path)

        if self.assets_path is None:
            self.assets_path = self.export_path / "BearImages"
        elif self.assets_path:
            self.assets_path = Path(str(self.assets_path))

    # ── Derived flags (read-only) ─────────────────────────────────────────

    @property
    def export_as_textbundles(self) -> bool:
        return self.export_format == "tb"

    @property
    def export_as_hybrids(self) -> bool:
        """TextBundle only when note actually contains images."""
        return self.export_format == "tb"

    @property
    def export_image_repository(self) -> bool:
        """Copy Bear images to a shared BearImages folder."""
        return self.export_format == "md"

    # ── Helpers ──────────────────────────────────────────────────────────

    @property
    def log_file(self) -> Path:
        return self.backup_path / "bear_export_sync_log.txt"

    @property
    def sync_ts_file(self) -> Path:
        return self.export_path / ".sync-time.log"

    @property
    def export_ts_file(self) -> Path:
        return self.export_path / ".export-time.log"


# ---------------------------------------------------------------------------
# Sync-gate / daemon config
# ---------------------------------------------------------------------------

_SYNC_GATE_DEFAULTS: dict = {
    "script_path":             "b2ou",
    "python_path":             "",
    "folder_md":               "./Export/MD_Export",
    "folder_tb":               "./Export/TB_Export",
    "backup_md":               "./Backup/MD_Backup",
    "backup_tb":               "./Backup/TB_Backup",
    "sync_interval_seconds":   30,
    "write_quiet_seconds":     30,
    "editor_cooldown_seconds": 5,
    "bear_settle_seconds":     3,
    "conflict_backup_dir":     "",
    "daemon_debounce_seconds": 3.0,
    "daemon_retry_seconds":    5.0,
}


@dataclass
class SyncGateConfig:
    """Settings for the sync-gate daemon orchestrator."""

    # ── Script invocation ────────────────────────────────────────────────
    script_path: Path = Path("b2ou")
    python_path: str = ""          # empty → use sys.executable

    # ── Export folders ───────────────────────────────────────────────────
    folder_md: Path = Path("./Export/MD_Export")
    folder_tb: Path = Path("./Export/TB_Export")
    backup_md: Path = Path("./Backup/MD_Backup")
    backup_tb: Path = Path("./Backup/TB_Backup")

    # ── Timing ───────────────────────────────────────────────────────────
    sync_interval_seconds: int = 30
    write_quiet_seconds: int = 30
    editor_cooldown_seconds: int = 5
    bear_settle_seconds: int = 3
    daemon_debounce_seconds: float = 3.0
    daemon_retry_seconds: float = 5.0

    # ── Optional conflict backup ──────────────────────────────────────────
    conflict_backup_dir: str = ""

    def __post_init__(self) -> None:
        self.script_path = Path(self.script_path)
        self.folder_md = Path(self.folder_md)
        self.folder_tb = Path(self.folder_tb)
        self.backup_md = Path(self.backup_md)
        self.backup_tb = Path(self.backup_tb)

    @property
    def python(self) -> str:
        if self.python_path:
            p = Path(self.python_path)
            if p.is_file() and os.access(p, os.X_OK):
                return str(p)
        return sys.executable

    @property
    def resolved_conflict_dir(self) -> Optional[Path]:
        if self.conflict_backup_dir.strip():
            return Path(self.conflict_backup_dir)
        return None


# ---------------------------------------------------------------------------
# Config file I/O
# ---------------------------------------------------------------------------

def load_sync_gate_config(config_file: Path) -> SyncGateConfig:
    """Load ``SyncGateConfig`` from *config_file*, creating it with defaults if absent."""
    if not config_file.exists():
        raw = dict(_SYNC_GATE_DEFAULTS)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(raw, indent=4, ensure_ascii=False), encoding="utf-8"
        )
        raise FileNotFoundError(
            f"Created default config at {config_file} — review and restart."
        )

    with config_file.open(encoding="utf-8") as fh:
        raw = json.load(fh)

    for k, v in _SYNC_GATE_DEFAULTS.items():
        raw.setdefault(k, v)

    return SyncGateConfig(
        script_path=Path(raw["script_path"]),
        python_path=raw.get("python_path", ""),
        folder_md=Path(raw["folder_md"]),
        folder_tb=Path(raw["folder_tb"]),
        backup_md=Path(raw["backup_md"]),
        backup_tb=Path(raw["backup_tb"]),
        sync_interval_seconds=int(raw["sync_interval_seconds"]),
        write_quiet_seconds=int(raw["write_quiet_seconds"]),
        editor_cooldown_seconds=int(raw["editor_cooldown_seconds"]),
        bear_settle_seconds=int(raw["bear_settle_seconds"]),
        daemon_debounce_seconds=float(raw["daemon_debounce_seconds"]),
        daemon_retry_seconds=float(raw["daemon_retry_seconds"]),
        conflict_backup_dir=raw.get("conflict_backup_dir", ""),
    )

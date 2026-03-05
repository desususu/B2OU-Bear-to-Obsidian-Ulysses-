import json
from pathlib import Path
import pytest
from b2ou.config import load_sync_gate_config, SyncGateConfig

def test_load_sync_gate_config(tmp_path):
    config_file = tmp_path / "b2ou_config.json"
    
    # Test creation of default config
    with pytest.raises(FileNotFoundError):
        load_sync_gate_config(config_file)
    
    assert config_file.exists()
    data = json.loads(config_file.read_text())
    assert data["sync_interval_seconds"] == 30
    
    # Test loading existing config
    data["sync_interval_seconds"] = 60
    config_file.write_text(json.dumps(data))
    
    cfg = load_sync_gate_config(config_file)
    assert isinstance(cfg, SyncGateConfig)
    assert cfg.sync_interval_seconds == 60
    assert cfg.folder_md == Path("./Export/MD_Export")

def test_config_paths_resolved(tmp_path):
    config_file = tmp_path / "b2ou_config.json"
    data = {
        "folder_md": "/tmp/md",
        "folder_tb": "/tmp/tb",
        "backup_md": "/tmp/bak_md",
        "backup_tb": "/tmp/bak_tb"
    }
    config_file.write_text(json.dumps(data))
    
    cfg = load_sync_gate_config(config_file)
    assert cfg.folder_md == Path("/tmp/md")
    assert cfg.backup_md == Path("/tmp/bak_md")

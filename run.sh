#!/usr/bin/env bash
# =============================================================================
#  Bear Markdown Export & Sync — Interactive Launcher
#  run.sh
#
#  Supports: bear_export_sync.py (core engine)
#            DualSync/sync_gate.py (smart sync daemon)
# =============================================================================

set -euo pipefail

# ── 优雅退出机制 (trap Ctrl+C) ────────────────────────────────────────────────
trap_ctrlc() {
    if [[ "${RUNNING_CHILD:-0}" -eq 1 ]]; then
        echo -e "\n"
        return 0
    fi
    echo -e "\n\n  \033[1;33m⚠\033[0m  $(t goodbye)"
    exit 0
}
trap trap_ctrlc SIGINT SIGTERM

# ── Resolve script directory (works even if called via symlink) ───────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_SCRIPT="$SCRIPT_DIR/bear_export_sync.py"
DUALSYNC_DIR="$SCRIPT_DIR/DualSync"
GATE_SCRIPT="$DUALSYNC_DIR/sync_gate.py"
CONFIG_FILE="$DUALSYNC_DIR/sync_config.json"
VENV_DIR="$SCRIPT_DIR/venv"
GATE_LOG="$DUALSYNC_DIR/sync_gate.log"

# ── 全局变量缓存 ──────────────────────────────────────────────────────────────
GLOBAL_PYTHON=""
GLOBAL_PIP=""
RUNNING_CHILD=0

# ── Color helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

# ── Language (set after user picks) ──────────────────────────────────────────
LANG_CODE=""   # "en" | "zh"

# =============================================================================
#  TEXT STRINGS  (bilingual)
# =============================================================================

t() {
    local key="$1"
    case "$LANG_CODE:$key" in
    *:select_lang)   echo "Please select a language / 请选择语言:" ;;
    en:main_title)   echo "Bear Markdown Export & Sync" ;;
    zh:main_title)   echo "Bear Markdown 导出与同步" ;;
    en:menu_prompt)  echo "Choose an option:" ;;
    zh:menu_prompt)  echo "请选择操作：" ;;
    en:opt_1)        echo "1) Quick sync (bear_export_sync.py — one run)" ;;
    zh:opt_1)        echo "1) 快速同步（bear_export_sync.py — 单次运行）" ;;
    en:opt_2)        echo "2) DualSync menu (sync_gate.py)" ;;
    zh:opt_2)        echo "2) DualSync 菜单（sync_gate.py）" ;;
    en:opt_3)        echo "3) Configure paths (sync_config.json)" ;;
    zh:opt_3)        echo "3) 配置路径（sync_config.json）" ;;
    en:opt_4)        echo "4) View logs" ;;
    zh:opt_4)        echo "4) 查看日志" ;;
    en:opt_5)        echo "5) Check / install dependencies" ;;
    zh:opt_5)        echo "5) 检查 / 安装依赖" ;;
    en:opt_q)        echo "q) Quit" ;;
    zh:opt_q)        echo "q) 退出" ;;
    en:main_tip)     echo "First-time setup: 5) Dependencies -> 3) Configure paths -> 2) Daemon mode" ;;
    zh:main_tip)     echo "首次使用建议：5) 检查依赖 -> 3) 配置路径 -> 2) 守护进程模式" ;;
    en:qs_title)     echo "Quick Sync Options" ;;
    zh:qs_title)     echo "快速同步选项" ;;
    en:qs_out)       echo "Export folder path:" ;;
    zh:qs_out)       echo "导出目录路径：" ;;
    en:qs_backup)    echo "Backup folder path:" ;;
    zh:qs_backup)    echo "备份目录路径：" ;;
    en:qs_format)    echo "Format — md / tb (default: md):" ;;
    zh:qs_format)    echo "格式 — md / tb（默认：md）：" ;;
    en:qs_skip)      echo "Mode:" ;;
    zh:qs_skip)      echo "模式：" ;;
    en:qs_skip1)     echo "  1) Export + Import (default)" ;;
    zh:qs_skip1)     echo "  1) 导出 + 导入（默认）" ;;
    en:qs_skip2)     echo "  2) Export only (skip import)" ;;
    zh:qs_skip2)     echo "  2) 仅导出（跳过导入）" ;;
    en:qs_skip3)     echo "  3) Import only (skip export)" ;;
    zh:qs_skip3)     echo "  3) 仅导入（跳过导出）" ;;
    en:qs_tag)       echo "Exclude tag (leave blank to skip):" ;;
    zh:qs_tag)       echo "排除标签（留空则不排除）：" ;;
    en:qs_running)   echo "Running bear_export_sync.py ..." ;;
    zh:qs_running)   echo "正在运行 bear_export_sync.py ..." ;;
    en:ds_title)     echo "DualSync — sync_gate.py" ;;
    zh:ds_title)     echo "DualSync — sync_gate.py" ;;
    en:ds_1)         echo "1) Run once (safe check, then sync)" ;;
    zh:ds_1)         echo "1) 单次运行（检查安全后同步）" ;;
    en:ds_2)         echo "2) Daemon mode (real-time, stays running)" ;;
    zh:ds_2)         echo "2) 守护进程模式（实时监控，持续运行）" ;;
    en:ds_3)         echo "3) Force sync (bypass all guards)" ;;
    zh:ds_3)         echo "3) 强制同步（跳过所有检查）" ;;
    en:ds_4)         echo "4) Dry run (show what would happen)" ;;
    zh:ds_4)         echo "4) 预演（显示将发生的操作，不实际执行）" ;;
    en:ds_5)         echo "5) Export only (skip import phase)" ;;
    zh:ds_5)         echo "5) 仅导出（跳过导入阶段）" ;;
    en:ds_6)         echo "6) Guard test (diagnose all editing guards)" ;;
    zh:ds_6)         echo "6) 守卫测试（诊断所有编辑检查层）" ;;
    en:ds_b)         echo "b) Back" ;;
    zh:ds_b)         echo "b) 返回" ;;
    en:ds_no_config) echo "Config file not found. Please run option 3 to configure paths first." ;;
    zh:ds_no_config) echo "未找到配置文件，请先选择选项 3 配置路径。" ;;
    en:ds_cfg_missing) echo "Config is incomplete. Please run option 3 guided setup. Missing fields:" ;;
    zh:ds_cfg_missing) echo "配置不完整，请先执行选项 3 引导配置。缺失字段：" ;;
    en:cfg_title)    echo "Path Configuration (sync_config.json)" ;;
    zh:cfg_title)    echo "路径配置（sync_config.json）" ;;
    en:cfg_intro)    echo "Enter paths for each field. Press Enter to keep the current value." ;;
    zh:cfg_intro)    echo "请为每个字段输入路径，直接按 Enter 保留当前值。" ;;
    en:cfg_warn)     echo "WARNING: Back up your Bear notes before first run!" ;;
    zh:cfg_warn)     echo "警告：首次运行前请务必备份 Bear 笔记！" ;;
    en:cfg_script)   echo "Path to bear_export_sync.py:" ;;
    zh:cfg_script)   echo "bear_export_sync.py 路径：" ;;
    en:cfg_python)   echo "Python interpreter path (leave blank to auto-detect):" ;;
    zh:cfg_python)   echo "Python 解释器路径（留空自动检测）：" ;;
    en:cfg_md)       echo "Markdown export folder:" ;;
    zh:cfg_md)       echo "Markdown 导出目录：" ;;
    en:cfg_tb)       echo "Textbundle export folder:" ;;
    zh:cfg_tb)       echo "Textbundle 导出目录：" ;;
    en:cfg_bmd)      echo "Markdown backup folder (must be OUTSIDE export folder):" ;;
    zh:cfg_bmd)      echo "Markdown 备份目录（必须在导出目录之外）：" ;;
    en:cfg_btb)      echo "Textbundle backup folder (must be OUTSIDE export folder):" ;;
    zh:cfg_btb)      echo "Textbundle 备份目录（必须在导出目录之外）：" ;;
    en:cfg_interval) echo "Sync interval in seconds (min 30, default 30):" ;;
    zh:cfg_interval) echo "同步间隔秒数（最小 30，默认 30）：" ;;
    en:cfg_saved)    echo "Configuration saved." ;;
    zh:cfg_saved)    echo "配置已保存。" ;;
    en:cfg_open)     echo "Opening config file in your default editor..." ;;
    zh:cfg_open)     echo "正在用默认编辑器打开配置文件..." ;;
    en:cfg_howto)    echo "How would you like to configure?" ;;
    zh:cfg_howto)    echo "请选择配置方式：" ;;
    en:cfg_guided)   echo "1) Guided setup (step-by-step prompts)" ;;
    zh:cfg_guided)   echo "1) 引导配置（逐步输入）" ;;
    en:cfg_editor)   echo "2) Open in text editor" ;;
    zh:cfg_editor)   echo "2) 用文本编辑器打开" ;;
    en:cfg_show)     echo "3) Show current config" ;;
    zh:cfg_show)     echo "3) 显示当前配置" ;;
    en:cfg_back)     echo "b) Back" ;;
    zh:cfg_back)     echo "b) 返回" ;;
    en:cfg_current)  echo "Current value:" ;;
    zh:cfg_current)  echo "当前值：" ;;
    en:dep_title)    echo "Dependency Check" ;;
    zh:dep_title)    echo "依赖检查" ;;
    en:dep_py_ok)    echo "Python 3 found:" ;;
    zh:dep_py_ok)    echo "已找到 Python 3：" ;;
    en:dep_py_fail)  echo "Python 3 not found. Please install Python 3.9+ from https://www.python.org" ;;
    zh:dep_py_fail)  echo "未找到 Python 3，请从 https://www.python.org 安装 Python 3.9+" ;;
    en:dep_ver_warn) echo "Warning: Python version may be too old (need 3.6+)." ;;
    zh:dep_ver_warn) echo "警告：Python 版本可能过低（需要 3.6+）。" ;;
    en:dep_venv_ok)  echo "Virtual environment found." ;;
    zh:dep_venv_ok)  echo "已找到虚拟环境。" ;;
    en:dep_venv_no)  echo "No virtual environment found." ;;
    zh:dep_venv_no)  echo "未找到虚拟环境。" ;;
    en:dep_venv_create) echo "Create a virtual environment now? [y/N]" ;;
    zh:dep_venv_create) echo "是否立即创建虚拟环境？[y/N]" ;;
    en:dep_venv_creating) echo "Creating virtual environment..." ;;
    zh:dep_venv_creating) echo "正在创建虚拟环境..." ;;
    en:dep_venv_done) echo "Virtual environment created." ;;
    zh:dep_venv_done) echo "虚拟环境已创建。" ;;
    en:dep_pkg_check) echo "Checking Python packages..." ;;
    zh:dep_pkg_check) echo "正在检查 Python 包..." ;;
    en:dep_ok)       echo "OK" ;;
    zh:dep_ok)       echo "已安装" ;;
    en:dep_missing)  echo "NOT INSTALLED" ;;
    zh:dep_missing)  echo "未安装" ;;
    en:dep_optional) echo "(optional)" ;;
    zh:dep_optional) echo "（可选）" ;;
    en:dep_install_prompt) echo "Install missing packages now? [y/N]" ;;
    zh:dep_install_prompt) echo "是否立即安装缺失的包？[y/N]" ;;
    en:dep_installing) echo "Installing packages..." ;;
    zh:dep_installing) echo "正在安装包..." ;;
    en:dep_done)     echo "Done." ;;
    zh:dep_done)     echo "完成。" ;;
    en:dep_macOS)    echo "macOS detected — OK." ;;
    zh:dep_macOS)    echo "检测到 macOS — 正常。" ;;
    en:dep_macOS_fail) echo "ERROR: This tool requires macOS. Exiting." ;;
    zh:dep_macOS_fail) echo "错误：本工具仅支持 macOS，退出。" ;;
    en:log_title)    echo "Logs" ;;
    zh:log_title)    echo "日志" ;;
    en:log_1)        echo "1) View sync_gate.log (last 50 lines)" ;;
    zh:log_1)        echo "1) 查看 sync_gate.log（最后 50 行）" ;;
    en:log_2)        echo "2) Open sync_gate.log in editor" ;;
    zh:log_2)        echo "2) 用编辑器打开 sync_gate.log" ;;
    en:log_none)     echo "Log file not found yet." ;;
    zh:log_none)     echo "日志文件尚不存在。" ;;
    en:log_b)        echo "b) Back" ;;
    zh:log_b)        echo "b) 返回" ;;
    en:press_enter)  echo "Press Enter to continue..." ;;
    zh:press_enter)  echo "按 Enter 继续..." ;;
    en:invalid)      echo "Invalid choice. Please try again." ;;
    zh:invalid)      echo "无效选项，请重新输入。" ;;
    en:goodbye)      echo "Goodbye!" ;;
    zh:goodbye)      echo "再见！" ;;
    en:back)         echo "Returning to main menu..." ;;
    zh:back)         echo "返回主菜单..." ;;
    en:need_python)  echo "Python 3 not found. Please run option 5 first." ;;
    zh:need_python)  echo "未找到 Python 3，请先执行选项 5 检查依赖。" ;;
    en:core_missing) echo "Required script not found:" ;;
    zh:core_missing) echo "未找到必需脚本：" ;;
    en:path_empty)   echo "Path cannot be empty:" ;;
    zh:path_empty)   echo "路径不能为空：" ;;
    en:path_conflict) echo "Backup folder must be outside export folder." ;;
    zh:path_conflict) echo "备份目录必须位于导出目录之外。" ;;
    en:cfg_invalid)  echo "Configuration not saved due to invalid paths." ;;
    zh:cfg_invalid)  echo "路径配置无效，未保存配置。" ;;
    en:run_exit_code) echo "Process exited with code" ;;
    zh:run_exit_code) echo "进程退出码" ;;
    *)               echo "[$key]" ;;
    esac
}

# =============================================================================
#  HELPERS
# =============================================================================

header() {
    echo ""
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════╗${RESET}"
    printf  "${BOLD}${CYAN}║  %-44s ║${RESET}\n" "$(t main_title)"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════╝${RESET}"
    echo ""
}

section() {
    echo ""
    echo -e "${BOLD}── $1 ──${RESET}"
    echo ""
}

ok()   { echo -e "  ${GREEN}✔${RESET}  $*"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
err()  { echo -e "  ${RED}✖${RESET}  $*"; }
info() { echo -e "  ${CYAN}→${RESET}  $*"; }

pause() { echo ""; read -rp "  $(t press_enter)" _; }
safe_clear() { clear || true; }

# 清洗用户输入路径：去首尾空格、去外层引号、解析波浪号
clean_input() {
    local val="$1"
    # Trim leading/trailing whitespace
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    # Remove surrounding quotes if dragged & dropped from Finder
    val="${val%\"}"; val="${val#\"}"
    val="${val%\'}"; val="${val#\'}"
    # Expand tilde
    val="${val/#\~/$HOME}"
    echo "$val"
}

detect_python() {
    if [[ -x "$VENV_DIR/bin/python3" ]]; then
        echo "$VENV_DIR/bin/python3"
    elif command -v python3 &>/dev/null; then
        echo "$(command -v python3)"
    else
        echo ""
    fi
}

detect_pip() {
    if [[ -x "$VENV_DIR/bin/pip3" ]]; then
        echo "$VENV_DIR/bin/pip3"
    elif [[ -x "$VENV_DIR/bin/pip" ]]; then
        echo "$VENV_DIR/bin/pip"
    elif command -v pip3 &>/dev/null; then
        echo "$(command -v pip3)"
    elif command -v pip &>/dev/null; then
        echo "$(command -v pip)"
    else
        echo ""
    fi
}

update_globals() {
    GLOBAL_PYTHON="$(detect_python)"
    GLOBAL_PIP="$(detect_pip)"
}

require_python() {
    if [[ -n "$GLOBAL_PYTHON" ]]; then
        return 0
    fi
    err "$(t need_python)"
    pause
    return 1
}

require_script() {
    local path="$1"
    if [[ -f "$path" ]]; then
        return 0
    fi
    err "$(t core_missing) $path"
    pause
    return 1
}

run_cmd_capture() {
    RUNNING_CHILD=1
    set +e
    "$@"
    local code=$?
    set -e
    RUNNING_CHILD=0
    return "$code"
}

_is_subpath_py() {
    local child="$1" parent="$2"
    "$GLOBAL_PYTHON" - "$child" "$parent" <<'PY'
import os, sys
child = os.path.realpath(os.path.abspath(os.path.expanduser(sys.argv[1])))
parent = os.path.realpath(os.path.abspath(os.path.expanduser(sys.argv[2])))
try:
    common = os.path.commonpath([child, parent])
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if common == parent else 1)
PY
}

validate_export_backup_paths() {
    local out="$1" backup="$2"
    if [[ -z "$out" ]]; then
        err "$(t path_empty) export"
        return 1
    fi
    if [[ -z "$backup" ]]; then
        err "$(t path_empty) backup"
        return 1
    fi

    if [[ -n "$GLOBAL_PYTHON" ]]; then
        if _is_subpath_py "$backup" "$out"; then
            err "$(t path_conflict)"
            info "export: $out"
            info "backup: $backup"
            return 1
        fi
    else
        # Fallback: simple string-prefix check if Python is unavailable.
        local n_out="${out%/}" n_backup="${backup%/}"
        if [[ "$n_backup" == "$n_out" || "$n_backup" == "$n_out/"* ]]; then
            err "$(t path_conflict)"
            info "export: $out"
            info "backup: $backup"
            return 1
        fi
    fi
    return 0
}

cfg_get() {
    local key="$1"
    if [[ -z "$GLOBAL_PYTHON" || ! -f "$CONFIG_FILE" ]]; then echo ""; return; fi
    "$GLOBAL_PYTHON" -c "
import json
try:
    d = json.load(open('$CONFIG_FILE'))
    print(d.get('$key', ''))
except:
    print('')
" 2>/dev/null || echo ""
}

resolve_runtime_python() {
    local cfg_py=""
    if [[ -n "$GLOBAL_PYTHON" && -f "$CONFIG_FILE" ]]; then
        cfg_py="$(cfg_get python_path)"
        cfg_py="$(clean_input "$cfg_py")"
    fi
    if [[ -n "$cfg_py" && -x "$cfg_py" ]]; then
        echo "$cfg_py"
        return 0
    fi
    echo "$GLOBAL_PYTHON"
}

validate_dualsync_config() {
    [[ ! -f "$CONFIG_FILE" ]] && return 1
    local _fmd _ftb _bmd _btb
    _fmd="$(clean_input "$(cfg_get folder_md)")"
    _ftb="$(clean_input "$(cfg_get folder_tb)")"
    _bmd="$(clean_input "$(cfg_get backup_md)")"
    _btb="$(clean_input "$(cfg_get backup_tb)")"
    local missing=()
    [[ -z "$_fmd" ]] && missing+=("folder_md")
    [[ -z "$_ftb" ]] && missing+=("folder_tb")
    [[ -z "$_bmd" ]] && missing+=("backup_md")
    [[ -z "$_btb" ]] && missing+=("backup_tb")
    if [[ ${#missing[@]} -gt 0 ]]; then
        warn "$(t ds_cfg_missing) ${missing[*]}"
        return 1
    fi
    return 0
}

# =============================================================================
#  LANGUAGE SELECTION
# =============================================================================

select_language() {
    safe_clear
    echo ""
    echo -e "  ${BOLD}Bear Markdown Export & Sync${RESET}"
    echo ""
    echo "  $(t select_lang)"
    echo ""
    echo "    1)  English"
    echo "    2)  中文"
    echo ""
    while true; do
        read -rp "  > " _lang_choice
        case "$_lang_choice" in
            1) LANG_CODE="en"; break ;;
            2) LANG_CODE="zh"; break ;;
            *) echo "  Please enter 1 or 2 / 请输入 1 或 2" ;;
        esac
    done
}

# =============================================================================
#  DEPENDENCY CHECK
# =============================================================================

dep_check() {
    safe_clear; header
    section "$(t dep_title)"

    # macOS check
    if [[ "$(uname -s)" == "Darwin" ]]; then
        ok "$(t dep_macOS)"
    else
        err "$(t dep_macOS_fail)"
        exit 1
    fi

    # Python 3
    if [[ -n "$GLOBAL_PYTHON" ]]; then
        local ver
        ver="$("$GLOBAL_PYTHON" --version 2>&1)"
        ok "$(t dep_py_ok) $ver  ($GLOBAL_PYTHON)"
        # Version check (need ≥ 3.6)
        local major minor
        major="$("$GLOBAL_PYTHON" -c 'import sys; print(sys.version_info.major)')"
        minor="$("$GLOBAL_PYTHON" -c 'import sys; print(sys.version_info.minor)')"
        if [[ "$major" -lt 3 || ("$major" -eq 3 && "$minor" -lt 6) ]]; then
            warn "$(t dep_ver_warn)"
        fi
    else
        err "$(t dep_py_fail)"
        pause; return
    fi

    # Virtual environment
    echo ""
    if [[ -d "$VENV_DIR" ]]; then
        ok "$(t dep_venv_ok) ($VENV_DIR)"
    else
        warn "$(t dep_venv_no)"
        read -rp "  $(t dep_venv_create) " _create_venv
        if [[ "$_create_venv" =~ ^[Yy]$ ]]; then
            info "$(t dep_venv_creating)"
            python3 -m venv "$VENV_DIR"
            ok "$(t dep_venv_done)"
            # Refresh globals to use newly created venv
            update_globals
        fi
    fi

    # Python packages
    echo ""
    info "$(t dep_pkg_check)"
    echo ""

    local missing_required=()
    local missing_optional=()

    _check_pkg() {
        local pkg="$1" display="$2" required="$3"
        local installed
        installed="$("$GLOBAL_PYTHON" -c "import importlib.util; print('yes' if importlib.util.find_spec('$pkg') else 'no')" 2>/dev/null || echo "no")"
        if [[ "$installed" == "yes" ]]; then
            ok "$display — $(t dep_ok)"
        else
            if [[ "$required" == "required" ]]; then
                err "$display — $(t dep_missing)"
                missing_required+=("$display")
            else
                warn "$display — $(t dep_missing) $(t dep_optional)"
                missing_optional+=("$display")
            fi
        fi
    }

    _check_pkg "AppKit"   "pyobjc-framework-Cocoa"  "required"
    _check_pkg "watchdog" "watchdog"                 "optional"
    _check_pkg "xxhash"   "xxhash"                   "optional"

    echo ""

    if [[ ${#missing_required[@]} -gt 0 || ${#missing_optional[@]} -gt 0 ]]; then
        if [[ -n "$GLOBAL_PIP" ]]; then
            read -rp "  $(t dep_install_prompt) " _install
            if [[ "$_install" =~ ^[Yy]$ ]]; then
                info "$(t dep_installing)"
                "$GLOBAL_PIP" install pyobjc-framework-Cocoa watchdog xxhash
                ok "$(t dep_done)"
            fi
        else
            warn "pip not found — install manually: pip install pyobjc-framework-Cocoa watchdog xxhash"
        fi
    fi

    pause
}

# =============================================================================
#  CONFIGURATION WIZARD
# =============================================================================

cfg_menu() {
    while true; do
        safe_clear; header
        section "$(t cfg_title)"
        echo "  $(t cfg_howto)"
        echo ""
        echo "  $(t cfg_guided)"
        echo "  $(t cfg_editor)"
        echo "  $(t cfg_show)"
        echo "  $(t cfg_back)"
        echo ""
        read -rp "  > " _cfg_choice
        case "$_cfg_choice" in
            1) cfg_guided ;;
            2) cfg_open_editor ;;
            3) cfg_show ;;
            b|B) return ;;
            *) echo "  $(t invalid)" ;;
        esac
    done
}

cfg_show() {
    safe_clear; header
    section "$(t cfg_title)"
    if [[ -f "$CONFIG_FILE" ]]; then
        echo ""
        cat "$CONFIG_FILE"
        echo ""
    else
        warn "Config file does not exist yet."
    fi
    pause
}

cfg_open_editor() {
    [[ ! -f "$CONFIG_FILE" ]] && cfg_write_defaults
    info "$(t cfg_open)"
    open "$CONFIG_FILE" 2>/dev/null || ${EDITOR:-nano} "$CONFIG_FILE"
    pause
}

cfg_write_defaults() {
    local venv_py=""
    [[ -x "$VENV_DIR/bin/python3" ]] && venv_py="$VENV_DIR/bin/python3"
    mkdir -p "$DUALSYNC_DIR"
    cat > "$CONFIG_FILE" <<EOF
{
    "script_path":              "$CORE_SCRIPT",
    "python_path":              "${venv_py}",
    "folder_md":                "",
    "folder_tb":                "",
    "backup_md":                "",
    "backup_tb":                "",
    "sync_interval_seconds":    30,
    "write_quiet_seconds":      30,
    "editor_cooldown_seconds":  5,
    "bear_settle_seconds":      3,
    "conflict_backup_dir":      "",
    "daemon_debounce_seconds":  3.0,
    "daemon_retry_seconds":     5.0
}
EOF
}

cfg_guided() {
    safe_clear; header
    section "$(t cfg_title)"
    if ! require_python; then
        return
    fi
    echo -e "  ${YELLOW}$(t cfg_warn)${RESET}"
    echo ""
    echo "  $(t cfg_intro)"
    echo ""

    [[ ! -f "$CONFIG_FILE" ]] && cfg_write_defaults

    # Load defaults
    local venv_py=""
    [[ -x "$VENV_DIR/bin/python3" ]] && venv_py="$VENV_DIR/bin/python3"
    
    local c_script="$CORE_SCRIPT"
    local c_python="${venv_py:-$GLOBAL_PYTHON}"
    local c_md="$HOME/Notes/Bear"
    local c_tb="$HOME/Notes/Bear_TB"
    local c_bmd="$HOME/Notes/BearBackup/MD"
    local c_btb="$HOME/Notes/BearBackup/TB"
    local c_interval="30"

    # Batch read config variables avoiding multiple Python calls
    if [[ -f "$CONFIG_FILE" && -n "$GLOBAL_PYTHON" ]]; then
        local cfg_vals
        cfg_vals=$("$GLOBAL_PYTHON" -c "
import json
try:
    d = json.load(open('$CONFIG_FILE'))
    print(d.get('script_path', ''))
    print(d.get('python_path', ''))
    print(d.get('folder_md', ''))
    print(d.get('folder_tb', ''))
    print(d.get('backup_md', ''))
    print(d.get('backup_tb', ''))
    print(d.get('sync_interval_seconds', 30))
except:
    pass
" 2>/dev/null)
        
        if [[ -n "$cfg_vals" ]]; then
            local arr=()
            while IFS= read -r _line; do
                arr+=("$_line")
            done <<< "$cfg_vals"
            [[ -n "${arr[0]:-}" ]] && c_script="${arr[0]}"
            [[ -n "${arr[1]:-}" ]] && c_python="${arr[1]}"
            [[ -n "${arr[2]:-}" ]] && c_md="${arr[2]}"
            [[ -n "${arr[3]:-}" ]] && c_tb="${arr[3]}"
            [[ -n "${arr[4]:-}" ]] && c_bmd="${arr[4]}"
            [[ -n "${arr[5]:-}" ]] && c_btb="${arr[5]}"
            [[ -n "${arr[6]:-}" ]] && c_interval="${arr[6]}"
        fi
    fi

    # Helper: prompt with current value shown
    _prompt_path() {
        local label="$1" current="$2"
        # Print prompts to stderr so command substitution captures only the value.
        echo -e "  ${BOLD}$(t "$label")${RESET}" >&2
        echo "  $(t cfg_current) $current" >&2
        read -rp "  > " _val
        if [[ -z "$_val" ]]; then
            echo "$current"
        else
            clean_input "$_val"
        fi
    }

    local v_script v_python v_md v_tb v_bmd v_btb v_interval

    v_script="$(_prompt_path cfg_script "$c_script")"; echo ""
    v_python="$(_prompt_path cfg_python "$c_python")"; echo ""
    v_md="$(_prompt_path cfg_md "$c_md")"; echo ""
    v_tb="$(_prompt_path cfg_tb "$c_tb")"; echo ""
    v_bmd="$(_prompt_path cfg_bmd "$c_bmd")"; echo ""
    v_btb="$(_prompt_path cfg_btb "$c_btb")"; echo ""
    v_interval="$(_prompt_path cfg_interval "$c_interval")"

    [[ ! "$v_interval" =~ ^[0-9]+$ ]] && v_interval=30
    (( v_interval < 30 )) && v_interval=30

    if ! require_script "$v_script"; then
        return
    fi

    if [[ "$v_md" == "$v_tb" ]]; then
        err "folder_md and folder_tb must be different paths / folder_md 与 folder_tb 不能相同"
        err "$(t cfg_invalid)"
        pause
        return
    fi

    if ! validate_export_backup_paths "$v_md" "$v_bmd" \
        || ! validate_export_backup_paths "$v_tb" "$v_btb" \
        || ! validate_export_backup_paths "$v_md" "$v_btb" \
        || ! validate_export_backup_paths "$v_tb" "$v_bmd"; then
        err "$(t cfg_invalid)"
        pause
        return
    fi

    if [[ -n "$v_python" && ! -x "$v_python" ]]; then
        warn "python_path is not executable — fallback to auto-detect."
        v_python=""
    fi

    if ! mkdir -p "$v_md" "$v_tb" "$v_bmd" "$v_btb"; then
        err "Failed to create one or more configured directories."
        err "$(t cfg_invalid)"
        pause
        return
    fi

    # Write config using environment variables to prevent Bash string injection issues
    export CFG_SCRIPT="$v_script"
    export CFG_PYTHON="$v_python"
    export CFG_MD="$v_md"
    export CFG_TB="$v_tb"
    export CFG_BMD="$v_bmd"
    export CFG_BTB="$v_btb"
    export CFG_INTERVAL="$v_interval"
    export CFG_FILE_PATH="$CONFIG_FILE"

    "$GLOBAL_PYTHON" - <<'PYEOF'
import json, os

data = {
    "script_path":             os.environ.get("CFG_SCRIPT", ""),
    "python_path":             os.environ.get("CFG_PYTHON", ""),
    "folder_md":               os.environ.get("CFG_MD", ""),
    "folder_tb":               os.environ.get("CFG_TB", ""),
    "backup_md":               os.environ.get("CFG_BMD", ""),
    "backup_tb":               os.environ.get("CFG_BTB", ""),
    "sync_interval_seconds":   int(os.environ.get("CFG_INTERVAL", 30)),
    "write_quiet_seconds":     30,
    "editor_cooldown_seconds": 5,
    "bear_settle_seconds":     3,
    "conflict_backup_dir":     "",
    "daemon_debounce_seconds": 3.0,
    "daemon_retry_seconds":    5.0,
}

cfg_path = os.environ.get("CFG_FILE_PATH")
os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
with open(cfg_path, "w") as f:
    json.dump(data, f, indent=4)
    f.write("\n")
PYEOF

    echo ""
    ok "$(t cfg_saved)"
    pause
}

# =============================================================================
#  QUICK SYNC  (bear_export_sync.py)
# =============================================================================

quick_sync() {
    safe_clear; header
    section "$(t qs_title)"

    if ! require_python; then
        return
    fi
    if ! require_script "$CORE_SCRIPT"; then
        return
    fi

    local _python
    _python="$(resolve_runtime_python)"

    echo ""
    echo -e "  ${BOLD}$(t qs_format)${RESET}"
    read -rp "  > " _format
    _format="$(clean_input "$_format")"
    [[ -z "$_format" ]] && _format="md"
    [[ "$_format" != "tb" ]] && _format="md"

    local _cfg_md _cfg_tb _cfg_bmd _cfg_btb
    _cfg_md="$(clean_input "$(cfg_get folder_md)")"
    _cfg_tb="$(clean_input "$(cfg_get folder_tb)")"
    _cfg_bmd="$(clean_input "$(cfg_get backup_md)")"
    _cfg_btb="$(clean_input "$(cfg_get backup_tb)")"

    local _default_out _default_backup
    if [[ "$_format" == "tb" ]]; then
        _default_out="${_cfg_tb:-$HOME/Notes/Bear_TB}"
        _default_backup="${_cfg_btb:-$HOME/Notes/BearBackup/TB}"
    else
        _default_out="${_cfg_md:-$HOME/Notes/Bear}"
        _default_backup="${_cfg_bmd:-$HOME/Notes/BearBackup/MD}"
    fi

    echo -e "  ${BOLD}$(t qs_out)${RESET}"
    echo "  $(t cfg_current) $_default_out"
    read -rp "  > " _out
    [[ -z "$_out" ]] && _out="$_default_out" || _out="$(clean_input "$_out")"

    echo ""
    echo -e "  ${BOLD}$(t qs_backup)${RESET}"
    echo "  $(t cfg_current) $_default_backup"
    read -rp "  > " _backup
    [[ -z "$_backup" ]] && _backup="$_default_backup" || _backup="$(clean_input "$_backup")"

    if ! validate_export_backup_paths "$_out" "$_backup"; then
        pause
        return
    fi

    echo ""
    echo -e "  ${BOLD}$(t qs_skip)${RESET}"
    echo "  $(t qs_skip1)"
    echo "  $(t qs_skip2)"
    echo "  $(t qs_skip3)"
    read -rp "  > " _mode
    local _skip_flag=""
    case "$_mode" in
        2) _skip_flag="--skipImport" ;;
        3) _skip_flag="--skipExport" ;;
        *) _skip_flag="" ;;
    esac

    echo ""
    echo -e "  ${BOLD}$(t qs_tag)${RESET}"
    read -rp "  > " _tag
    _tag="$(clean_input "$_tag")"

    echo ""
    info "$(t qs_running)"
    echo ""

    local _cmd=(
        "$_python" "$CORE_SCRIPT"
        "--out" "$_out"
        "--backup" "$_backup"
        "--format" "$_format"
    )
    [[ -n "$_skip_flag" ]] && _cmd+=("$_skip_flag")
    [[ -n "$_tag" ]] && _cmd+=("--excludeTag" "$_tag")

    local _exit=0
    if run_cmd_capture "${_cmd[@]}"; then
        _exit=0
    else
        _exit=$?
    fi

    echo ""
    if [[ $_exit -eq 0 ]]; then
        ok "No changes detected — nothing to sync."
    elif [[ $_exit -eq 1 ]]; then
        ok "Sync complete."
    else
        warn "$(t run_exit_code) $_exit. Check output above for details."
    fi

    pause
}

# =============================================================================
#  DUALSYNC MENU  (sync_gate.py)
# =============================================================================

run_gate_action() {
    local python_bin="$1"
    shift
    local msg="$1"
    shift
    echo ""
    info "$msg"
    echo ""
    local _exit=0
    if run_cmd_capture "$python_bin" "$GATE_SCRIPT" "$@"; then
        _exit=0
    else
        _exit=$?
    fi
    echo ""
    if [[ $_exit -ne 0 ]]; then
        warn "$(t run_exit_code) $_exit"
    fi
    pause
}

dualsync_menu() {
    while true; do
        safe_clear; header
        section "$(t ds_title)"

        echo "  $(t ds_1)"
        echo "  $(t ds_2)"
        echo "  $(t ds_3)"
        echo "  $(t ds_4)"
        echo "  $(t ds_5)"
        echo "  $(t ds_6)"
        echo "  $(t ds_b)"
        echo ""
        read -rp "  > " _ds_choice
        local _python=""

        if [[ "$_ds_choice" != "b" && "$_ds_choice" != "B" ]]; then
            if ! require_python; then
                continue
            fi
            if [[ ! -f "$CONFIG_FILE" ]]; then
                echo ""
                warn "$(t ds_no_config)"
                pause; continue
            fi
            if ! validate_dualsync_config; then
                pause; continue
            fi
            if ! require_script "$GATE_SCRIPT"; then
                continue
            fi
            _python="$(resolve_runtime_python)"
        fi

        case "$_ds_choice" in
            1) run_gate_action "$_python" "Running sync_gate.py (run-once)..." ;;
            2) run_gate_action "$_python" "Running sync_gate.py --daemon  (Ctrl-C to stop and return)..." --daemon ;;
            3) run_gate_action "$_python" "Running sync_gate.py --force..." --force ;;
            4) run_gate_action "$_python" "Running sync_gate.py --dry-run..." --dry-run ;;
            5) run_gate_action "$_python" "Running sync_gate.py --export-only..." --export-only ;;
            6) run_gate_action "$_python" "Running sync_gate.py --guard-test..." --guard-test ;;
            b|B) return ;;
            *) echo "  $(t invalid)" ;;
        esac
    done
}

# =============================================================================
#  LOG VIEWER
# =============================================================================

logs_menu() {
    while true; do
        safe_clear; header
        section "$(t log_title)"
        echo "  $(t log_1)"
        echo "  $(t log_2)"
        echo "  $(t log_b)"
        echo ""
        read -rp "  > " _log_choice
        case "$_log_choice" in
            1)
                echo ""
                if [[ -f "$GATE_LOG" ]]; then
                    tail -n 50 "$GATE_LOG"
                else
                    warn "$(t log_none)"
                fi
                pause ;;
            2)
                if [[ -f "$GATE_LOG" ]]; then
                    open "$GATE_LOG" 2>/dev/null || ${EDITOR:-less} "$GATE_LOG"
                else
                    warn "$(t log_none)"
                fi
                pause ;;
            b|B) return ;;
            *) echo "  $(t invalid)" ;;
        esac
    done
}

# =============================================================================
#  MAIN MENU
# =============================================================================

main_menu() {
    while true; do
        safe_clear; header
        echo "  $(t menu_prompt)"
        echo ""
        echo "  $(t opt_1)"
        echo "  $(t opt_2)"
        echo "  $(t opt_3)"
        echo "  $(t opt_4)"
        echo "  $(t opt_5)"
        echo "  $(t opt_q)"
        echo ""
        echo -e "  ${YELLOW}$(t main_tip)${RESET}"
        echo ""
        read -rp "  > " _main_choice
        case "$_main_choice" in
            1) quick_sync ;;
            2) dualsync_menu ;;
            3) cfg_menu ;;
            4) logs_menu ;;
            5) dep_check ;;
            q|Q)
                echo ""; info "$(t goodbye)"; echo ""
                exit 0 ;;
            *) echo "  $(t invalid)" ;;
        esac
    done
}

# =============================================================================
#  ENTRY POINT
# =============================================================================

main() {
    # macOS only — fail fast
    if [[ "$(uname -s)" != "Darwin" ]]; then
        echo "ERROR: This tool requires macOS."
        exit 1
    fi

    # 初始化全局 Python 缓存
    update_globals

    select_language
    main_menu
}

main "$@"

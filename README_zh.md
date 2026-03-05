# Bear Markdown 导出与同步

> 本项目是对 [andymatuschak 的 fork](https://github.com/andymatuschak/Bear-Markdown-Export)（原版 [rovest/Bear-Markdown-Export](https://github.com/rovest/Bear-Markdown-Export)）的深度重构。
> 已适配 Bear 2.0 · 全面性能重构 · 实时同步守护进程 · 面向新手的交互式启动器。

**English documentation: [README.md](README.md)**

---

## ⚠️ 请先备份

首次运行脚本前，请备份你的 Bear 笔记：
**Bear → 文件 → 备份笔记…**

同时建议用 Time Machine 或其他工具备份整台 Mac。脚本内部使用了 `rsync` 和 `shutil.rmtree` 等强力命令——路径配置有误可能导致文件被覆盖或删除。

---

## 相比原版的改进

原版 `bear_export_sync.py`（rovest → andymatuschak）是坚实的基础，本次重构系统性地解决了所有已知性能瓶颈，并新增了原版从未有过的生产级实时同步守护进程。

### b2ou cli — 性能与可靠性

| 模块 | 状态 |
|---|---|
| **架构** | 现代模块化 Python 包 (`b2ou/`) |
| **逻辑** | 解耦的核心引擎 + FSEvents 守护进程 |
| **类型安全** | 完全类型化 (PEP 484/585/604) |
| **正则表达式** | 预编译的静态正则，性能最优 |
| **文件日期** | 原生 `AppKit` / `NSFileManager` API |
| **编辑守卫** | 三层保护（lsof、mtime、前台应用检测） |

### 架构说明

```
B2OU-Bear-to-Obsidian-Ulysses/
├── b2ou/                  # 源代码包
│   ├── cli.py             # 命令行入口
│   ├── daemon.py          # 同步逻辑与守护进程
│   ├── export.py          # Bear → 磁盘
│   ├── import_.py         # 磁盘 → Bear
│   ├── guard.py           # 编辑保护逻辑
│   └── ...
├── pyproject.toml         # 构建与依赖元数据
└── b2ou_config.json       # 本地同步配置文件
```

---

## 快速上手 (仅限 macOS)

```bash
# 1. 克隆仓库
git clone https://github.com/desususu/B2OU-Bear-to-Obsidian-Ulysses-.git
cd B2OU-Bear-to-Obsidian-Ulysses-

# 2. 设置环境
python3 -m venv venv
source venv/bin/activate
pip install -e ".[all]"

# 3. 初始化配置
# 这将在当前目录创建一个默认的 b2ou_config.json
b2ou gate
```

修改 `b2ou_config.json` 中的路径后：

```bash
# 执行单次同步
b2ou sync

# 以守护进程模式运行（实时同步）
b2ou daemon
```

---

## 命令行参考

| 命令 | 说明 |
|---|---|
| `b2ou export` | 导出 Bear 笔记到磁盘（单向） |
| `b2ou import` | 从磁盘导入变更笔记到 Bear |
| `b2ou sync` | 执行一次完整的 导入 + 导出 循环 |
| `b2ou gate` | 单次智能同步（适合 cron/launchd） |
| `b2ou daemon` | FSEvents 驱动的守护进程模式（实时） |
| `b2ou guard-test` | 诊断编辑守卫层 |

### 参数说明

大多数命令支持：
- `--out PATH`: 导出笔记的目标目录
- `--backup PATH`: 冲突备份目录
- `--format md|tb`: 输出格式（Markdown 或 TextBundle）
- `--exclude-tag TAG`: 跳过特定标签的笔记

针对 `gate` 和 `daemon`:
- `--config FILE`: 配置文件路径（默认：`b2ou_config.json`）
- `--force`: 绕过守卫立即同步（仅限 gate）
- `--export-only`: 跳过导入阶段

| 变更来源 | 模式 | 延迟 |
|---|---|---|
| Bear 保存笔记 | 守护进程 | ~3–5 秒（防抖 → 第二层通过 → 同步） |
| 外部编辑器保存文件 | 守护进程 | ~30–35 秒（防抖 → 第二层等待静默期 → 重试 → 同步） |
| 任意变更 | 单次运行（launchd） | 0 – `sync_interval_seconds` 秒轮询抖动 |

### sync_config.json 配置参考

```json
{
    "script_path":              "./bear_export_sync.py",
    "python_path":              "./venv/bin/python3",
    "folder_md":                "/你的路径/MD_Export",
    "folder_tb":                "/你的路径/TB_Export",
    "backup_md":                "/你的路径/MD_Backup",
    "backup_tb":                "/你的路径/TB_Backup",
    "sync_interval_seconds":    30,
    "write_quiet_seconds":      30,
    "editor_cooldown_seconds":  5,
    "bear_settle_seconds":      3,
    "conflict_backup_dir":      "",
    "daemon_debounce_seconds":  3.0,
    "daemon_retry_seconds":     5.0
}
```

| 配置项 | 说明 |
|---|---|
| `script_path` | `bear_export_sync.py` 的路径（相对于 `DualSync/` 或绝对路径） |
| `python_path` | Python 解释器路径（留空 `""` 则自动检测） |
| `folder_md` | Markdown 导出目标目录 |
| `folder_tb` | Textbundle 导出目标目录 |
| `backup_md` / `backup_tb` | 冲突备份目录，必须在导出目录之外 |
| `sync_interval_seconds` | 单次运行/守护进程兜底轮询间隔（最小 30） |
| `write_quiet_seconds` | 允许同步前所需的静默时长——第二层守卫 |
| `editor_cooldown_seconds` | 编辑器切到后台后需等待的秒数——第三层守卫冷却 |
| `bear_settle_seconds` | Bear 数据库变更后同步前的等待时长 |
| `conflict_backup_dir` | 额外的冲突文件副本目录（可选） |
| `daemon_debounce_seconds` | 守护进程模式下的 FSEvents 防抖窗口 |
| `daemon_retry_seconds` | 守护进程模式下守卫阻塞时的重试间隔 |

---

## run.sh

```bash
bash run.sh
```

### 菜单结构

```
语言选择（English / 中文）
│
├── 1  快速同步      — 交互式运行 bear_export_sync.py 一次
├── 2  DualSync 菜单 — 单次运行 / 守护进程 / 强制 / 预演 / 仅导出 / 守卫测试
├── 3  配置路径      — 引导向导或直接打开 sync_config.json 编辑
├── 4  查看日志      — 查看 sync_gate.log 末尾行或用编辑器打开
├── 5  依赖检查      — 检查 Python / venv / 包；一键安装缺失项
└── q  退出
```

---

## 同步机制详解

### 导出（Bear → 磁盘）

1. 检查 `database.sqlite` 修改时间——若无变化立即退出
2. 从 Bear 的 SQLite 数据库查询所有笔记
3. 对每篇变更笔记：直接写入 `.md` 或 `.textbundle` 到导出目录
4. 仅复制变更笔记所引用的图片（增量，无全量 rsync）
5. 剥离 Bear 专有语法；在文件末尾追加 `BearID` 标记供回程匹配
6. `_cleanup_stale_notes()` — 删除 Bear 中已删除笔记对应的文件（复用导出时的预期路径集，零额外遍历）
7. `_cleanup_root_orphan_images()` — 删除不再被任何笔记引用的图片

### 导入（磁盘 → Bear）

1. 扫描导出目录，找出自上次同步后被修改的 `.md` / `.textbundle` 文件
2. 通过嵌入的 `BearID` 匹配对应的 Bear 笔记
3. 通过 `bear://x-callback-url/add-text?mode=replace` 更新笔记（保留原始创建日期和笔记 ID）
4. 发生冲突时：两个版本均保留在 Bear 中，并附有冲突提示
5. 不含 `BearID` 的新文件将作为新笔记导入 Bear

---

## 注意事项

**Obsidian 用户** — 每篇笔记的第一行必须是 `# 一级标题`。Obsidian 以此行推导文件名，若缺失则文件链接在整个笔记库中失效。

**sync_config.json 已加入 git 忽略列表** — 该文件包含本地路径，请勿提交。

**大型笔记库** — 首次导出可能需要一两分钟，后续同步只处理变更笔记，速度很快。

**未安装 watchdog** — `sync_gate.py` 将退化为轮询模式。守护进程仍可运行，但改为按轮询间隔响应，而非 FSEvents 驱动。

**launchd 配置** — 单次运行模式专为 launchd 设计。示例 plist 未包含在仓库中（路径因机器而异）。将 launchd 指向 `DualSync/sync_gate.py`，使用 venv 中的 Python，并设置 `StartInterval` 为 30–60 秒。

---

## 致谢

- 原始作者：[rovest](https://github.com/rovest)（[@rorves](https://twitter.com/rorves)）
- 修改者：[andymatuschak](https://github.com/andymatuschak)（[@andy_matuschak](https://twitter.com/andy_matuschak)）
- 进一步重构与维护：[desususu](https://github.com/desususu)

---

## 许可证

MIT — 详见 [LICENSE](LICENSE)。

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

### bear_export_sync.py — 性能全面重构

| 模块 | 原版 | 本次重构 |
|---|---|---|
| **正则表达式** | 每次处理笔记时在函数内编译 | 模块加载时一次性编译 29 个正则，永久复用 |
| **文件创建日期** | `SetFile -d` 子进程调用（约 50 ms / 文件） | `NSFileManager` 原生 API（< 1 ms / 文件） |
| **导出流程** | 全部笔记写入 `~/Temp/BearExportTemp` → rsync 到目标目录 | 直接原地写入目标目录，无临时文件夹，无 rsync |
| **图片同步** | `copy_bear_images()` 每次循环对整个 Bear 图片库执行 `rsync -r` | 在导出循环内增量复制——仅复制变更笔记引用的图片 |
| **过期文件清理** | 未实现 | `_cleanup_stale_notes()` 利用导出时构建的预期路径集，零额外遍历 |
| **孤儿图片清理** | 未实现 | `_cleanup_root_orphan_images()` 交叉比对所有笔记中的图片引用 |
| **图片语法支持** | 仅 Bear `[image:…]` | 新增 HTML `<img src=…>`、`![[wikilink]]`、引用式链接 |
| **运行模式** | 仅导出 + 导入 | 新增 `--skipExport`、`--skipImport`、`--excludeTag`、`--hideTags`、`--format md/tb` |
| **代码规模** | 763 行，38 个函数 | 1 430 行，60 个函数 |

**实际效果（约 500 篇笔记、200 张图片的笔记库）：**

| 操作 | 原版 | 本次重构 |
|---|---|---|
| 为 20 个新文件设置创建日期 | ~1 000 ms（20 × 50 ms 子进程） | < 20 ms（原生 API） |
| 图片同步 | 每次循环全量 rsync | 无变更笔记时零成本 |
| 过期笔记删除 | 手动或不处理 | 自动，寄生于导出循环，零额外 I/O |

### DualSync/sync_gate.py — 全新实时守护进程

原版项目没有实时守护进程，只能依赖 cron/launchd 定时器每 5–15 分钟轮询一次，且没有任何编辑保护机制。`sync_gate.py` 是从头编写的专用同步守卫：

- **三层编辑守卫** — 绝不打断正在进行的编辑
  - **第一层 — 文件打开检测：** `lsof +D` 检测笔记目录中是否有编辑器持有的文件句柄
  - **第二层 — 写入静默期：** 最近 N 秒内（`write_quiet_seconds`）不得有笔记被写入
  - **第三层 — 前台应用检测：** `NSWorkspace.frontmostApplication()`（< 1 ms，无子进程）检测 Bear、Obsidian、Typora 或 Ulysses 是否在前台
- **FSEvents 守护模式**（`--daemon`）— 通过 `watchdog` 监控 Bear 的 SQLite WAL 文件和导出目录；Bear 保存笔记后约 3–5 秒内触发同步
- **两阶段防抖 + 重试** — 快速写入风暴由防抖计时器合并处理；若防抖后守卫仍阻塞，每隔 `daemon_retry_seconds` 秒重试，直到条件满足
- **自身事件抑制** — 同步完成后的冷却窗口静默丢弃守护进程自身写入产生的 FSEvents 回声
- **VaultSnapshot** — 每个目录每次循环只做一次 `os.walk`，同时为变更检测、云端垃圾清理和内容哈希提供数据（取代原来三次独立遍历）
- **内容哈希** — `xxhash`（xxh3_128），可回退至 SHA-256；大小预过滤在哈希计算之前短路未变更的文件
- **锁文件** — 防止多实例并发运行
- **云端垃圾过滤** — 自动清除 `.DS_Store`、群晖 `@eaDir`、Dropbox 临时文件、空白笔记文件等云同步垃圾
- **兼容 launchd 的单次运行模式**（默认）— 检查守卫 → 安全则同步 → 退出；内存占用极低，延迟约为 0–`sync_interval_seconds` 秒
- **信号处理** — `SIGTERM` / `SIGINT` 触发干净退出（停止观察者、保存状态、释放锁）

### run.sh — 全新交互式启动器

原版项目仅附带一个将开发者本地路径硬编码其中的单一 Shell 脚本。`run.sh` 是一个完整的双语交互式启动器：

- 启动时选择语言（English / 中文）
- 依赖检查：Python 版本、虚拟环境、`pyobjc-framework-Cocoa`、`watchdog`、`xxhash`
- 一键创建 venv 并通过 pip 安装缺失包
- 引导式配置向导——通过 Python 写入 `sync_config.json`，即使路径含空格或特殊字符也能保证 JSON 合法
- 支持从 Finder 拖拽路径（自动去除引号和首尾空格）
- 覆盖两个脚本的全部运行模式
- 日志查看器（查看末尾行 / 用编辑器打开）
- 不包含任何硬编码路径

---

## 架构说明

```
Bear-Markdown-Export/
├── bear_export_sync.py       # 核心导出/导入引擎
├── run.sh                    # 双语交互式启动器
├── DualSync/
│   ├── sync_gate.py          # 智能同步守护进程
│   └── sync_config.json      # 本地配置文件（不提交到 git）
├── LICENSE
├── README.md                 # 英文文档
└── README_zh.md              # 中文文档（本文件）
```

数据流（以 Bear 为中心的轮辐式架构，Bear 始终是唯一真相来源）：

```
外部编辑器修改 MD/TB 文件
  └→ sync_gate 检测到变更（FSEvents 或轮询）
     └→ bear_export_sync 导入到 Bear（--skipExport）
        └→ Bear 数据库变更 → FSEvents 触发
           └→ bear_export_sync 导出到所有配置的目录
```

---

## 环境要求

- **macOS 12 Monterey 或更新版本**（依赖 `AppKit`、`NSWorkspace`、`NSFileManager`、FSEvents）
- **Bear 2.0** 已安装并登录
- **Python 3.9+**（最低 3.6+）

### Python 依赖包

| 依赖包 | 使用方 | 是否必须 |
|---|---|---|
| `pyobjc-framework-Cocoa` | 两个脚本 — `AppKit`、`NSWorkspace`、`NSFileManager` | **必须** |
| `watchdog` | `sync_gate.py` — FSEvents 守护模式 | 强烈建议 |
| `xxhash` | `sync_gate.py` — 快速内容哈希 | 可选（可回退至 SHA-256） |

---

## 快速上手

```bash
# 1. 克隆仓库
git clone https://github.com/desususu/B2OU-Bear-to-Obsidian-Ulysses-.git
cd B2OU-Bear-to-Obsidian-Ulysses-

# 2. 启动交互式向导
bash run.sh
```

在菜单中：
1. **选项 5** — 检查并安装依赖
2. **选项 3** — 配置导出路径（选择「引导配置」）
3. **选项 1** 单次同步，或 **选项 2 → 守护进程模式** 启动实时同步

---

## bear_export_sync.py

核心引擎，可单独运行，也可由 `sync_gate.py` 驱动。

### 命令行参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--out PATH` | `~/Work/BearNotes` | 导出笔记的目标目录 |
| `--backup PATH` | `~/Work/BearSyncBackup` | 冲突备份目录（必须在 `--out` 之外） |
| `--format md\|tb` | `md` | 输出格式：纯 Markdown 或 Textbundle |
| `--images PATH` | `<out>/BearImages` | 自定义图片库路径 |
| `--skipImport` | 关 | 跳过导入阶段，仅导出 |
| `--skipExport` | 关 | 跳过导出阶段，仅导入 |
| `--excludeTag TAG` | — | 排除带有此标签的笔记（可重复使用） |
| `--hideTags` | 关 | 导出时将标签包裹在 HTML 注释中 |

### 退出码

| 退出码 | 含义 |
|---|---|
| `0` | 无变更，无需导出 |
| `1` | 笔记导出成功 |

### 使用示例

```bash
source venv/bin/activate

# 完整同步 — Markdown 格式
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup

# Textbundle 格式
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --format tb

# 仅导出（跳过导入）
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --skipImport

# 仅导入（跳过导出）
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --skipExport

# 排除私密笔记
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --excludeTag private

# 自定义图片目录
python3 bear_export_sync.py --out ~/Notes/Bear --backup ~/Notes/BearBackup --images ~/Notes/Images
```

---

## DualSync/sync_gate.py

智能同步守护进程，从 `DualSync/sync_config.json` 读取配置。

### 运行模式

| 命令 | 说明 |
|---|---|
| `python3 DualSync/sync_gate.py` | **单次运行** — 检查守卫，安全则同步，退出。适合 launchd。 |
| `python3 DualSync/sync_gate.py --daemon` | **守护进程** — 常驻内存，监听 FSEvents，秒级响应变更 |
| `python3 DualSync/sync_gate.py --force` | 绕过所有守卫，立即强制同步 |
| `python3 DualSync/sync_gate.py --export-only` | 跳过导入阶段 |
| `python3 DualSync/sync_gate.py --dry-run` | 预演——显示将执行的操作，不实际写入 |
| `python3 DualSync/sync_gate.py --guard-test` | 诊断三层守卫并退出 |

### 各场景同步延迟

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

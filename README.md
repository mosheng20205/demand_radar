# Demand Radar

中文：`Demand Radar` 是一个面向“高价值、高摩擦需求信号”的轻量采集、评分、去重、导出与通知工具。它适合用来持续观察公开网页、RSS、JSON 接口和已登录浏览器页面中的需求线索，然后把结果沉淀到 SQLite、CSV 和企业微信/飞书/邮件通知里。  
English: `Demand Radar` is a lightweight pipeline for collecting, scoring, deduplicating, exporting, and notifying on high-friction public demand signals. It watches public pages, RSS feeds, JSON endpoints, and logged-in browser pages, then stores the results in SQLite, CSV exports, and optional WeCom/Feishu/email notifications.

中文：这个仓库目前是 CLI + 配置文件驱动，没有 Web 后台。它更像一个“可扩展的需求信号雷达骨架”，而不是一个开箱即用的 SaaS 产品。  
English: This repository is currently CLI-first and config-driven. It is closer to an extensible demand-signal radar skeleton than a polished SaaS product.

中文：当前仓库主要在 Windows + PowerShell 环境下验证，核心逻辑是纯 Python。样例配置可以离线运行；真实配置会依赖网络、站点可用性、Playwright 和部分 Cloak 浏览器 profile。  
English: The repository has mainly been validated on Windows + PowerShell, while the core pipeline is plain Python. Sample configs run offline; real configs depend on network availability, site stability, Playwright, and in some cases Cloak browser profiles.

## 10 行内超短命令版 / 10-Line Copy-Paste Version

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
python -m playwright install chromium
Copy-Item scripts\set_env_example.ps1 scripts\set_env.ps1
notepad scripts\set_env.ps1
python -m radar.cli --config config\sources.sample.json --export exports\leads.csv
powershell -ExecutionPolicy Bypass -File scripts\run_radar.ps1 -ConfigPath config\sources.real.sample.json
python scripts\send_test_notification.py --config config\sources.real.sample.json --kind lead
```

中文：第 7 行是本地离线样例验证，不需要 Cloak；第 8-9 行是接近真实运行的路径，需要你先填好 `scripts/set_env.ps1`。  
English: Line 7 is the offline smoke test and does not require Cloak. Lines 8-9 are the near-real run path and require `scripts/set_env.ps1` to be configured first.

## 3 分钟上手 / 3-Minute Onboarding

1. 中文：先跑 `config/sources.sample.json`，确认 Python、依赖、SQLite 导出链路都正常。  
   English: Start with `config/sources.sample.json` to verify Python, dependencies, SQLite, and CSV exports.
2. 中文：复制 `scripts/set_env_example.ps1` 为 `scripts/set_env.ps1`，填好企业微信/飞书/邮箱 webhook，以及需要的 Cloak profile。  
   English: Copy `scripts/set_env_example.ps1` to `scripts/set_env.ps1`, then fill in your WeCom/Feishu/email webhooks and any Cloak profiles you need.
3. 中文：用 `scripts/run_radar.ps1` 跑 `config/sources.real.sample.json`，查看 `logs/`、`exports/`、`data/`。  
   English: Run `config/sources.real.sample.json` through `scripts/run_radar.ps1`, then inspect `logs/`, `exports/`, and `data/`.
4. 中文：用 `scripts/send_test_notification.py` 先测试通知，再决定是否注册定时任务。  
   English: Test notifications with `scripts/send_test_notification.py` before registering a scheduled task.

## 项目解决什么问题 / What This Project Solves

中文：很多“真需求”不会直接出现在 CRM 里，而是散落在评论区、求助帖、服务市场、招标页、招聘描述和已登录页面里。这个项目的目标是把这些分散信号统一采集下来，再通过关键词规则、来源权重和去重逻辑，筛出更值得跟进的线索。  
English: Many real demand signals never appear directly inside a CRM. They are scattered across comments, forum posts, service marketplaces, procurement pages, job descriptions, and logged-in pages. This project pulls those signals into one place and ranks them using keyword rules, source bonuses, and deduplication.

中文：它当前已经覆盖了几类真实来源示例，包括猪八戒需求大厅、闲鱼服务、Bilibili、抖音、小红书、淘宝服务市场、CNode、Remote OK、政府采购公告和飞书应用市场。  
English: The repository already contains real-source examples for several source families, including ZBJ demand pages, Xianyu listings, Bilibili, Douyin, Xiaohongshu, Taobao Service Market, CNode, Remote OK, government procurement notices, and the Feishu app marketplace.

中文：它不是什么：不是通用爬虫平台，不是反检测绕过教程，不承诺所有站点始终稳定，也不是法律/合规兜底方案。  
English: What it is not: it is not a general-purpose scraping platform, not a stealth-bypass tutorial, does not promise every site will stay stable forever, and is not a compliance substitute.

## 核心流程 / Core Flow

1. 中文：读取 JSON 配置，解析环境变量占位符，例如 `${DEMAND_RADAR_WECOM_WEBHOOK}`。  
   English: Load the JSON config and resolve environment placeholders such as `${DEMAND_RADAR_WECOM_WEBHOOK}`.
2. 中文：逐个来源抓取内容，支持 `rss`、`json`、`html_links`、`html_text_regex`、`cloak_cdp_page`。  
   English: Fetch each source using `rss`, `json`, `html_links`, `html_text_regex`, or `cloak_cdp_page`.
3. 中文：按关键词规则、来源权重和机会强度打分，并写入 SQLite。  
   English: Score each item using keyword rules, source bonuses, and opportunity strength, then store it in SQLite.
4. 中文：导出主结果、来源健康度、Top20、主题榜、产品方向等 CSV。  
   English: Export the main lead list, source health, Top20, theme leaderboard, and product directions to CSV.
5. 中文：按阈值推送企业微信、飞书或邮件，并可发送失败告警和每日汇总。  
   English: Send WeCom, Feishu, or email notifications above the configured threshold, plus failure alerts and daily summaries.

## 环境要求 / Requirements

- 中文：Python `3.11+`  
  English: Python `3.11+`
- 中文：`playwright>=1.54,<2`，并安装 Chromium  
  English: `playwright>=1.54,<2` with Chromium installed
- 中文：Windows PowerShell 用于仓库内现成脚本；直接 CLI 运行不限于 PowerShell  
  English: Windows PowerShell for the helper scripts shipped in this repo; the core CLI itself is not PowerShell-only
- 中文：如果要跑登录态来源，需要已安装并可用的 Cloak 浏览器  
  English: Cloak is required only for logged-in / profile-based sources
- 中文：如果要推送通知，需要企业微信机器人、飞书机器人或 SMTP 邮箱配置  
  English: Notification delivery requires a WeCom bot, Feishu bot, or SMTP email configuration

## 安装 / Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
python -m playwright install chromium
```

中文：如果你只想先确认项目能不能跑，安装到这里就够了。  
English: If you only want to confirm the project runs, the steps above are enough.

## 快速运行样例 / Fast Offline Sample Run

```powershell
python -m radar.cli --config config/sources.sample.json --export exports/leads.csv
```

中文：这个配置只读取仓库内 `samples/` 的本地样例文件，不访问外网，不依赖 Cloak，不需要登录态。它是最适合首次验收仓库是否可运行的入口。  
English: This config only reads local sample files under `samples/`. It does not hit the network, does not need Cloak, and does not require login state. It is the safest first-run entrypoint for new maintainers.

中文：如果你想看一套“干净的首次结果”，先删除旧的 `data/demand_radar.db` 和 `exports/*.csv`，因为仓库里可能已经带着历史运行产物。  
English: If you want a clean first-run result set, delete the old `data/demand_radar.db` and `exports/*.csv` first, because the repository may already contain historical run artifacts.

中文：你跑完后至少会看到这些文件：  
English: After the run, you should at least see these files:

- `exports/leads.csv`
- `exports/source_health.csv`
- `exports/top20_leads.csv`
- `exports/opportunity_themes.csv`
- `exports/product_directions.csv`
- `data/demand_radar.db`

## 真实运行 / Real Run

### 1. 复制本地环境模板 / Copy the Local Environment Template

```powershell
Copy-Item scripts\set_env_example.ps1 scripts\set_env.ps1
notepad scripts\set_env.ps1
```

中文：`scripts/run_radar.ps1` 会自动加载 `scripts/set_env.ps1`。这个文件应该只存在于你本地，不应该提交到 Git。  
English: `scripts/run_radar.ps1` automatically loads `scripts/set_env.ps1`. This file should stay local and should not be committed to Git.

### 2. 填写通知和浏览器变量 / Fill Notification and Browser Variables

中文：最常见需要填写的是下面这些变量。  
English: These are the most common variables you will need to fill in.

```powershell
$env:DEMAND_RADAR_WECOM_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
$env:DEMAND_RADAR_FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_HOOK"
$env:DEMAND_RADAR_CLOAK_API_BASE_URL = "http://127.0.0.1:54381"
$env:DEMAND_RADAR_ZBJ_PROFILE_ID = ""
$env:DEMAND_RADAR_BILIBILI_PROFILE_ID = ""
$env:DEMAND_RADAR_DOUYIN_PROFILE_ID = ""
$env:DEMAND_RADAR_XIAOHONGSHU_PROFILE_ID = ""
```

中文：如果你没有 profile ID，也可以填 `*_PROFILE_NAME`，但优先推荐 `*_PROFILE_ID`，因为更稳定。  
English: You can use `*_PROFILE_NAME` when you do not have the profile ID yet, but `*_PROFILE_ID` is preferred because it is more stable.

### 3. 运行真实配置 / Run a Real Config

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_radar.ps1 -ConfigPath config\sources.real.sample.json
```

中文：这个配置是“多来源真实示例总表”，会混合公共来源和部分需要 Cloak 的来源。  
English: This config is the multi-source real-world example and mixes public sources with several Cloak-backed sources.

中文：如果你想单独调某个平台，仓库里也有更小粒度的配置，例如：  
English: If you want to debug one platform at a time, the repo also includes more focused configs such as:

- `config/sources.cloak.sample.json`
- `config/sources.xianyu.sample.json`
- `config/sources.xianyu.real.json`
- `config/sources.bilibili.real.json`
- `config/sources.douyin.real.json`
- `config/sources.xiaohongshu.real.json`
- `config/sources.site.sample.json`

## 通知测试 / Notification Test

中文：不要等正式采集结束才知道 webhook 配错。先发一条测试消息。  
English: Do not wait for a real collection run to discover your webhook is wrong. Send a test notification first.

```powershell
. .\scripts\set_env.ps1
python scripts\send_test_notification.py --config config/sources.real.sample.json --kind lead
python scripts\send_test_notification.py --config config/sources.real.sample.json --kind failure
python scripts\send_test_notification.py --config config/sources.real.sample.json --kind daily
```

中文：`lead` 测试新线索通知，`failure` 测试来源失败告警，`daily` 测试每日汇总。  
English: `lead` sends a new-lead notification, `failure` sends a source-failure alert, and `daily` sends a daily summary.

## 输出文件说明 / Output Files

中文：运行结果主要分成 4 类。  
English: Runtime outputs mainly fall into 4 groups.

- 中文：`data/*.db` 是 SQLite 数据库，保存去重后的线索、来源运行记录和汇总状态。  
  English: `data/*.db` stores deduplicated leads, source run history, and report state in SQLite.
- 中文：`exports/*.csv` 是给人看和给表格分析用的结果，包含主线索表、来源健康度、Top20、主题榜和产品方向。  
  English: `exports/*.csv` is the human-readable analysis layer: main leads, source health, Top20, theme leaderboard, and product directions.
- 中文：`logs/*.log` 是主运行日志；`logs/*.rendered.html`、`*.png`、`*.network.json`、`*.items.json` 是页面抓取和 Cloak/CDP 调试产物。  
  English: `logs/*.log` is the main runtime log; `logs/*.rendered.html`, `*.png`, `*.network.json`, and `*.items.json` are page-capture and Cloak/CDP debugging artifacts.
- 中文：仓库里现有的 `data/`、`exports/`、`logs/` 文件大多是历史运行产物，可以删掉后重新生成。  
  English: Most existing files under `data/`, `exports/`, and `logs/` are historical run artifacts and can be deleted and regenerated.

中文：首次验收时，重点看这几个文件：  
English: For first-time validation, focus on these files:

- `logs/demand_radar.log`
- `exports/leads.real.csv`
- `exports/source_health.csv`
- `exports/top20_leads.csv`
- `exports/opportunity_themes.csv`
- `exports/product_directions.csv`

## 配置文件怎么理解 / How to Read the Config Files

中文：每个配置文件都是“顶层运行参数 + sources 列表”的组合。  
English: Each config file is a combination of top-level runtime settings plus a `sources` list.

```json
{
  "database_path": "data/demand_radar.db",
  "export_path": "exports/leads.csv",
  "notifications": {
    "wecom": { "enabled": false, "webhook_url": "" }
  },
  "sources": [
    {
      "name": "sample_posts",
      "kind": "json",
      "category": "forum_posts",
      "location": "samples/posts_feed.json",
      "site_kind": "optional-parser-shape",
      "max_items": 20
    }
  ]
}
```

中文：几个最重要的字段如下。  
English: The most important fields are:

- `database_path`: 中文：SQLite 数据库位置。  
  English: SQLite database path.
- `export_path`: 中文：主线索 CSV 导出位置。  
  English: Main lead CSV export path.
- `notifications`: 中文：企业微信、飞书、邮箱开关和目标地址。  
  English: WeCom, Feishu, and email notification settings.
- `sources[]`: 中文：真正的数据来源列表。  
  English: The actual list of data sources.
- `sources[].name`: 中文：来源标识，必须稳定且唯一。  
  English: Stable unique source identifier.
- `sources[].kind`: 中文：抓取方式，例如 `json`、`rss`、`html_links`、`html_text_regex`、`cloak_cdp_page`。  
  English: Fetch mode such as `json`, `rss`, `html_links`, `html_text_regex`, or `cloak_cdp_page`.
- `sources[].category`: 中文：来源大类，会影响后续评分权重和通知语义。  
  English: Source category, which influences scoring and notification meaning.
- `sources[].location`: 中文：本地样例文件路径或线上地址。  
  English: Local sample file path or live URL.
- `sources[].site_kind`: 中文：特定站点解析器的形状提示，不是所有来源都需要。  
  English: Parser shape hint for site-specific extraction; not every source needs it.

中文：仓库里几个关键配置分别适合这些场景。  
English: The key config files in this repo are intended for these scenarios.

- `config/sources.sample.json`: 中文：最小离线样例，第一次跑项目用它。  
  English: Minimal offline sample; use this for your first run.
- `config/sources.site.sample.json`: 中文：更像真实站点形状的离线样例，适合理解站点解析。  
  English: Richer offline samples shaped like real sites; useful for parser understanding.
- `config/sources.cloak.sample.json`: 中文：最小 Cloak/CDP 示例。  
  English: Minimal Cloak/CDP example.
- `config/sources.real.sample.json`: 中文：真实多来源总配置。  
  English: Full multi-source real-world example config.
- `config/sources.expansion.sample.json`: 中文：新来源扩展骨架，对应 `SOURCE_BACKLOG.md`。  
  English: Source-expansion skeleton tied to `SOURCE_BACKLOG.md`.

## Cloak 浏览器配置 / Cloak Browser Setup

中文：没装 Cloak 的同事先去官网安装，再继续看后面的 profile 配置步骤。  
English: If you do not have Cloak installed yet, install it from the official site first, then continue with the profile setup steps below.

中文：Cloak 官网：<https://bcloak.com/>  
English: Cloak official site: <https://bcloak.com/>

中文：在这个项目里，Cloak 的作用不是“神秘黑盒”，而是一个本地浏览器 profile 管理器。它帮你保留登录态、Cookie、设备环境和页面会话，项目再通过本地 Cloak API + Playwright CDP 连接到这个 profile 去抓页面。  
English: In this project, Cloak is not a mysterious black box. It is a local browser profile manager that preserves login state, cookies, device environment, and session context. The project then connects to that profile through the local Cloak API and Playwright CDP.

中文：需要 Cloak 的常见场景：闲鱼、Bilibili、抖音、小红书、猪八戒这类在未登录或风控状态下很难稳定读取的页面。  
English: Typical Cloak use cases are Xianyu, Bilibili, Douyin, Xiaohongshu, and ZBJ pages that are hard to read reliably when logged out or under stronger anti-automation checks.

### 配置步骤 / Setup Steps

1. 中文：安装 Cloak，并确认本机可打开本地 API 文档。  
   English: Install Cloak and confirm the local API docs are reachable.

   - `http://127.0.0.1:54381/swagger-ui/`
   - `http://127.0.0.1:54381/api-docs/openapi.json`

2. 中文：在 Cloak 里为目标平台创建 profile，例如 ZBJ、Bilibili、Douyin、Xiaohongshu。  
   English: Create platform-specific profiles in Cloak, for example ZBJ, Bilibili, Douyin, and Xiaohongshu.
3. 中文：手动打开每个 profile，登录对应站点，确认页面能正常访问。  
   English: Open each profile manually, log in to the target site, and confirm the pages load correctly.
4. 中文：把 profile 的 ID 或名称写入 `scripts/set_env.ps1`。优先使用 ID。  
   English: Put the profile ID or name into `scripts/set_env.ps1`. Prefer the ID.
5. 中文：保持 Cloak 运行，然后再执行真实配置。  
   English: Keep Cloak running before executing real configs.

### 常用变量 / Common Variables

```powershell
$env:DEMAND_RADAR_CLOAK_API_BASE_URL = "http://127.0.0.1:54381"
$env:DEMAND_RADAR_ZBJ_PROFILE_ID = ""
$env:DEMAND_RADAR_ZBJ_PROFILE_NAME = "ZBJ Browser"
$env:DEMAND_RADAR_BILIBILI_PROFILE_ID = ""
$env:DEMAND_RADAR_BILIBILI_PROFILE_NAME = "Bilibili Browser"
$env:DEMAND_RADAR_DOUYIN_PROFILE_ID = ""
$env:DEMAND_RADAR_DOUYIN_PROFILE_NAME = "Douyin Browser"
$env:DEMAND_RADAR_XIAOHONGSHU_PROFILE_ID = ""
$env:DEMAND_RADAR_XIAOHONGSHU_PROFILE_NAME = "Xiaohongshu Browser"
```

中文：原则上每个平台一个 profile，不要混用。  
English: In practice, use one dedicated profile per platform instead of sharing one profile across multiple sites.

## 定时运行 / Scheduling

中文：仓库自带 3 个 PowerShell 脚本，分别用于注册、查看和删除计划任务。  
English: The repo ships with three PowerShell scripts for registering, inspecting, and removing scheduled tasks.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_task.ps1 -TaskName DemandRadar -ConfigPath config\sources.real.sample.json -IntervalMinutes 30
powershell -ExecutionPolicy Bypass -File scripts\show_task.ps1 -TaskName DemandRadar
powershell -ExecutionPolicy Bypass -File scripts\unregister_task.ps1 -TaskName DemandRadar
```

中文：`register_task.ps1` 最终调用的仍然是 `scripts/run_radar.ps1`，所以它也会自动加载 `scripts/set_env.ps1`。  
English: `register_task.ps1` still routes through `scripts/run_radar.ps1`, so it also auto-loads `scripts/set_env.ps1`.

## 常见报错排查 / Common Troubleshooting

### 1. `Cloak profile not found`

中文：通常是 profile ID / 名称填错，或者 Cloak 本地 API 没起来。  
English: This usually means the profile ID/name is wrong or the local Cloak API is not available.

排查建议 / What to check:

- 中文：确认 Cloak 正在运行。  
  English: Confirm Cloak is running.
- 中文：确认 `DEMAND_RADAR_CLOAK_API_BASE_URL` 指向正确地址，默认是 `http://127.0.0.1:54381`。  
  English: Confirm `DEMAND_RADAR_CLOAK_API_BASE_URL` points to the correct address, usually `http://127.0.0.1:54381`.
- 中文：优先改用 `*_PROFILE_ID`，不要只依赖模糊的 profile 名称。  
  English: Prefer `*_PROFILE_ID` instead of relying on profile names.
- 中文：先手动打开该 profile，确认它确实存在且能访问目标网站。  
  English: Open the profile manually first and verify it exists and can reach the target site.

### 2. `source_errors > 0`

中文：这不一定代表整次运行失败。`Demand Radar` 的设计是“单来源失败不阻断全局导出”，所以你可能仍然会拿到一部分结果。  
English: This does not always mean the whole run failed. `Demand Radar` is intentionally tolerant of per-source failures, so partial exports may still succeed.

排查建议 / What to check:

- 中文：先看 `logs/demand_radar.log`。  
  English: Start with `logs/demand_radar.log`.
- 中文：再看 `exports/source_health.csv` 里的 `last_error`、`consecutive_failures`、`cooldown_until`。  
  English: Then inspect `last_error`, `consecutive_failures`, and `cooldown_until` in `exports/source_health.csv`.
- 中文：如果你刚接手项目，先把问题缩小，优先单跑某个平台自己的 config，而不是一上来跑全量多来源。  
  English: If you are new to the project, reduce the blast radius and run a single-platform config before the full multi-source config.
- 中文：公共站点结构变化很常见，所以 `source_errors > 0` 更像“有来源需要调试”，不一定是主流程完全不可用。  
  English: Public site shape changes are common, so `source_errors > 0` often means a source needs tuning rather than the whole pipeline being unusable.

### 3. `ModuleNotFoundError: playwright` 或浏览器未安装

中文：这是最常见的新机器问题。  
English: This is one of the most common issues on a fresh machine.

解决方式 / Fix:

```powershell
pip install -e .
python -m playwright install chromium
```

中文：如果你在虚拟环境里安装过，先确认当前 PowerShell 已经激活同一个虚拟环境。  
English: If you installed dependencies inside a virtual environment, make sure the current PowerShell session is using that same environment.

### 4. 企业微信没有收到消息 / WeCom Notification Did Not Arrive

中文：先别怀疑采集逻辑，先测 webhook。  
English: Test the webhook first before debugging the fetch logic.

```powershell
. .\scripts\set_env.ps1
python scripts\send_test_notification.py --config config/sources.real.sample.json --kind lead
```

中文：同时确认配置里 `notifications.wecom.enabled` 是 `true`，并且 webhook 没有填错。  
English: Also confirm `notifications.wecom.enabled` is `true` and the webhook value is correct.

## 仓库结构 / Repository Layout

```text
config/    runnable configs and source definitions
data/      SQLite databases generated by runs
exports/   CSV exports generated by runs
logs/      runtime logs and page debug artifacts
radar/     core Python package
samples/   local sample payloads for offline validation
scripts/   PowerShell helpers and notification test script
SOURCE_BACKLOG.md  source expansion backlog and status
```

中文：如果你第一次接手，只需要先理解 `config/`、`radar/`、`scripts/` 这三个目录。  
English: If you are inheriting the project for the first time, focus on `config/`, `radar/`, and `scripts/` first.

## 给新维护者的验收清单 / Handoff Checklist for New Maintainers

- 中文：能成功运行 `config/sources.sample.json`。  
  English: You can run `config/sources.sample.json` successfully.
- 中文：`python -m playwright install chromium` 已执行。  
  English: `python -m playwright install chromium` has been executed.
- 中文：`scripts/set_env.ps1` 已按本地环境填写。  
  English: `scripts/set_env.ps1` has been filled for the local environment.
- 中文：`scripts/send_test_notification.py` 能发出至少一条测试通知。  
  English: `scripts/send_test_notification.py` can send at least one test notification.
- 中文：至少能跑通一个真实配置，并知道去哪里看日志和导出文件。  
  English: At least one real config can run, and you know where to inspect logs and exports.

## 合规边界 / Compliance Boundary

中文：这个仓库的定位是需求观察与公开信号整理，不是灰黑产自动化工具。请只采集你有权访问的页面和账号上下文，不要用于盗号、撞库、绕过支付、隐私窃取或其他违法用途。  
English: This repository is intended for demand observation and public-signal aggregation, not for abusive automation. Only collect pages and account contexts you are authorized to access, and do not use it for account theft, credential stuffing, payment bypass, privacy theft, or other illegal activity.

中文：仓库里虽然有风险关键词的负向打分规则，但那只是筛选逻辑，不是法律意见，也不是合规保证。  
English: The repository includes negative scoring rules for risky keywords, but those are only filtering heuristics, not legal advice or a compliance guarantee.

## 后续扩展 / Extending the Project

中文：如果你想继续加新来源，先看 `SOURCE_BACKLOG.md`。这个文件把新来源扩展拆成了“骨架、抓取、调优、完成”几个阶段。  
English: If you want to add new sources, start with `SOURCE_BACKLOG.md`. It breaks source expansion into skeleton, fetcher, tuning, and done stages.

中文：建议先补样例 payload 和 config skeleton，再做真实抓取，不要反过来。  
English: The recommended order is to add a sample payload and config skeleton first, then implement the real fetcher.

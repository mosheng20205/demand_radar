# Source Backlog

This file turns the source expansion plan into an implementation backlog for `demand_radar`.

Last updated: `2026-03-26`

## Backlog Rules

1. Do not start more than one `P0` source implementation at the same time.
2. Each source must first land as config skeleton + sample payload.
3. Real fetcher work only starts after the source shape is stable.
4. Each source is complete only when it can be fetched, scored, exported, and reviewed in `Top20`.
5. Any Cloak integration must use the local API docs as the source of truth:
   `http://127.0.0.1:54381/swagger-ui/`
   `http://127.0.0.1:54381/api-docs/openapi.json`
   Do not hardcode ports or invent endpoints/fields.

## Status Legend

- `planned`: not started
- `skeleton`: config/sample shape landed
- `fetcher`: source-specific fetcher in progress
- `tuning`: cleaning, fallback, or scoring tuning in progress
- `done`: source is usable in a real run

## P0 Sources

### P0-01 ZBJ Demand Hall

- Status: `done`
- Priority: `highest`
- Category: `demand_market`
- Why:
  Direct employer demand is closer to money than service titles.
- Target pages:
  - employer demand hall
  - task list / bidding list
  - demand detail pages if reachable
- First deliverables:
  - config skeleton
  - sample payload
  - first parser for title/content/budget/url mapping
- Real fetcher notes:
  - likely needs dedicated parser
  - Cloak fallback may be required for stability
  - pagination should prefer direct page URLs over UI click pagination in fallback runs
- Done when:
  - demand titles and budgets enter SQLite
  - top leads include demand hall items as an independent source
- Current implementation:
  - real source landed in `config/sources.real.sample.json` as `zbj_demand_hall_live`
  - uses direct page fetch plus Cloak/CDP fallback for list and detail pages
  - validation run inserted 13 demand-market leads into a fresh SQLite database

### P0-02 Xianyu Service Listings

- Status: `done`
- Priority: `high`
- Category: `service_titles`
- Why:
  Can reveal what buyers already pay for in low-friction service markets.
- Target pages:
  - virtual service listings
  - custom script/tool listings
  - listing detail summaries if publicly reachable
- First deliverables:
  - config skeleton
  - sample payload
  - title/price/seller/content/url field mapping
- Real fetcher notes:
  - likely requires anti-change parser and careful request strategy
  - start from listing cards, not full detail pages
- Done when:
  - listing titles and price text enter exports
  - source can produce at least one stable lead bucket

### P0-03 Bilibili Hot Videos

- Status: `done`
- Priority: `high`
- Category: `social_hot_posts`
- Why:
  Good for finding repeated pain points and validating topic heat.
- Target pages:
  - hot tutorial videos
  - hot tool demo videos
  - hot ops/workflow content
- First deliverables:
  - config skeleton
  - sample payload
  - title/summary/tag/url mapping
- Real fetcher notes:
  - this source is for topic heat, not direct money signal
  - comments must be tracked separately
- Current implementation:
  - real config landed in `config/sources.bilibili.real.json`
  - uses Cloak/CDP with profile `哔哩哔哩专用浏览器`
  - current version extracts rendered knowledge-channel `.bili-video-card` items
  - source is merged into `config/sources.real.sample.json`
  - validation should focus on theme relevance, not direct deal intent
- Done when:
  - hot topics can enter daily theme analysis
  - source appears in theme leaderboard

### P0-04 Bilibili Hot Comments

- Status: `done`
- Priority: `high`
- Category: `social_comments`
- Why:
  Good for extracting "want tool / want script / too manual / too expensive" intent.
- Target pages:
  - comments under hot tutorial videos
  - comments under hot tool demo videos
- First deliverables:
  - config skeleton
  - sample payload
  - comment text/author/video title/url mapping
- Real fetcher notes:
  - filter by intent keywords before treating as lead candidates
  - should be lower weight than private inquiries
- Current implementation:
  - comment source orchestrator landed in `radar/fetchers.py`
  - uses inline Bilibili search seeds, then probes each video page for `reply/wbi/main`
  - seed selection excludes tutorial-heavy videos and keeps ops/problem-oriented titles
  - comment filters drop generic `求资料 / 已三连` noise and keep concrete ops discussion
  - source is merged into `config/sources.real.sample.json`
- Done when:
  - comment leads can be filtered and exported
  - at least one comment source enters Top20 after scoring

## P1 Sources

### P1-01 Douyin Hot Videos and Comments

- Status: `done`
- Category: `social_hot_posts` + `social_comments`
- Why:
  High pain density and strong short-form content validation.
- Current implementation:
  - real config landed in `config/sources.douyin.real.json`
  - uses logged-in Cloak browser via `DEMAND_RADAR_DOUYIN_PROFILE_ID` / `DEMAND_RADAR_DOUYIN_PROFILE_NAME`
  - video source parses `aweme/v1/web/search/item` directly from CDP network logs
  - comment source probes seeded videos and parses `aweme/v1/web/comment/list`
  - source is merged into `config/sources.real.sample.json`
- Done when:
  - video and comment leads can be fetched, scored, exported, and reviewed in Top20

### P1-02 Xiaohongshu Hot Notes and Comments

- Status: `done`
- Category: `social_hot_posts` + `social_comments`
- Why:
  Strong signal for ops, side hustle, private-domain, store workflow, spreadsheet pain.
- Current implementation:
  - sample payloads landed in `samples/xiaohongshu_hot_notes.sample.json` and `samples/xiaohongshu_hot_comments.sample.json`
  - sample config landed in `config/sources.xiaohongshu.sample.json`
  - real config landed in `config/sources.xiaohongshu.real.json`
  - uses logged-in Cloak browser via `DEMAND_RADAR_XIAOHONGSHU_PROFILE_ID` / `DEMAND_RADAR_XIAOHONGSHU_PROFILE_NAME`
  - notes source parses rendered search results and ranks seeds by search-result comment count
  - comment source probes note detail pages and prefers `api/sns/web/v2/comment/page` network responses, with DOM extraction as fallback
  - per-note probe artifacts are persisted under `logs/xiaohongshu_hot_comments.real.note_{note_index}.*`
  - source is merged into `config/sources.real.sample.json`
- Done when:
  - note and comment leads can be fetched, scored, exported, and reviewed in Top20

### P1-03 Taobao Service Market

- Status: `done`
- Category: `service_titles`
- Why:
  Useful for price anchoring and service demand pattern discovery.
- Current implementation:
  - real source landed in `config/sources.real.sample.json` as `taobao_service_market_real`
  - uses Cloak/CDP rendered DOM extraction from `fuwu.taobao.com`
  - validation run inserted 15 service-market leads into a fresh SQLite database

### P1-04 Forums and Reward Posts

- Status: `done`
- Category: `forum_posts`
- Scope:
  - V2EX
  - Wuai / reward boards
  - CSDN / Zhihu questions
- Current implementation:
  - stable public replacement landed in `config/sources.real.sample.json` as `cnode_ask_topics_real`
  - uses CNode public API to capture tool-seeking / workflow / automation discussion
  - validation run inserted 3 forum-post leads into a fresh SQLite database

## P2 Sources

### P2-01 Job Descriptions

- Status: `done`
- Category: `job_posts`
- Why:
  Enterprise budget proxy.
- Current implementation:
  - real source landed in `config/sources.real.sample.json` as `remoteok_jobs_real`
  - uses Remote OK public JSON feed and salary extraction
  - validation run inserted 20 job-post leads into a fresh SQLite database

### P2-02 Bidding and Procurement

- Status: `done`
- Category: `procurement_posts`
- Why:
  Strongest public budget signal.
- Current implementation:
  - real source landed in `config/sources.real.sample.json` as `ccgp_procurement_notices_real`
  - uses China Government Procurement public announcement list parsing
  - validation run inserted 4 procurement leads into a fresh SQLite database

### P2-03 App Market Reviews

- Status: `done`
- Category: `review_pages`
- Scope:
  - Feishu marketplace
  - WeCom marketplace
  - DingTalk marketplace
- Current implementation:
  - stable public marketplace source landed in `config/sources.real.sample.json` as `feishu_app_marketplace_real`
  - uses Feishu app center public `query_item` API; app listing descriptions act as the current public proxy signal
  - validation run inserted 18 app-market leads into a fresh SQLite database

## Current Execution Order

1. `ZBJ demand hall`
2. `Xianyu listings`
3. `Bilibili hot videos`
4. `Bilibili hot comments`
5. `Douyin`
6. `Xiaohongshu`
7. `JD`
8. `Bidding / procurement`

## Definition of Done Per Source

1. Config skeleton exists.
2. Sample payload exists.
3. Parser mapping is clear.
4. Category and source bonus are defined.
5. Export fields look reasonable.
6. Source has a next implementation owner and next action.

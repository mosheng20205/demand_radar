from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path

from radar.models import Lead


def _default_theme_rules() -> dict[str, list[str]]:
    return {
        "流程自动化与集成": ["自动", "批量", "脚本", "工具", "workflow", "automation", "integration", "效率", "monitor", "alert"],
        "高级定制与开发支持": ["custom", "programming", "developer", "api", "documentation", "advanced", "代码", "开发", "脚本"],
        "定价与ROI优化": ["pricing", "price", "expensive", "预算", "monthly", "annual", "roi", "成本", "付费"],
        "稳定性与告警监控": ["error", "delay", "late", "duplicate", "troubleshoot", "unstable", "support", "报警", "异常", "监控"],
        "CRM与销售流程": ["crm", "lead", "pipeline", "客户", "销售", "线索", "跟进", "商机"],
        "电商运营自动化": ["店铺", "订单", "库存", "售后", "商品", "上架", "商家", "日报", "对账", "客服", "电商"],
    }


_OPPORTUNITY_PATTERNS: list[tuple[str, list[str]]] = [
    ("高级自动化仍需代码支持", ["custom automation", "require programming", "require code", "advanced functions", "developer", "documentation", "api"]),
    ("自动化任务定价和ROI不透明", ["pricing", "expensive", "price", "monthly", "annual", "roi", "cost can quickly add up", "budget"]),
    ("自动化执行故障排查困难", ["errors", "troubleshoot", "delay", "missing features", "late response", "bad configuration", "duplicate entries", "difficult to predict"]),
    ("移动端和实时告警能力不足", ["mobile", "ios", "android", "notify", "late", "notification", "on-the-go"]),
    ("企业支持和实施服务存在缺口", ["support", "enterprise plan", "late response", "customer support", "expert programmer"]),
]


_PRODUCT_DIRECTION_CATALOG: list[dict[str, object]] = [
    {
        "direction": "复杂自动化定制交付台",
        "mapped_themes": ["高级自动化仍需代码支持", "流程自动化与集成", "高级定制与开发支持"],
        "match_terms": ["custom", "developer", "api", "documentation", "advanced", "integration", "脚本", "开发", "自动化"],
        "core_offer": "把分散的 Excel、ERP、CRM、表单、邮件流程做成可交付的自动化项目。",
        "mvp_features": [
            "任务编排器：支持定时执行、条件分支、批量处理",
            "连接器层：Excel、MySQL、HTTP API、企业微信机器人",
            "失败重试：失败重跑、异常截图、人工接管入口",
            "交付模板库：日报汇总、订单同步、客户分发、对账归档",
            "项目后台：任务日志、执行次数、交付配置导出",
        ],
        "pricing_range": "￥8,000-30,000 / 项目，或 ￥2,000-6,000 / 月托管",
        "demo_video_title": "3 天交付一个企业自动化项目：从 Excel 到 API 全流程跑通",
    },
    {
        "direction": "自动化监控重试中心",
        "mapped_themes": ["自动化执行故障排查困难", "稳定性与告警监控"],
        "match_terms": ["error", "troubleshoot", "delay", "duplicate", "unstable", "alert", "异常", "监控", "告警"],
        "core_offer": "给现成脚本、RPA、接口任务加上监控、重试、日志和告警，直接卖“稳定性”。",
        "mvp_features": [
            "任务健康面板：成功率、平均耗时、近 24 小时失败数",
            "失败重试策略：自动重试、退避重试、人工确认后重跑",
            "告警中心：企业微信、飞书、邮件三通道推送",
            "异常归因：超时、权限、接口返回异常、重复执行识别",
            "值班日报：失败来源、恢复时间、待修问题汇总",
        ],
        "pricing_range": "￥5,000-15,000 / 部署，或 ￥1,500-4,000 / 月 SaaS",
        "demo_video_title": "把无人值守脚本变成可运营系统：失败重试 + 企业微信告警实战",
    },
    {
        "direction": "自动化 ROI 与成本仪表盘",
        "mapped_themes": ["自动化任务定价和ROI不透明", "定价与ROI优化"],
        "match_terms": ["pricing", "price", "roi", "budget", "monthly", "annual", "成本", "预算", "付费"],
        "core_offer": "把任务量、人工时、调用成本、节省时长算清楚，让客户愿意持续付费。",
        "mvp_features": [
            "成本模型：人工成本、API 调用费、服务器费、错误损失",
            "ROI 看板：节省工时、回本周期、月度收益趋势",
            "任务计费器：按执行次数、按成功任务、按部门分摊",
            "客户报告：周报、月报、老板版一页纸导出",
            "方案对比：人工流程 vs 自动化流程收益测算",
        ],
        "pricing_range": "￥6,000-18,000 / 项目，或 ￥999-2,999 / 月订阅",
        "demo_video_title": "为什么客户愿意续费自动化系统：ROI 仪表盘现场演示",
    },
    {
        "direction": "移动告警与老板看板",
        "mapped_themes": ["移动端和实时告警能力不足"],
        "match_terms": ["mobile", "ios", "android", "notify", "notification", "实时", "提醒", "老板"],
        "core_offer": "把后台任务状态、关键订单、异常波动推到手机端，卖“老板随时可看”。",
        "mvp_features": [
            "移动告警模版：任务失败、库存告急、对账异常、线索到达",
            "老板看板：今日订单、待处理异常、自动化节省时长",
            "订阅规则：按客户、项目、来源、异常级别过滤",
            "日报摘要：每天固定时间推送关键指标",
            "轻量移动入口：H5 页面或企业微信图文跳转",
        ],
        "pricing_range": "￥3,000-10,000 / 项目，或 ￥499-1,999 / 月订阅",
        "demo_video_title": "老板手机上直接看自动化状态：移动告警看板 5 分钟演示",
    },
    {
        "direction": "企业实施陪跑服务包",
        "mapped_themes": ["企业支持和实施服务存在缺口"],
        "match_terms": ["support", "enterprise", "implementation", "customer support", "实施", "顾问", "培训"],
        "core_offer": "不是只卖脚本，而是卖需求梳理、部署上线、培训交接、持续优化服务。",
        "mvp_features": [
            "实施清单：需求访谈、流程拆解、权限核对、上线计划",
            "部署支持：环境初始化、任务迁移、账号权限校验",
            "培训材料：录屏 SOP、交付文档、常见问题手册",
            "周会复盘：问题回顾、性能优化、增购机会识别",
            "售后工单：故障登记、处理 SLA、升级建议记录",
        ],
        "pricing_range": "￥8,000-25,000 / 首期实施，或 ￥2,000-8,000 / 月顾问",
        "demo_video_title": "从接单到续费：自动化项目实施陪跑服务怎么卖",
    },
]


def derive_opportunity_title(lead: Lead) -> str:
    text = f"{lead.title}\n{lead.content}".lower()
    best_label = lead.title
    best_score = 0
    for label, patterns in _OPPORTUNITY_PATTERNS:
        score = sum(1 for pattern in patterns if pattern in text)
        if score > best_score:
            best_score = score
            best_label = label
    if best_score > 0:
        return best_label
    return lead.title


def assign_themes(leads: list[Lead], theme_rules: dict[str, list[str]] | None = None) -> dict[str, list[Lead]]:
    rules = theme_rules or _default_theme_rules()
    theme_map: dict[str, list[Lead]] = defaultdict(list)
    for lead in leads:
        text = f"{lead.title}\n{lead.content}".lower()
        matched = False
        for theme, keywords in rules.items():
            if any(keyword.lower() in text for keyword in keywords):
                theme_map[theme].append(lead)
                matched = True
        if not matched:
            theme_map["其他线索"].append(lead)
    return dict(theme_map)


def build_theme_leaderboard(leads: list[Lead], theme_rules: dict[str, list[str]] | None = None) -> list[dict]:
    theme_map = assign_themes(leads, theme_rules)
    leaderboard: list[dict] = []
    for theme, items in theme_map.items():
        total_score = sum(item.score for item in items)
        max_score = max(item.score for item in items) if items else 0
        source_count = len({item.source_name for item in items})
        top_titles = " | ".join(derive_opportunity_title(item) for item in sorted(items, key=lambda x: x.score, reverse=True)[:3])
        leaderboard.append(
            {
                "theme": theme,
                "lead_count": len(items),
                "total_score": total_score,
                "max_score": max_score,
                "source_count": source_count,
                "top_titles": top_titles,
            }
        )
    leaderboard.sort(key=lambda item: (item["total_score"], item["lead_count"], item["source_count"]), reverse=True)
    return leaderboard


def _product_match(lead: Lead, product: dict[str, object]) -> bool:
    text = f"{derive_opportunity_title(lead)}\n{lead.title}\n{lead.content}".lower()
    mapped_themes = [str(item).lower() for item in product.get("mapped_themes", [])]
    match_terms = [str(item).lower() for item in product.get("match_terms", [])]
    return any(term in text for term in mapped_themes + match_terms)


def build_product_directions(leads: list[Lead], limit: int = 5) -> list[dict]:
    rows: list[dict] = []
    for product in _PRODUCT_DIRECTION_CATALOG[:limit]:
        matched_leads = [lead for lead in leads if _product_match(lead, product)]
        evidence_titles = " | ".join(
            derive_opportunity_title(lead) for lead in sorted(matched_leads, key=lambda item: item.score, reverse=True)[:3]
        )
        rows.append(
            {
                "direction": str(product["direction"]),
                "core_offer": str(product["core_offer"]),
                "lead_count": len(matched_leads),
                "total_score": sum(lead.score for lead in matched_leads),
                "source_count": len({lead.source_name for lead in matched_leads}),
                "evidence_titles": evidence_titles,
                "mvp_features": "；".join(str(item) for item in product["mvp_features"]),
                "pricing_range": str(product["pricing_range"]),
                "demo_video_title": str(product["demo_video_title"]),
            }
        )
    rows.sort(key=lambda item: (item["total_score"], item["lead_count"], item["source_count"]), reverse=True)
    return rows


def export_theme_leaderboard(path: str | Path, leaderboard: list[dict]) -> int:
    export_path = Path(path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["theme", "lead_count", "total_score", "max_score", "source_count", "top_titles"])
        for item in leaderboard:
            writer.writerow(
                [
                    item["theme"],
                    item["lead_count"],
                    item["total_score"],
                    item["max_score"],
                    item["source_count"],
                    item["top_titles"],
                ]
            )
    return len(leaderboard)


def export_product_directions(path: str | Path, rows: list[dict]) -> int:
    export_path = Path(path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "direction",
                "core_offer",
                "lead_count",
                "total_score",
                "source_count",
                "evidence_titles",
                "mvp_features",
                "pricing_range",
                "demo_video_title",
            ]
        )
        for item in rows:
            writer.writerow(
                [
                    item["direction"],
                    item["core_offer"],
                    item["lead_count"],
                    item["total_score"],
                    item["source_count"],
                    item["evidence_titles"],
                    item["mvp_features"],
                    item["pricing_range"],
                    item["demo_video_title"],
                ]
            )
    return len(rows)


def select_top_leads(leads: list[Lead], limit: int = 20) -> list[Lead]:
    filtered = []
    noise_patterns = [r"^good\b", r"^helpful\b", r"^excellent\b", r"^nice\b"]
    for lead in leads:
        title = lead.title.strip().lower()
        text = f"{lead.title}\n{lead.content}".lower()
        if any(re.search(pattern, title) for pattern in noise_patterns) and not any(
            token in text for token in ["error", "pricing", "support", "developer", "api", "mobile", "duplicate", "delay"]
        ):
            continue
        filtered.append(lead)
    return sorted(
        filtered,
        key=lambda item: (
            item.opportunity_strength,
            item.score,
            max(item.cumulative_sale_count, item.sale_count),
            item.comment_count,
            item.price_value,
            len(item.matched_rules),
            len(item.content),
        ),
        reverse=True,
    )[:limit]


def export_top_leads(path: str | Path, leads: list[Lead]) -> int:
    export_path = Path(path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "source_name",
                "category",
                "opportunity_title",
                "original_title",
                "content",
                "url",
                "score",
                "opportunity_strength",
                "priority",
                "price_text",
                "price_value",
                "sale_count",
                "cumulative_sale_count",
                "comment_count",
                "good_comment_count",
                "delivery_text",
                "matched_rules",
            ]
        )
        for lead in leads:
            writer.writerow(
                [
                    lead.source_name,
                    lead.category,
                    derive_opportunity_title(lead),
                    lead.title,
                    lead.content,
                    lead.url,
                    lead.score,
                    lead.opportunity_strength,
                    lead.priority,
                    lead.price_text,
                    lead.price_value,
                    lead.sale_count,
                    lead.cumulative_sale_count,
                    lead.comment_count,
                    lead.good_comment_count,
                    lead.delivery_text,
                    ",".join(lead.matched_rules),
                ]
            )
    return len(leads)

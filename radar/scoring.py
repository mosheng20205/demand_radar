from __future__ import annotations

import math

from radar.models import Lead


def _priority_from_score(score: int) -> str:
    if score >= 80:
        return "P1"
    if score >= 60:
        return "P2"
    return "P3"


def _opportunity_strength(lead: Lead) -> int:
    sales = max(lead.cumulative_sale_count, lead.sale_count)
    comments = max(lead.comment_count, lead.good_comment_count)
    price = float(lead.price_value or 0.0)

    keyword_component = min(40, max(0, lead.score))
    sales_component = min(30, int(math.log10(sales + 1) * 16))
    comments_component = min(20, int(math.log10(comments + 1) * 14))
    good_comment_component = min(10, int(math.log10(lead.good_comment_count + 1) * 10))

    if price >= 10000:
        price_component = 20
    elif price >= 5000:
        price_component = 16
    elif price >= 2000:
        price_component = 12
    elif price >= 800:
        price_component = 8
    elif price > 0:
        price_component = 4
    else:
        price_component = 0

    return keyword_component + sales_component + comments_component + good_comment_component + price_component


def score_lead(lead: Lead, config: dict) -> Lead:
    text = f"{lead.title}\n{lead.content}".lower()
    matched: list[str] = []
    score = 0

    for rule_name, rule in config.get("keyword_rules", {}).items():
        keywords = rule.get("keywords", [])
        hits = [keyword for keyword in keywords if keyword.lower() in text]
        if hits:
            score += int(rule.get("score", 0))
            matched.append(f"{rule_name}:{'|'.join(hits)}")

    score += int(config.get("source_score_bonus", {}).get(lead.category, 0))
    lead.score = score
    lead.opportunity_strength = _opportunity_strength(lead)
    lead.priority = _priority_from_score(score)
    lead.matched_rules = matched
    return lead

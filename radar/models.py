from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class Lead:
    source_name: str
    source_kind: str
    category: str
    title: str
    content: str
    url: str
    published_at: str = ""
    score: int = 0
    opportunity_strength: int = 0
    priority: str = "P3"
    price_text: str = ""
    price_value: float = 0.0
    sale_count: int = 0
    cumulative_sale_count: int = 0
    comment_count: int = 0
    good_comment_count: int = 0
    delivery_text: str = ""
    matched_rules: list[str] = field(default_factory=list)

    @property
    def fingerprint_text(self) -> str:
        return f"{self.source_name}|{self.title}|{self.url}".strip().lower()

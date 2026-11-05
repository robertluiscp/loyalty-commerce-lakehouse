"""Contrato das fontes: entidades, formatos, colunas esperadas e chaves de negócio.

A Bronze lê TODAS as colunas como texto (StringType) para preservar o valor original;
a conversão de tipos acontece somente na Silver, onde falhas viram motivos de rejeição.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceEntity:
    name: str                 # nome lógico (também usado como pasta no landing)
    file_format: str          # "csv" ou "json"
    columns: tuple[str, ...]  # colunas esperadas, na ordem do arquivo
    business_key: str         # chave de negócio única na Silver
    kind: str                 # "dimension" ou "fact"
    source_system: str

    @property
    def file_name(self) -> str:
        return f"{self.name}.{'jsonl' if self.file_format == 'json' else 'csv'}"


CUSTOMERS = SourceEntity(
    "customers", "csv",
    ("customer_id", "registration_date", "customer_segment", "region",
     "preferred_channel", "is_active", "created_at"),
    "customer_id", "dimension", "CRM",
)
PARTNERS = SourceEntity(
    "partners", "csv",
    ("partner_id", "partner_name", "partner_category", "region", "partner_status",
     "onboarding_date"),
    "partner_id", "dimension", "PARTNER_PORTAL",
)
CAMPAIGNS = SourceEntity(
    "campaigns", "csv",
    ("campaign_id", "campaign_name", "campaign_type", "start_date", "end_date",
     "campaign_status", "target_segment", "points_multiplier"),
    "campaign_id", "dimension", "MARTECH",
)
CHANNELS = SourceEntity(
    "channels", "csv",
    ("channel_id", "channel_name", "channel_type"),
    "channel_id", "dimension", "MASTER_DATA",
)
REWARDS = SourceEntity(
    "rewards", "csv",
    ("reward_id", "reward_name", "reward_category", "points_cost", "partner_id"),
    "reward_id", "dimension", "REWARDS_CATALOG",
)
PURCHASES = SourceEntity(
    "purchases", "csv",
    ("purchase_id", "customer_id", "partner_id", "channel_id", "purchase_timestamp",
     "gross_amount", "discount_amount", "net_amount", "currency", "purchase_status",
     "status_updated_at", "campaign_id", "source_system"),
    "purchase_id", "fact", "ECOMMERCE",
)
POINTS_TRANSACTIONS = SourceEntity(
    "points_transactions", "json",
    ("points_transaction_id", "purchase_id", "redemption_id", "customer_id",
     "transaction_type", "reason_code", "points_amount", "transaction_timestamp",
     "status", "status_updated_at", "expected_completion_timestamp",
     "actual_completion_timestamp", "expiration_date", "operator_id", "source_system"),
    "points_transaction_id", "fact", "POINTS_ENGINE",
)
REDEMPTIONS = SourceEntity(
    "redemptions", "csv",
    ("redemption_id", "customer_id", "reward_id", "partner_id", "redemption_timestamp",
     "points_used", "redemption_status", "status_updated_at", "completion_timestamp",
     "source_system"),
    "redemption_id", "fact", "REWARDS_PLATFORM",
)
CAMPAIGN_EVENTS = SourceEntity(
    "campaign_events", "json",
    ("event_id", "campaign_id", "customer_id", "event_type", "event_timestamp",
     "channel_id", "purchase_id", "source_system"),
    "event_id", "fact", "MARTECH",
)

# Ordem de processamento: dimensões antes dos fatos; compras e resgates antes de pontos
# (pontos referenciam compras e resgates).
ENTITIES: tuple[SourceEntity, ...] = (
    CHANNELS, PARTNERS, CUSTOMERS, CAMPAIGNS, REWARDS,
    PURCHASES, REDEMPTIONS, POINTS_TRANSACTIONS, CAMPAIGN_EVENTS,
)
ENTITY_BY_NAME = {e.name: e for e in ENTITIES}

# ---- Domínios de valores ------------------------------------------------------
PURCHASE_STATUSES = ("PENDING", "COMPLETED", "CANCELLED", "RETURNED")
PURCHASE_TRANSITIONS = {
    "PENDING": {"COMPLETED", "CANCELLED"},
    "COMPLETED": {"RETURNED"},
    "CANCELLED": set(),
    "RETURNED": set(),
}
POINTS_TYPES = ("CREDIT", "DEBIT", "EXPIRATION", "ADJUSTMENT")
POINTS_STATUSES = ("PENDING", "COMPLETED", "REJECTED", "CANCELLED")
POINTS_TRANSITIONS = {
    "PENDING": {"COMPLETED", "REJECTED", "CANCELLED"},
    "COMPLETED": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
}
# Créditos que não exigem compra associada (exceções documentadas).
CREDIT_REASONS_WITHOUT_PURCHASE = ("WELCOME_BONUS", "CAMPAIGN_BONUS")
REDEMPTION_STATUSES = ("REQUESTED", "COMPLETED", "REJECTED", "CANCELLED")
REDEMPTION_TRANSITIONS = {
    "REQUESTED": {"COMPLETED", "REJECTED", "CANCELLED"},
    "COMPLETED": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
}
CAMPAIGN_EVENT_TYPES = ("EXPOSURE", "CLICK", "CONVERSION", "PURCHASE_ATTRIBUTED")
CUSTOMER_SEGMENTS = ("ENTRY", "REGULAR", "PREMIUM", "VIP")
REGIONS = ("NORTE", "NORDESTE", "CENTRO_OESTE", "SUDESTE", "SUL")

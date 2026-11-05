"""Camada Gold: modelo dimensional (fatos/dimensões) e tabelas de indicadores.

Estratégia: *full refresh* determinístico a partir da Silver a cada execução. No volume do
projeto isso é barato e evita inconsistências; a incrementalidade (MERGE) está na Silver.
Cada tabela declara sua granularidade em ``GOLD_TABLES`` (também em docs/data_dictionary.md).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from .business_metrics import (attribute_purchases, classify_credit_sla, customer_balances, effective_flag,
                               purchase_measures, safe_rate, signed_points)
from .governance import GovernanceRecorder
from .reconciliation import POINTS_CHECKS
from .silver import QUARANTINE_TABLE
from .utils import Lakehouse

GOLD_TABLES = {
    "dim_date": "uma linha por data do período analisado",
    "dim_customer": "uma linha por cliente",
    "dim_partner": "uma linha por parceiro",
    "dim_campaign": "uma linha por campanha",
    "dim_channel": "uma linha por canal",
    "dim_reward": "uma linha por recompensa",
    "fact_purchases": "uma linha por compra válida (purchase_id)",
    "fact_points_transactions": "uma linha por movimentação de pontos válida",
    "fact_redemptions": "uma linha por solicitação de resgate válida",
    "fact_campaign_events": "uma linha por evento de campanha válido",
    "kpi_daily_sales": "data da compra x parceiro x canal",
    "kpi_monthly_sales": "mês da compra",
    "kpi_partner_monthly": "mês x parceiro",
    "kpi_segment_monthly": "mês x segmento de cliente",
    "kpi_points_daily": "data da movimentação x tipo x status",
    "kpi_credit_sla_daily": "data do crédito x categoria de prazo",
    "kpi_points_summary": "uma linha (data de referência)",
    "kpi_redemptions_daily": "data do resgate x categoria da recompensa x status",
    "kpi_redemption_summary": "uma linha (data de referência)",
    "kpi_campaign_performance": "uma linha por campanha",
    "kpi_campaign_segment_channel": "campanha x segmento x canal de exposição",
    "customer_activity_monthly": "cliente x mês com alguma atividade",
    "kpi_active_customers_monthly": "mês",
    "kpi_cohort_retention": "mês de coorte x meses desde a primeira compra",
    "customer_snapshot": "uma linha por cliente (data de referência)",
    "operational_backlog": "tipo de pendência x entidade",
    "kpi_quality_summary": "categoria de verificação (execução atual)",
}


def _month(col) -> F.Column:
    return F.to_date(F.date_trunc("month", col))


SILVER_INPUTS = ("customers", "partners", "campaigns", "channels", "rewards", "purchases",
                 "points_transactions", "redemptions", "campaign_events", QUARANTINE_TABLE)


def build_gold(lh: Lakehouse, gov: GovernanceRecorder) -> dict[str, int]:
    """Lê a Silver e a governança da execução atual, calcula e grava todas as tabelas Gold."""
    inputs = {n: lh.read(lh.table("silver", n)) for n in SILVER_INPUTS}
    inputs["reconciliation_audit"] = (lh.read(lh.table("governance", "reconciliation_audit"))
                                      .where(F.col("run_id") == gov.run_id))
    inputs["data_quality_results"] = (lh.read(lh.table("governance", "data_quality_results"))
                                      .where(F.col("run_id") == gov.run_id))
    out = compute_gold(lh.settings, inputs, gov.run_id)
    rows: dict[str, int] = {}
    for name, df in out.items():
        with gov.step("gold_build", entity=name) as info:
            ref_t = lh.table("gold", name)
            lh.write(df, ref_t, mode="overwrite", overwrite_schema=True)
            rows[name] = lh.read(ref_t).count()
            info["rows_written"] = rows[name]
            info["message"] = f"grão: {GOLD_TABLES[name]}"
    return rows


def compute_gold(settings, inputs: dict[str, DataFrame], run_id: str) -> dict[str, DataFrame]:
    """Calcula as tabelas Gold a partir de DataFrames (sem I/O) - testável em memória."""
    ref_ts = settings.reference_timestamp
    tz = settings.rules["timezone"]
    ref = F.lit(ref_ts).cast("timestamp")
    ref_date = F.lit(ref_ts.date())
    s = inputs.__getitem__
    customers, partners, campaigns = s("customers"), s("partners"), s("campaigns")
    channels, rewards = s("channels"), s("rewards")
    purchases, points, redemptions, events = s("purchases"), s("points_transactions"), s("redemptions"), s("campaign_events")
    audit = s("reconciliation_audit")
    quarantine = s(QUARANTINE_TABLE)
    biz = lambda df: df.select(*[c for c in df.columns if not c.startswith("_")])
    out: dict[str, DataFrame] = {}

    # ---------------- Dimensões ----------------
    bounds = purchases.agg(F.min("purchase_date").alias("d0"), F.max("purchase_date").alias("d1"))
    out["dim_date"] = (bounds.select(F.explode(F.sequence("d0", "d1")).alias("date"))
                       .select("date", F.year("date").alias("year"), F.month("date").alias("month"),
                               _month(F.col("date")).alias("month_start"), F.weekofyear("date").alias("iso_week"),
                               F.dayofweek("date").alias("day_of_week"),
                               F.dayofweek("date").isin(1, 7).alias("is_weekend")))
    out["dim_customer"] = biz(customers)
    out["dim_partner"] = biz(partners)
    out["dim_campaign"] = biz(campaigns)
    out["dim_channel"] = biz(channels)
    out["dim_reward"] = biz(rewards)

    # ---------------- Fatos ----------------
    attr = attribute_purchases(purchases, events, campaigns, int(settings.rules["attribution_window_days"]))
    fp = (purchase_measures(biz(purchases)).join(attr, "purchase_id", "left")
          .withColumn("attribution_status", F.coalesce("attribution_status", F.lit("NO_CAMPAIGN"))))
    out["fact_purchases"] = fp
    pts = classify_credit_sla(effective_flag(signed_points(biz(points)), ref_ts), ref_ts)
    pts = (pts.withColumn("movement_date", F.to_date(F.from_utc_timestamp("transaction_timestamp", tz)))
              .withColumn("effective_date", F.to_date(F.from_utc_timestamp("actual_completion_timestamp", tz))))
    out["fact_points_transactions"] = pts
    debit_div = (audit.where(F.col("check_name") == "REDEMPTION_DEBIT_MISMATCH")
                 .select(F.col("business_key").alias("redemption_id"), F.lit(True).alias("has_debit_divergence")))
    fr = (biz(redemptions).join(debit_div, "redemption_id", "left")
          .withColumn("has_debit_divergence", F.coalesce("has_debit_divergence", F.lit(False)))
          .withColumn("redemption_date", F.to_date(F.from_utc_timestamp("redemption_timestamp", tz)))
          .withColumn("completion_hours", F.when(F.col("redemption_status") == "COMPLETED", F.round(
              (F.unix_timestamp("completion_timestamp") - F.unix_timestamp("redemption_timestamp")) / 3600.0, 2))))
    out["fact_redemptions"] = fr
    out["fact_campaign_events"] = biz(events)

    # ---------------- Compras ----------------
    p_cat = F.broadcast(partners.select("partner_id", "partner_category"))
    sales_aggs = [
        F.count("*").alias("purchases_total"),
        F.sum(F.col("is_sold").cast("int")).alias("completed_purchases"),
        F.sum(F.col("is_realized").cast("int")).alias("realized_purchases"),
        F.sum(F.col("is_returned").cast("int")).alias("returned_purchases"),
        F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_purchases"),
        F.sum(F.col("is_pending").cast("int")).alias("pending_purchases"),
        F.sum("gross_sales").alias("gross_sales"), F.sum("discounts").alias("discounts"),
        F.sum("net_sales").alias("net_sales"), F.sum("returns_amount").alias("returns_amount"),
        F.sum("realized_revenue").alias("realized_revenue"),
        F.countDistinct(F.when(F.col("is_sold"), F.col("customer_id"))).alias("buyers"),
    ]
    out["kpi_daily_sales"] = (fp.groupBy("purchase_date", "partner_id", "channel_id").agg(*sales_aggs)
                              .join(p_cat, "partner_id", "left"))
    monthly = fp.groupBy(_month(F.col("purchase_date")).alias("month")).agg(*sales_aggs)
    w_m = Window.orderBy("month")  # poucas linhas (meses): partição única é aceitável aqui
    out["kpi_monthly_sales"] = (monthly
        .withColumn("avg_ticket", F.round(safe_rate(F.col("net_sales"), F.col("completed_purchases")), 2))
        .withColumn("prev_month_net_sales", F.lag("net_sales").over(w_m))
        .withColumn("mom_net_sales_change_pct", F.round(100 * safe_rate(
            F.col("net_sales") - F.col("prev_month_net_sales"), F.col("prev_month_net_sales")), 2)))
    credits_by_purchase = (pts.where((F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE")
                                     & F.col("affects_balance"))
                           .groupBy("purchase_id").agg(F.sum("points_amount").alias("points_credited")))
    pm = (fp.join(credits_by_purchase, "purchase_id", "left")
            .groupBy(_month(F.col("purchase_date")).alias("month"), "partner_id")
            .agg(*sales_aggs, F.sum("points_credited").alias("points_credited")))
    w_share = Window.partitionBy("month")
    out["kpi_partner_monthly"] = (pm.join(p_cat, "partner_id", "left")
        .withColumn("avg_ticket", F.round(safe_rate(F.col("net_sales"), F.col("completed_purchases")), 2))
        .withColumn("share_of_net_sales_pct", F.round(100 * safe_rate(
            F.col("net_sales"), F.sum("net_sales").over(w_share)), 2)))
    seg = F.broadcast(customers.select("customer_id", "customer_segment"))
    out["kpi_segment_monthly"] = (fp.join(seg, "customer_id", "left")
        .groupBy(_month(F.col("purchase_date")).alias("month"), "customer_segment").agg(*sales_aggs)
        .withColumn("avg_ticket", F.round(safe_rate(F.col("net_sales"), F.col("completed_purchases")), 2))
        .withColumn("purchase_frequency", F.round(safe_rate(F.col("completed_purchases"), F.col("buyers")), 3)))

    # ---------------- Pontos ----------------
    out["kpi_points_daily"] = (pts.groupBy("movement_date", "transaction_type", "status")
        .agg(F.count("*").alias("movements"), F.sum(F.abs("points_amount")).alias("points_abs"),
             F.sum(F.when(F.col("affects_balance"), F.col("signed_points")).otherwise(0)).alias("signed_points_effective")))
    credits = pts.where(F.col("transaction_type") == "CREDIT")
    out["kpi_credit_sla_daily"] = (credits.groupBy(F.col("movement_date").alias("credit_date"), "sla_category")
        .agg(F.count("*").alias("credits"), F.sum("points_amount").alias("points"),
             F.round(F.avg("completion_hours"), 2).alias("avg_completion_hours")))
    balances = customer_balances(biz(points), ref_ts)
    sla = credits.agg(*[F.sum((F.col("sla_category") == c).cast("int")).alias(c.lower()) for c in
                        ("COMPLETED_ON_TIME", "COMPLETED_LATE", "PENDING_WITHIN_SLA", "PENDING_OVERDUE", "NOT_APPLICABLE")])
    bal = balances.agg(
        F.sum("points_credited").alias("points_credited"), F.sum("points_debited").alias("points_debited"),
        F.sum("points_expired").alias("points_expired"), F.sum("points_adjusted").alias("points_adjusted"),
        F.sum("points_balance").alias("points_balance"), F.sum("pending_credit_points").alias("pending_credit_points"),
        F.sum(F.col("has_negative_balance").cast("int")).alias("customers_negative_balance"),
        F.sum((F.col("points_balance") > 0).cast("int")).alias("customers_with_balance"))
    # Registros distintos: o mesmo crédito duplicado pode ser rejeitado em mais de um lote.
    dup = quarantine.where(F.array_contains("reasons", "DUPLICATE_CREDIT")).agg(
        F.countDistinct("business_key").alias("duplicate_credits_quarantined"))
    div = (audit.where(F.col("check_name").isin(*[c for c in POINTS_CHECKS if c != "NEGATIVE_BALANCE"])
                       & (F.col("status") == "DIVERGENT")).agg(F.count("*").alias("points_divergences")))
    sold_n = purchases.where(F.col("purchase_status").isin("COMPLETED", "RETURNED")).agg(F.count("*").alias("sold"))
    red_n = redemptions.where(F.col("redemption_status") == "COMPLETED").agg(F.count("*").alias("red_completed"))
    nonsold_credits = (points.where((F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE"))
                       .join(purchases.where(~F.col("purchase_status").isin("COMPLETED", "RETURNED"))
                             .select("purchase_id"), "purchase_id")
                       .agg(F.count("*").alias("nonsold_credits")))
    out["kpi_points_summary"] = (sla.crossJoin(bal).crossJoin(dup).crossJoin(div).crossJoin(sold_n)
        .crossJoin(red_n).crossJoin(nonsold_credits)
        .withColumn("reference_timestamp", ref)
        .withColumn("on_time_credit_rate", F.round(safe_rate(
            F.col("completed_on_time"), F.col("completed_on_time") + F.col("completed_late")), 4))
        .withColumn("points_inconsistency_rate", F.round(safe_rate(
            F.col("points_divergences"), F.col("sold") + F.col("red_completed") + F.col("nonsold_credits")), 5))
        .withColumnRenamed("sold", "audited_sold_purchases")
        .withColumnRenamed("red_completed", "audited_completed_redemptions")
        .withColumnRenamed("nonsold_credits", "audited_credits_for_unsold_purchases"))

    # ---------------- Resgates ----------------
    rw = F.broadcast(rewards.select("reward_id", "reward_category"))
    out["kpi_redemptions_daily"] = (fr.join(rw, "reward_id", "left")
        .groupBy("redemption_date", "reward_category", "redemption_status")
        .agg(F.count("*").alias("redemptions"), F.sum("points_used").alias("points_requested"),
             F.sum(F.when(F.col("redemption_status") == "COMPLETED", F.col("points_used")).otherwise(0)).alias("points_used_completed"),
             F.round(F.avg("completion_hours"), 2).alias("avg_completion_hours"),
             F.sum(F.col("has_debit_divergence").cast("int")).alias("debit_divergences")))
    st = lambda v: F.sum((F.col("redemption_status") == v).cast("int"))
    out["kpi_redemption_summary"] = (fr.agg(
            F.count("*").alias("requested"), st("COMPLETED").alias("completed"), st("REJECTED").alias("rejected"),
            st("CANCELLED").alias("cancelled"), st("REQUESTED").alias("pending"),
            F.sum(F.when(F.col("redemption_status") == "COMPLETED", F.col("points_used")).otherwise(0)).alias("points_used_completed"),
            F.round(F.avg("completion_hours"), 2).alias("avg_completion_hours"),
            F.sum(F.col("has_debit_divergence").cast("int")).alias("debit_divergences"))
        .withColumn("completion_rate", F.round(safe_rate(
            F.col("completed"), F.col("completed") + F.col("rejected") + F.col("cancelled")), 4))
        .withColumn("reference_timestamp", ref))

    # ---------------- Campanhas ----------------
    ev = events
    exposed = ev.where(F.col("event_type") == "EXPOSURE")
    attributed = fp.where(F.col("attribution_status") == "ATTRIBUTED")
    per_campaign = [
        exposed.groupBy("campaign_id").agg(F.countDistinct("customer_id").alias("exposed_customers"),
                                           F.count("*").alias("exposures")),
        ev.where(F.col("event_type") == "CLICK").groupBy("campaign_id").agg(
            F.countDistinct("customer_id").alias("clicked_customers")),
        attributed.groupBy(F.col("attributed_campaign_id").alias("campaign_id")).agg(
            F.count("*").alias("attributed_purchases"), F.countDistinct("customer_id").alias("converted_customers"),
            F.sum("net_sales").alias("attributed_net_sales"), F.sum("realized_revenue").alias("attributed_realized_revenue")),
        fp.where(F.col("attribution_status") == "TAGGED_WITHOUT_EXPOSURE").groupBy("campaign_id").agg(
            F.count("*").alias("tagged_without_exposure")),
        ev.where(F.col("event_type") == "CONVERSION").groupBy("campaign_id").agg(
            F.count("*").alias("tracking_conversion_events"),
            F.countDistinct("purchase_id").alias("tracking_unique_conversions")),
    ]
    cp = biz(campaigns)
    for part in per_campaign:
        cp = cp.join(part, "campaign_id", "left")
    count_cols = ["exposed_customers", "exposures", "clicked_customers", "attributed_purchases",
                  "converted_customers", "tagged_without_exposure", "tracking_conversion_events",
                  "tracking_unique_conversions"]
    out["kpi_campaign_performance"] = (cp.fillna(0, subset=count_cols)
        .withColumn("click_rate", F.round(safe_rate(F.col("clicked_customers"), F.col("exposed_customers")), 4))
        .withColumn("conversion_rate", F.round(safe_rate(F.col("converted_customers"), F.col("exposed_customers")), 4))
        .withColumn("tracking_duplicate_conversions", F.col("tracking_conversion_events") - F.col("tracking_unique_conversions"))
        .withColumn("roi", F.lit(None).cast("double"))
        .withColumn("roi_note", F.lit("ROI não calculado: não há dados de custo da campanha")))
    conv_cust = attributed.select(F.col("attributed_campaign_id").alias("campaign_id"), "customer_id").distinct() \
        .withColumn("converted", F.lit(True))
    out["kpi_campaign_segment_channel"] = (exposed.select("campaign_id", "customer_id", "channel_id").distinct()
        .join(seg, "customer_id", "left").join(conv_cust, ["campaign_id", "customer_id"], "left")
        .groupBy("campaign_id", "customer_segment", F.col("channel_id").alias("exposure_channel_id"))
        .agg(F.countDistinct("customer_id").alias("exposed_customers"),
             F.countDistinct(F.when(F.col("converted"), F.col("customer_id"))).alias("converted_customers"))
        .withColumn("conversion_rate", F.round(safe_rate(F.col("converted_customers"), F.col("exposed_customers")), 4)))

    # ---------------- Clientes ----------------
    sold = fp.where("is_sold")
    act_p = sold.groupBy("customer_id", _month(F.col("purchase_date")).alias("month")).agg(
        F.count("*").alias("sold_purchases"), F.countDistinct("purchase_date").alias("purchase_days"),
        F.sum("net_sales").alias("net_sales"))
    act_c = (pts.where((F.col("transaction_type") == "CREDIT") & F.col("affects_balance"))
             .groupBy("customer_id", _month(F.col("effective_date")).alias("month")).agg(F.count("*").alias("credits")))
    act_r = fr.groupBy("customer_id", _month(F.col("redemption_date")).alias("month")).agg(F.count("*").alias("redemption_requests"))
    act_e = (ev.where(F.col("event_type").isin("CLICK", "CONVERSION"))
             .groupBy("customer_id", _month(F.col("event_date")).alias("month")).agg(F.count("*").alias("campaign_interactions")))
    activity = act_p
    for part in (act_c, act_r, act_e):
        activity = activity.join(part, ["customer_id", "month"], "full")
    activity = (activity.fillna(0, subset=["sold_purchases", "purchase_days", "credits", "redemption_requests",
                                           "campaign_interactions"])
        .withColumn("is_active_buyer", F.col("sold_purchases") > 0)
        .withColumn("is_active_points", F.col("credits") > 0)
        .withColumn("is_active_redeemer", F.col("redemption_requests") > 0)
        .withColumn("is_active_campaign", F.col("campaign_interactions") > 0)
        .withColumn("is_recurring_buyer", (F.col("sold_purchases") >= 2) & (F.col("purchase_days") >= 2)))
    out["customer_activity_monthly"] = activity
    first = sold.groupBy("customer_id").agg(_month(F.min("purchase_date")).alias("cohort_month"))
    cnt = lambda c: F.sum(F.col(c).cast("int"))
    out["kpi_active_customers_monthly"] = (activity.join(first, "customer_id", "left").groupBy("month").agg(
            cnt("is_active_buyer").alias("active_buyers"), cnt("is_active_points").alias("active_points_earners"),
            cnt("is_active_redeemer").alias("active_redeemers"), cnt("is_active_campaign").alias("active_campaign_interactors"),
            F.count("*").alias("active_any_activity"), cnt("is_recurring_buyer").alias("recurring_buyers"),
            F.sum("sold_purchases").alias("sold_purchases"),
            F.sum((F.col("cohort_month") == F.col("month")).cast("int")).alias("new_buyers"))
        .withColumn("recurring_rate", F.round(safe_rate(F.col("recurring_buyers"), F.col("active_buyers")), 4))
        .withColumn("purchase_frequency", F.round(safe_rate(F.col("sold_purchases"), F.col("active_buyers")), 3)))
    buyers_m = act_p.select("customer_id", "month").join(first, "customer_id")
    cohort = (buyers_m.withColumn("months_since_first", F.months_between("month", "cohort_month").cast("int"))
              .groupBy("cohort_month", "months_since_first").agg(F.countDistinct("customer_id").alias("active_customers")))
    size = first.groupBy("cohort_month").agg(F.count("*").alias("cohort_size"))
    out["kpi_cohort_retention"] = (cohort.join(size, "cohort_month")
        .withColumn("retention_rate", F.round(safe_rate(F.col("active_customers"), F.col("cohort_size")), 4)))
    window_days = int(settings.rules["inactivity_window_days"])
    life = sold.groupBy("customer_id").agg(F.count("*").alias("lifetime_sold_purchases"),
                                           F.sum("net_sales").alias("lifetime_net_sales"),
                                           F.max("purchase_date").alias("last_purchase_date"))
    snap = (biz(customers).join(balances, "customer_id", "left").join(life, "customer_id", "left")
            .fillna(0, subset=["points_credited", "points_debited", "points_expired", "points_adjusted",
                               "points_balance", "pending_credit_points", "lifetime_sold_purchases"])
            .withColumn("has_negative_balance", F.coalesce("has_negative_balance", F.lit(False)))
            .withColumn("days_since_last_purchase", F.datediff(ref_date, F.col("last_purchase_date")))
            .withColumn("inactivity_status", F.when(F.col("last_purchase_date").isNull(), "NO_PURCHASE")
                        .when(F.col("days_since_last_purchase") > window_days, f"INACTIVE_{window_days}D")
                        .otherwise("ACTIVE_WINDOW"))
            .withColumn("reference_timestamp", ref))
    out["customer_snapshot"] = snap

    # ---------------- Pendências operacionais ----------------
    backlog = [
        credits.where(F.col("sla_category").isin("PENDING_WITHIN_SLA", "PENDING_OVERDUE")).groupBy(
            F.lit("POINTS").alias("issue_group"), F.col("sla_category").alias("issue_type"),
            F.lit("points_transactions").alias("entity")).agg(
            F.count("*").alias("open_items"), F.sum("points_amount").alias("points_at_stake"),
            F.min("transaction_timestamp").alias("oldest_item_at")),
        fr.where(F.col("redemption_status") == "REQUESTED").groupBy(
            F.lit("REDEMPTIONS").alias("issue_group"), F.lit("REDEMPTION_PENDING").alias("issue_type"),
            F.lit("redemptions").alias("entity")).agg(
            F.count("*").alias("open_items"), F.sum("points_used").alias("points_at_stake"),
            F.min("redemption_timestamp").alias("oldest_item_at")),
        quarantine.groupBy(F.lit("DATA_QUALITY").alias("issue_group"), F.col("primary_reason").alias("issue_type"),
                           "entity").agg(F.count("*").alias("open_items"),
                                         F.lit(None).cast("bigint").alias("points_at_stake"),
                                         F.min("_ingestion_timestamp").alias("oldest_item_at")),
        audit.where(F.col("status") == "DIVERGENT").groupBy(
            F.lit("RECONCILIATION").alias("issue_group"), F.col("check_name").alias("issue_type"), "entity").agg(
            F.count("*").alias("open_items"),
            F.sum(F.abs("difference")).cast("bigint").alias("points_at_stake"),
            F.min("audited_at").alias("oldest_item_at")),
    ]
    bl = backlog[0]
    for part in backlog[1:]:
        bl = bl.unionByName(part)
    out["operational_backlog"] = bl.withColumn("reference_timestamp", ref)

    # ---------------- Qualidade ----------------
    dq = s("data_quality_results")
    by_cat = dq.where(F.col("passed").isNotNull()).groupBy("check_category").agg(
        F.count("*").alias("checks"), F.sum(F.col("passed").cast("int")).alias("checks_passed"))
    total = dq.where(F.col("passed").isNotNull()).agg(
        F.lit("ALL").alias("check_category"), F.count("*").alias("checks"),
        F.sum(F.col("passed").cast("int")).alias("checks_passed"))
    out["kpi_quality_summary"] = (by_cat.unionByName(total)
        .withColumn("pct_checks_passed", F.round(100 * safe_rate(F.col("checks_passed"), F.col("checks")), 2))
        .withColumn("run_id", F.lit(run_id)))
    return out

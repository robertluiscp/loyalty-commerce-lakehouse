"""Regras de negócio reutilizáveis (funções puras sobre DataFrames).

Cada função recebe DataFrames da Silver e devolve colunas/DataFrames derivados. Nada aqui
grava tabelas: isso facilita testes determinísticos com DataFrames pequenos em memória.
Definições completas: docs/business_rules.md e docs/metric_definitions.md.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, Window, functions as F

# ---------------------------------------------------------------------------
# Compras
# ---------------------------------------------------------------------------
SOLD_STATUSES = ("COMPLETED", "RETURNED")   # vendas concluídas (houve venda)
REALIZED_STATUSES = ("COMPLETED",)          # venda concluída e não devolvida


def purchase_measures(purchases: DataFrame) -> DataFrame:
    """Acrescenta medidas financeiras com a política documentada.

    * ``gross_sales``/``discounts``/``net_sales``: somente vendas concluídas (COMPLETED + RETURNED);
    * ``returns_amount``: valor líquido das devolvidas (RETURNED);
    * ``realized_revenue``: líquido das COMPLETED (= net_sales - returns_amount);
    * canceladas e pendentes não entram em nenhuma medida financeira.
    """
    sold = F.col("purchase_status").isin(*SOLD_STATUSES)
    zero = F.lit(0).cast("decimal(12,2)")
    return (purchases
            .withColumn("is_sold", sold)
            .withColumn("is_realized", F.col("purchase_status").isin(*REALIZED_STATUSES))
            .withColumn("is_returned", F.col("purchase_status") == "RETURNED")
            .withColumn("is_cancelled", F.col("purchase_status") == "CANCELLED")
            .withColumn("is_pending", F.col("purchase_status") == "PENDING")
            .withColumn("gross_sales", F.when(sold, F.col("gross_amount")).otherwise(zero))
            .withColumn("discounts", F.when(sold, F.col("discount_amount")).otherwise(zero))
            .withColumn("net_sales", F.when(sold, F.col("net_amount")).otherwise(zero))
            .withColumn("returns_amount", F.when(F.col("is_returned"), F.col("net_amount")).otherwise(zero))
            .withColumn("realized_revenue", F.when(F.col("is_realized"), F.col("net_amount")).otherwise(zero)))


def safe_rate(numerator: Column, denominator: Column) -> Column:
    """Razão com denominador explícito; retorna NULL (não 0) quando o denominador é 0."""
    return F.when(denominator > 0, numerator.cast("double") / denominator.cast("double"))


# ---------------------------------------------------------------------------
# Pontos
# ---------------------------------------------------------------------------
def signed_points(df: DataFrame) -> DataFrame:
    """Sinal por tipo: CREDIT +, DEBIT -, EXPIRATION -, ADJUSTMENT = valor com sinal da origem."""
    amount = F.col("points_amount")
    return df.withColumn("signed_points", F.when(F.col("transaction_type") == "CREDIT", amount)
                         .when(F.col("transaction_type").isin("DEBIT", "EXPIRATION"), -amount)
                         .when(F.col("transaction_type") == "ADJUSTMENT", amount)
                         .otherwise(F.lit(0)).cast("bigint"))


def effective_flag(df: DataFrame, reference_ts: datetime) -> DataFrame:
    """Uma movimentação afeta o saldo somente se COMPLETED e concluída até a data de referência."""
    ref = F.lit(reference_ts).cast("timestamp")
    return df.withColumn("affects_balance", (F.col("status") == "COMPLETED")
                         & F.col("actual_completion_timestamp").isNotNull()
                         & (F.col("actual_completion_timestamp") <= ref))


def classify_credit_sla(df: DataFrame, reference_ts: datetime) -> DataFrame:
    """Classifica créditos quanto ao prazo, na data de referência.

    * COMPLETED_ON_TIME: concluído até a referência e ``actual <= expected``;
    * COMPLETED_LATE: concluído até a referência e ``actual > expected``;
    * PENDING_WITHIN_SLA: ainda não concluído na referência e ``referência <= expected``;
    * PENDING_OVERDUE: ainda não concluído na referência e ``referência > expected``;
    * NOT_APPLICABLE: REJECTED/CANCELLED; NULL para movimentações que não são créditos.
    Um crédito pendente nunca é contado como concluído (no prazo ou fora dele).
    """
    ref = F.lit(reference_ts).cast("timestamp")
    actual, expected = F.col("actual_completion_timestamp"), F.col("expected_completion_timestamp")
    completed_by_ref = (F.col("status") == "COMPLETED") & actual.isNotNull() & (actual <= ref)
    open_at_ref = (F.col("status") == "PENDING") | ((F.col("status") == "COMPLETED") & (actual > ref))
    category = (F.when(F.col("transaction_type") != "CREDIT", F.lit(None).cast("string"))
                 .when(F.col("status").isin("REJECTED", "CANCELLED"), "NOT_APPLICABLE")
                 .when(completed_by_ref & (actual <= expected), "COMPLETED_ON_TIME")
                 .when(completed_by_ref, "COMPLETED_LATE")
                 .when(open_at_ref & (ref <= expected), "PENDING_WITHIN_SLA")
                 .when(open_at_ref, "PENDING_OVERDUE"))
    hours = (F.unix_timestamp(actual) - F.unix_timestamp(F.col("transaction_timestamp"))) / 3600.0
    return (df.withColumn("sla_category", category)
              .withColumn("completion_hours", F.when(completed_by_ref, F.round(hours, 2))))


def customer_balances(points: DataFrame, reference_ts: datetime) -> DataFrame:
    """Saldo por cliente na data de referência + menor saldo acumulado (detecção de negativo).

    O saldo é o acumulado de TODAS as movimentações efetivas até a referência (não apenas do
    mês): pontos podem vir de períodos anteriores, expirar ou ser ajustados.
    A window ordenada por cliente/instante gera shuffle por ``customer_id`` (necessário).
    """
    df = effective_flag(signed_points(points), reference_ts)
    eff = df.where("affects_balance")
    w = (Window.partitionBy("customer_id").orderBy("actual_completion_timestamp", "points_transaction_id")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    running = eff.withColumn("running_balance", F.sum("signed_points").over(w))
    pos = lambda t: F.sum(F.when(F.col("transaction_type") == t, F.col("points_amount")).otherwise(0))
    agg = running.groupBy("customer_id").agg(
        pos("CREDIT").alias("points_credited"),
        pos("DEBIT").alias("points_debited"),
        pos("EXPIRATION").alias("points_expired"),
        F.sum(F.when(F.col("transaction_type") == "ADJUSTMENT", F.col("points_amount")).otherwise(0)).alias("points_adjusted"),
        F.sum("signed_points").alias("points_balance"),
        F.min("running_balance").alias("min_running_balance"),
        F.max("actual_completion_timestamp").alias("last_movement_at"),
    )
    # Pendente na referência: PENDING, ou COMPLETED com conclusão posterior à referência.
    pending = (df.where(F.col("status").isin("PENDING", "COMPLETED") & ~F.col("affects_balance")
                        & (F.col("transaction_type") == "CREDIT"))
                 .groupBy("customer_id").agg(F.sum("points_amount").alias("pending_credit_points")))
    return (agg.join(pending, "customer_id", "full")
               .fillna(0, subset=["points_credited", "points_debited", "points_expired", "points_adjusted",
                                  "points_balance", "pending_credit_points"])
               .withColumn("has_negative_balance", F.coalesce(F.col("min_running_balance") < 0, F.lit(False))))


def expected_credit_points(purchases: DataFrame, campaigns: DataFrame, rate: float) -> DataFrame:
    """Pontos esperados por compra: floor(net_amount x taxa x multiplicador da campanha).

    O multiplicador vale quando a compra tem ``campaign_id`` de campanha MULTIPLIER e a data
    da compra está entre ``start_date`` e ``end_date`` (inclusive). Caso contrário, 1.
    """
    c = campaigns.select(F.col("campaign_id").alias("c_id"), "campaign_type", "start_date", "end_date",
                         "points_multiplier")
    j = purchases.join(F.broadcast(c), purchases["campaign_id"] == F.col("c_id"), "left")
    mult = F.when((F.col("campaign_type") == "MULTIPLIER")
                  & F.col("purchase_date").between(F.col("start_date"), F.col("end_date")),
                  F.col("points_multiplier")).otherwise(F.lit(1).cast("decimal(4,2)"))
    return j.withColumn("expected_points",
                        F.floor(F.col("net_amount") * F.lit(rate).cast("decimal(6,4)") * mult).cast("bigint")) \
            .drop("c_id", "campaign_type", "start_date", "end_date", "points_multiplier")


# ---------------------------------------------------------------------------
# Campanhas
# ---------------------------------------------------------------------------
def attribute_purchases(purchases: DataFrame, events: DataFrame, campaigns: DataFrame,
                        window_days: int) -> DataFrame:
    """Regra de atribuição (uma campanha por compra, no máximo):

    Uma compra é ATTRIBUTED à campanha C quando: (1) ``campaign_id = C`` (código aplicado na
    compra); (2) status COMPLETED ou RETURNED; (3) data da compra dentro da vigência de C; e
    (4) o cliente teve EXPOSURE ou CLICK de C nos ``window_days`` dias anteriores à compra.
    Demais compras com código: TAGGED_WITHOUT_EXPOSURE, TAGGED_OUTSIDE_PERIOD ou NOT_SOLD.
    """
    touches = (events.where(F.col("event_type").isin("EXPOSURE", "CLICK"))
               .select(F.col("campaign_id").alias("e_campaign"), F.col("customer_id").alias("e_customer"),
                       F.col("event_timestamp").alias("e_ts")))
    tagged = purchases.where(F.col("campaign_id").isNotNull()).select(
        "purchase_id", "customer_id", "campaign_id", "purchase_timestamp", "purchase_date", "purchase_status")
    window = F.expr(f"INTERVAL {int(window_days)} DAYS")
    cond = ((tagged["campaign_id"] == F.col("e_campaign")) & (tagged["customer_id"] == F.col("e_customer"))
            & (F.col("e_ts") <= tagged["purchase_timestamp"])
            & (F.col("e_ts") >= tagged["purchase_timestamp"] - window))
    # Join por intervalo pode multiplicar linhas (várias exposições): agregamos por compra.
    touched = (tagged.join(touches, cond, "left")
               .groupBy("purchase_id").agg(F.max(F.col("e_ts").isNotNull()).alias("has_touch")))
    c = campaigns.select(F.col("campaign_id").alias("c_id"), "start_date", "end_date")
    out = (tagged.join(touched, "purchase_id").join(F.broadcast(c), tagged["campaign_id"] == F.col("c_id"), "left"))
    status = (F.when(~F.col("purchase_status").isin(*SOLD_STATUSES), "NOT_SOLD")
               .when(~F.col("purchase_date").between(F.col("start_date"), F.col("end_date")), "TAGGED_OUTSIDE_PERIOD")
               .when(~F.col("has_touch"), "TAGGED_WITHOUT_EXPOSURE")
               .otherwise("ATTRIBUTED"))
    return out.select("purchase_id", status.alias("attribution_status"),
                      F.when(status == "ATTRIBUTED", F.col("campaign_id")).alias("attributed_campaign_id"))

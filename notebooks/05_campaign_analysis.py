# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise de campanhas
# MAGIC **Regra de atribuição** (uma campanha por compra, no máximo): a compra tem o código da campanha,
# MAGIC foi vendida (COMPLETED/RETURNED), está na vigência da campanha e o cliente teve EXPOSURE ou CLICK
# MAGIC da mesma campanha nos 7 dias anteriores. Conversão = cliente único com ≥ 1 compra atribuída.
# MAGIC
# MAGIC **Não calculamos ROI** (não há custos confiáveis) e **não afirmamos causalidade**: comparar
# MAGIC expostos x não expostos ou antes x depois não isola o efeito da campanha sem um grupo de controle.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("05_campaign_analysis.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

g = lambda name: lh.read(lh.table("gold", name))  # noqa: E731
show(g("kpi_campaign_performance").select(
    "campaign_id", "campaign_name", "campaign_type", "target_segment", "exposed_customers", "clicked_customers",
    "converted_customers", "conversion_rate", "attributed_purchases", "attributed_net_sales",
    "tagged_without_exposure", "tracking_conversion_events", "tracking_duplicate_conversions", "roi_note")
     .orderBy(F.desc("conversion_rate")))

# COMMAND ----------

# MAGIC %md Status de atribuição das compras com código de campanha:

# COMMAND ----------

show(g("fact_purchases").where("campaign_id IS NOT NULL").groupBy("attribution_status").count())

# COMMAND ----------

# MAGIC %md Conversão por segmento e canal de exposição (clientes expostos por vários canais aparecem em
# MAGIC cada linha: **não some** `exposed_customers` entre canais).

# COMMAND ----------

show(g("kpi_campaign_segment_channel").orderBy("campaign_id", "customer_segment"), 60)

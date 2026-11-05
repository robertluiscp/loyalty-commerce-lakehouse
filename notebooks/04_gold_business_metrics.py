# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: modelo dimensional e KPIs
# MAGIC Executa a pipeline completa (Bronze -> Silver -> reconciliação -> qualidade -> Gold) e mostra
# MAGIC os principais indicadores. A Gold é recalculada (full refresh) a partir da Silver.
# MAGIC Definições das métricas: `docs/metric_definitions.md`.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("04_gold_business_metrics.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

from loyalty_lakehouse.pipeline import run_pipeline  # noqa: E402

result = run_pipeline(settings, spark=spark)
print(result.run_id, result.reconciliation)

# COMMAND ----------

g = lambda name: lh.read(lh.table("gold", name))  # noqa: E731
show(g("kpi_monthly_sales").orderBy("month"))
show(g("kpi_points_summary"))
show(g("kpi_redemption_summary"))

# COMMAND ----------

# MAGIC %md Faturamento por categoria de parceiro (medidas aditivas somadas; ticket recalculado):

# COMMAND ----------

spark.sql(open(REPO_ROOT / "sql" / "business_kpis.sql", encoding="utf-8").read()
          .split("-- [Q03]")[1].split(";")[0].split("\n", 1)[1]).show()

# COMMAND ----------

show(g("kpi_active_customers_monthly").orderBy("month"))
show(g("kpi_cohort_retention").orderBy("cohort_month", "months_since_first"), 50)
show(g("operational_backlog").orderBy("issue_group", "issue_type"), 50)

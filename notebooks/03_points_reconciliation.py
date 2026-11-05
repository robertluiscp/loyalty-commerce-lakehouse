# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Reconciliação de pontos
# MAGIC Compara compras, movimentações de pontos e resgates na Silver e grava divergências em
# MAGIC `governance.reconciliation_audit`. Divergências **não** alteram a Silver: viram pendências.
# MAGIC
# MAGIC | Verificação | Regra |
# MAGIC |---|---|
# MAGIC | MISSING_CREDIT | venda concluída (COMPLETED/RETURNED) sem crédito PURCHASE |
# MAGIC | CREDIT_FOR_NON_COMPLETED_PURCHASE | crédito válido para compra cancelada/pendente |
# MAGIC | POINTS_AMOUNT_MISMATCH | pontos != floor(líquido x taxa x multiplicador) |
# MAGIC | RETURN_REVERSAL_MISMATCH | devolução sem estorno integral do crédito concluído |
# MAGIC | REDEMPTION_DEBIT_MISMATCH | resgate concluído sem exatamente 1 débito igual |
# MAGIC | NEGATIVE_BALANCE | saldo acumulado < 0 em algum momento (window por cliente) |

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("03_points_reconciliation.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

from loyalty_lakehouse.business_metrics import customer_balances  # noqa: E402
from loyalty_lakehouse.governance import GovernanceRecorder  # noqa: E402
from loyalty_lakehouse.reconciliation import run_reconciliation  # noqa: E402
from loyalty_lakehouse.utils import new_run_id  # noqa: E402

gov = GovernanceRecorder(lh, new_run_id())
gov.ensure_tables()
print(run_reconciliation(lh, gov))
gov.flush()

# COMMAND ----------

audit = lh.read(lh.table("governance", "reconciliation_audit")).where(F.col("run_id") == gov.run_id)
show(audit.groupBy("check_name", "status").count().orderBy("check_name"))
show(audit.where("check_name = 'NEGATIVE_BALANCE'"))

# COMMAND ----------

# MAGIC %md Saldo por cliente na data de referência (acumulado de todo o histórico, não só do mês):

# COMMAND ----------

show(customer_balances(lh.read(lh.table("silver", "points_transactions")), settings.reference_timestamp)
     .orderBy("min_running_balance"))

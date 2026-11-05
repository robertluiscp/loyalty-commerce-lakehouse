-- =============================================================================
-- Reconciliação compras x pontos x resgates (governance.reconciliation_audit)
-- As mesmas regras são implementadas em PySpark em src/loyalty_lakehouse/reconciliation.py;
-- as consultas [R04]-[R06] reproduzem algumas delas em SQL puro, diretamente na Silver.
-- =============================================================================

-- [R01] Resumo da última execução por verificação
SELECT check_name, status, count(*) AS records, sum(abs(difference)) AS abs_difference
FROM loyalty_lakehouse.governance.reconciliation_audit
WHERE run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.reconciliation_audit)
GROUP BY check_name, status
ORDER BY check_name;

-- [R02] Origem x Bronze: contagens do manifesto x linhas ingeridas
SELECT entity, business_key AS batch_id, expected_value, actual_value, status
FROM loyalty_lakehouse.governance.reconciliation_audit
WHERE check_name = 'SOURCE_TO_BRONZE_ROWCOUNT'
  AND run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.reconciliation_audit)
ORDER BY batch_id, entity;

-- [R03] Clientes com saldo negativo em algum momento (detalhe)
SELECT business_key AS customer_id, actual_value AS min_running_balance, details
FROM loyalty_lakehouse.governance.reconciliation_audit
WHERE check_name = 'NEGATIVE_BALANCE'
  AND run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.reconciliation_audit);

-- [R04] (SQL puro) Créditos de pontos para compras canceladas ou pendentes
SELECT t.points_transaction_id, t.purchase_id, p.purchase_status, t.points_amount
FROM loyalty_lakehouse.silver.points_transactions t
JOIN loyalty_lakehouse.silver.purchases p USING (purchase_id)
WHERE t.transaction_type = 'CREDIT' AND t.reason_code = 'PURCHASE'
  AND t.status NOT IN ('REJECTED', 'CANCELLED')
  AND p.purchase_status IN ('CANCELLED', 'PENDING');

-- [R05] (SQL puro) Saldo acumulado por cliente com window function e menor saldo atingido
WITH eff AS (
  SELECT customer_id, actual_completion_timestamp, points_transaction_id,
         CASE transaction_type WHEN 'CREDIT' THEN points_amount
                               WHEN 'ADJUSTMENT' THEN points_amount
                               ELSE -points_amount END AS signed_points
  FROM loyalty_lakehouse.silver.points_transactions
  WHERE status = 'COMPLETED' AND actual_completion_timestamp <= TIMESTAMP '2025-07-01 03:00:00'),
running AS (
  SELECT *, sum(signed_points) OVER (PARTITION BY customer_id
              ORDER BY actual_completion_timestamp, points_transaction_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM eff)
SELECT customer_id, sum(signed_points) AS balance, min(running_balance) AS min_running_balance
FROM running GROUP BY customer_id
HAVING min(running_balance) < 0;

-- [R06] (SQL puro) Resgates concluídos sem exatamente um débito com os mesmos pontos
SELECT r.redemption_id, r.points_used, count(t.points_transaction_id) AS debits, sum(t.points_amount) AS debited
FROM loyalty_lakehouse.silver.redemptions r
LEFT JOIN loyalty_lakehouse.silver.points_transactions t
  ON t.redemption_id = r.redemption_id AND t.transaction_type = 'DEBIT' AND t.status = 'COMPLETED'
WHERE r.redemption_status = 'COMPLETED'
GROUP BY r.redemption_id, r.points_used
HAVING count(t.points_transaction_id) <> 1 OR sum(t.points_amount) <> r.points_used;

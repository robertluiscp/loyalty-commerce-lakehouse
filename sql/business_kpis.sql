-- =============================================================================
-- KPIs de negócio sobre a camada Gold (Databricks SQL / Spark SQL)
-- Catálogo: loyalty_lakehouse. No modo local, troque `loyalty_lakehouse.gold.X` por
-- delta.`data/lakehouse/gold/X`. Definições: docs/metric_definitions.md
-- =============================================================================

-- [Q01] Visão executiva: vendas e ticket médio por mês, com variação mês contra mês
SELECT month, completed_purchases, gross_sales, discounts, net_sales, returns_amount,
       realized_revenue, avg_ticket, mom_net_sales_change_pct
FROM loyalty_lakehouse.gold.kpi_monthly_sales
ORDER BY month;

-- [Q02] Desempenho por parceiro no último mês completo (share do faturamento líquido)
SELECT p.partner_name, k.partner_category, k.completed_purchases, k.net_sales, k.avg_ticket,
       k.share_of_net_sales_pct, k.points_credited
FROM loyalty_lakehouse.gold.kpi_partner_monthly k
JOIN loyalty_lakehouse.gold.dim_partner p USING (partner_id)
WHERE k.month = (SELECT max(month) FROM loyalty_lakehouse.gold.kpi_partner_monthly)
ORDER BY k.net_sales DESC;

-- [Q03] Desempenho por categoria de parceiro (soma de medidas aditivas; ticket recalculado)
SELECT partner_category, sum(completed_purchases) AS completed_purchases,
       sum(net_sales) AS net_sales,
       round(sum(net_sales) / nullif(sum(completed_purchases), 0), 2) AS avg_ticket
FROM loyalty_lakehouse.gold.kpi_daily_sales
GROUP BY partner_category
ORDER BY net_sales DESC;

-- [Q04] Desempenho por canal (nunca some a coluna `buyers` entre linhas: não é aditiva)
SELECT c.channel_name, sum(k.completed_purchases) AS completed_purchases, sum(k.net_sales) AS net_sales,
       round(sum(k.net_sales) / nullif(sum(k.completed_purchases), 0), 2) AS avg_ticket
FROM loyalty_lakehouse.gold.kpi_daily_sales k
JOIN loyalty_lakehouse.gold.dim_channel c USING (channel_id)
GROUP BY c.channel_name
ORDER BY net_sales DESC;

-- [Q05] Pontos: totais, saldo e pontualidade dos créditos (na data de referência)
SELECT points_credited, points_debited, points_expired, points_adjusted, points_balance,
       pending_credit_points, completed_on_time, completed_late, on_time_credit_rate,
       pending_within_sla, pending_overdue, duplicate_credits_quarantined,
       points_divergences, points_inconsistency_rate, customers_negative_balance
FROM loyalty_lakehouse.gold.kpi_points_summary;

-- [Q06] Distribuição das movimentações por tipo e status
SELECT transaction_type, status, sum(movements) AS movements, sum(points_abs) AS points
FROM loyalty_lakehouse.gold.kpi_points_daily
GROUP BY transaction_type, status
ORDER BY transaction_type, status;

-- [Q07] Créditos por categoria de prazo e mês do crédito
SELECT date_trunc('month', credit_date) AS month, sla_category, sum(credits) AS credits
FROM loyalty_lakehouse.gold.kpi_credit_sla_daily
GROUP BY 1, 2
ORDER BY 1, 2;

-- [Q08] Resgates: conclusão, pontos usados e tempo médio
SELECT requested, completed, rejected, cancelled, pending, completion_rate,
       points_used_completed, avg_completion_hours, debit_divergences
FROM loyalty_lakehouse.gold.kpi_redemption_summary;

-- [Q09] Comparação entre campanhas (clientes únicos; ROI não calculado: sem custos)
SELECT campaign_id, campaign_name, campaign_type, target_segment, exposed_customers,
       clicked_customers, converted_customers, conversion_rate, attributed_purchases,
       attributed_net_sales, tagged_without_exposure, tracking_duplicate_conversions
FROM loyalty_lakehouse.gold.kpi_campaign_performance
ORDER BY conversion_rate DESC NULLS LAST;

-- [Q10] Conversão por segmento e canal de exposição
SELECT campaign_id, customer_segment, exposure_channel_id, exposed_customers, converted_customers,
       conversion_rate
FROM loyalty_lakehouse.gold.kpi_campaign_segment_channel
ORDER BY campaign_id, customer_segment, exposure_channel_id;

-- [Q11] Clientes ativos por tipo de atividade, recorrência e frequência
SELECT month, active_buyers, active_points_earners, active_redeemers, active_campaign_interactors,
       active_any_activity, recurring_buyers, recurring_rate, purchase_frequency, new_buyers
FROM loyalty_lakehouse.gold.kpi_active_customers_monthly
ORDER BY month;

-- [Q12] Retenção por coorte (matriz: linhas = coorte, colunas = meses desde a 1ª compra)
SELECT cohort_month,
       max(CASE WHEN months_since_first = 0 THEN retention_rate END) AS m0,
       max(CASE WHEN months_since_first = 1 THEN retention_rate END) AS m1,
       max(CASE WHEN months_since_first = 2 THEN retention_rate END) AS m2,
       max(CASE WHEN months_since_first = 3 THEN retention_rate END) AS m3,
       max(CASE WHEN months_since_first = 4 THEN retention_rate END) AS m4,
       max(CASE WHEN months_since_first = 5 THEN retention_rate END) AS m5
FROM loyalty_lakehouse.gold.kpi_cohort_retention
GROUP BY cohort_month
ORDER BY cohort_month;

-- [Q13] Ticket médio e frequência por segmento (período completo)
SELECT customer_segment, sum(completed_purchases) AS completed_purchases, sum(net_sales) AS net_sales,
       round(sum(net_sales) / nullif(sum(completed_purchases), 0), 2) AS avg_ticket
FROM loyalty_lakehouse.gold.kpi_segment_monthly
GROUP BY customer_segment
ORDER BY net_sales DESC;

-- [Q14] Clientes inativos pela janela configurada (não é "churn": é ausência de compra)
SELECT inactivity_status, count(*) AS customers, round(avg(points_balance), 1) AS avg_points_balance
FROM loyalty_lakehouse.gold.customer_snapshot
GROUP BY inactivity_status;

-- [Q15] Variação semanal de vendas (exemplo de window function sobre a Gold)
WITH weekly AS (
  SELECT date_trunc('week', purchase_date) AS week, sum(net_sales) AS net_sales
  FROM loyalty_lakehouse.gold.kpi_daily_sales GROUP BY 1)
SELECT week, net_sales,
       round(100 * (net_sales - lag(net_sales) OVER (ORDER BY week)) / lag(net_sales) OVER (ORDER BY week), 2)
         AS wow_change_pct
FROM weekly ORDER BY week;

-- [Q16] Top 2 parceiros por categoria e mês: vendas, ticket, crescimento, participação, campanhas e SLA
WITH vendas AS (
  SELECT f.partner_id, p.partner_name, p.partner_category,
         date_trunc('MONTH', f.purchase_date)                     AS mes,
         count(*)                                                  AS vendas,
         sum(f.net_sales)                                          AS faturamento_liquido,
         count(DISTINCT f.customer_id)                             AS compradores,
         sum(CASE WHEN f.attribution_status = 'ATTRIBUTED' THEN f.net_sales ELSE 0 END) AS faturamento_campanha
  FROM loyalty_lakehouse.gold.fact_purchases f
  JOIN loyalty_lakehouse.gold.dim_partner p ON p.partner_id = f.partner_id
  WHERE f.is_sold
  GROUP BY ALL
),
creditos AS (
  SELECT f.partner_id, date_trunc('MONTH', f.purchase_date) AS mes,
         avg(CASE t.sla_category WHEN 'COMPLETED_ON_TIME' THEN 1.0
                                 WHEN 'COMPLETED_LATE'    THEN 0.0 END) AS taxa_no_prazo
  FROM loyalty_lakehouse.gold.fact_points_transactions t
  JOIN loyalty_lakehouse.gold.fact_purchases f ON f.purchase_id = t.purchase_id
  WHERE t.transaction_type = 'CREDIT' AND t.reason_code = 'PURCHASE'
  GROUP BY ALL
)
SELECT v.mes, v.partner_category, v.partner_name, v.vendas, v.compradores,
       round(v.faturamento_liquido, 2)                                        AS faturamento_liquido,
       round(v.faturamento_liquido / v.vendas, 2)                              AS ticket_medio,
       round(100 * (v.faturamento_liquido - lag(v.faturamento_liquido) OVER w_parceiro)
                 / lag(v.faturamento_liquido) OVER w_parceiro, 1)              AS var_mes_anterior_pct,
       rank() OVER (PARTITION BY v.mes, v.partner_category ORDER BY v.faturamento_liquido DESC) AS ranking_categoria,
       round(100 * v.faturamento_liquido
                 / sum(v.faturamento_liquido) OVER (PARTITION BY v.mes, v.partner_category), 1) AS share_categoria_pct,
       round(100 * v.faturamento_campanha / v.faturamento_liquido, 1)          AS pct_via_campanha,
       round(100 * c.taxa_no_prazo, 1)                                         AS pct_creditos_no_prazo
FROM vendas v
LEFT JOIN creditos c ON c.partner_id = v.partner_id AND c.mes = v.mes
WINDOW w_parceiro AS (PARTITION BY v.partner_id ORDER BY v.mes)
QUALIFY ranking_categoria <= 2
ORDER BY v.mes DESC, v.partner_category, ranking_categoria;

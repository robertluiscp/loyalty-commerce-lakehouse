-- =============================================================================
-- Qualidade de dados e operação do pipeline (governance + silver.quarantine)
-- =============================================================================

-- [DQ01] Última execução: verificações, valores e limites
SELECT entity, batch_id, check_name, check_category, numerator, denominator,
       round(metric_value, 6) AS metric_value, comparator, threshold, passed, severity
FROM loyalty_lakehouse.governance.data_quality_results
WHERE run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.data_quality_results)
ORDER BY passed NULLS FIRST, entity, batch_id, check_name;

-- [DQ02] Percentual de verificações aprovadas (somente as que têm limite)
SELECT check_category, checks, checks_passed, pct_checks_passed
FROM loyalty_lakehouse.gold.kpi_quality_summary
ORDER BY check_category;

-- [DQ03] Registros rejeitados por entidade e motivo principal
SELECT entity, primary_reason, count(*) AS records
FROM loyalty_lakehouse.silver.quarantine
GROUP BY entity, primary_reason
ORDER BY records DESC;

-- [DQ04] Rastreabilidade: um registro rejeitado com valor original e arquivo de origem
SELECT entity, business_key, reasons, raw_record, _source_file, _batch_id, _ingestion_timestamp
FROM loyalty_lakehouse.silver.quarantine
WHERE array_contains(reasons, 'INVALID_TIMESTAMP')
LIMIT 5;

-- [DQ05] Incidentes por categoria e severidade (última execução)
SELECT category, severity, count(*) AS incidents, sum(affected_records) AS affected_records
FROM loyalty_lakehouse.governance.data_quality_incidents
WHERE run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.data_quality_incidents)
GROUP BY category, severity
ORDER BY incidents DESC;

-- [DQ06] Histórico de execuções (duração e volume por etapa)
SELECT run_id, step, count(*) AS tasks, sum(rows_read) AS rows_read, sum(rows_written) AS rows_written,
       sum(rows_quarantined) AS rows_quarantined, round(sum(duration_seconds), 1) AS seconds,
       max(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS had_failure
FROM loyalty_lakehouse.governance.pipeline_execution_log
GROUP BY run_id, step
ORDER BY run_id DESC, step;

-- [DQ07] Taxa de duplicidade e de inválidos por lote (com denominador explícito)
SELECT batch_id, entity,
       max(CASE WHEN check_name = 'duplicate_rate' THEN numerator END) AS duplicates,
       max(CASE WHEN check_name = 'invalid_rate' THEN numerator END) AS invalid,
       max(CASE WHEN check_name = 'duplicate_rate' THEN denominator END) AS rows_in_batch,
       max(CASE WHEN check_name = 'referential_integrity' THEN round(metric_value * 100, 3) END) AS ref_integrity_pct
FROM loyalty_lakehouse.governance.data_quality_results
WHERE run_id = (SELECT max(run_id) FROM loyalty_lakehouse.governance.data_quality_results)
  AND batch_id IS NOT NULL
GROUP BY batch_id, entity
ORDER BY batch_id, entity;

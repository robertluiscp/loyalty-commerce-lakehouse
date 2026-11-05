-- =============================================================================
-- Unity Catalog: organização lógica do Loyalty Commerce Lakehouse
-- -----------------------------------------------------------------------------
-- STATUS DE EXECUÇÃO (Databricks Free Edition):
--   [EXECUTADO]      Seções 1 e 2 (catálogo, schemas e volume) - criados com sucesso.
--   [EXECUTADO]      Seção 4 (consultas de metadados).
--   [NÃO EXECUTADO]  Seção 3 (GRANTs para grupos): o Free Edition não oferece console de
--                    conta/SCIM para criar grupos; os nomes abaixo são PLACEHOLDERS.
--                    Execute num workspace com grupos configurados (ver docs/governance.md).
-- Nada aqui é destrutivo: só CREATE ... IF NOT EXISTS, GRANT e consultas.
-- =============================================================================

-- 1) Catálogo (container de mais alto nível para o projeto) ---------------------
CREATE CATALOG IF NOT EXISTS loyalty_lakehouse
  COMMENT 'Portfolio: loyalty commerce lakehouse (dados 100% sinteticos)';

-- 2) Schemas por camada + volume de landing ----------------------------------
CREATE SCHEMA IF NOT EXISTS loyalty_lakehouse.bronze
  COMMENT 'Dados brutos rastreaveis (texto original + colunas de auditoria)';
CREATE SCHEMA IF NOT EXISTS loyalty_lakehouse.silver
  COMMENT 'Dados tipados, validados e deduplicados; quarentena de rejeitados';
CREATE SCHEMA IF NOT EXISTS loyalty_lakehouse.gold
  COMMENT 'Modelo dimensional e tabelas de indicadores';
CREATE SCHEMA IF NOT EXISTS loyalty_lakehouse.governance
  COMMENT 'Registros operacionais do pipeline (escolha do projeto, nao recurso nativo do UC)';

-- Volume gerenciado: área de arquivos de origem (CSV/JSON) lida pela Bronze.
CREATE VOLUME IF NOT EXISTS loyalty_lakehouse.bronze.landing
  COMMENT 'Arquivos de origem sinteticos (landing zone)';

-- As tabelas são criadas pela própria pipeline (saveAsTable / MERGE) como tabelas Delta
-- GERENCIADAS pelo Unity Catalog. Exemplo equivalente em SQL (não necessário executar):
-- CREATE TABLE IF NOT EXISTS loyalty_lakehouse.governance.pipeline_execution_log (
--   run_id STRING NOT NULL, batch_id STRING, step STRING NOT NULL, entity STRING,
--   status STRING NOT NULL, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP,
--   duration_seconds DOUBLE, rows_read BIGINT, rows_written BIGINT, rows_quarantined BIGINT,
--   message STRING, environment STRING NOT NULL
-- ) USING DELTA;

-- 3) Permissões (menor privilégio) - [NÃO EXECUTADO: placeholders] ------------
-- Grupos sugeridos (crie no console da conta / SCIM):
--   `lakehouse_engineers` : escreve bronze/silver/gold/governance (job da pipeline)
--   `lakehouse_analysts`  : lê somente gold
--   `lakehouse_auditors`  : lê governance e a quarentena da silver
--
-- Engenharia (pipeline):
-- GRANT USE CATALOG ON CATALOG loyalty_lakehouse TO `lakehouse_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA loyalty_lakehouse.bronze     TO `lakehouse_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA loyalty_lakehouse.silver     TO `lakehouse_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA loyalty_lakehouse.gold       TO `lakehouse_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA loyalty_lakehouse.governance TO `lakehouse_engineers`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME loyalty_lakehouse.bronze.landing TO `lakehouse_engineers`;
--
-- Análise (somente leitura da Gold; sem acesso a dados brutos):
-- GRANT USE CATALOG ON CATALOG loyalty_lakehouse TO `lakehouse_analysts`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA loyalty_lakehouse.gold TO `lakehouse_analysts`;
--
-- Auditoria (governança + quarentena, sem escrita):
-- GRANT USE CATALOG ON CATALOG loyalty_lakehouse TO `lakehouse_auditors`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA loyalty_lakehouse.governance TO `lakehouse_auditors`;
-- GRANT USE SCHEMA ON SCHEMA loyalty_lakehouse.silver TO `lakehouse_auditors`;
-- GRANT SELECT ON TABLE loyalty_lakehouse.silver.quarantine TO `lakehouse_auditors`;
--
-- Evite: GRANT ALL PRIVILEGES ... ou conceder MODIFY a `account users`.

-- 4) Metadados e verificação --------------------------------------------------
SHOW SCHEMAS IN loyalty_lakehouse;
SHOW TABLES IN loyalty_lakehouse.gold;
SHOW GRANTS ON CATALOG loyalty_lakehouse;
SELECT table_schema, table_name, table_type, data_source_format
FROM loyalty_lakehouse.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold', 'governance')
ORDER BY table_schema, table_name;

-- Lineage nativo (se as system tables estiverem habilitadas no workspace; pode não estar
-- disponível no Free Edition - ver docs/governance.md):
-- SELECT source_table_full_name, target_table_full_name, event_time
-- FROM system.access.table_lineage
-- WHERE target_table_catalog = 'loyalty_lakehouse'
-- ORDER BY event_time DESC LIMIT 50;

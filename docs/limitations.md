# Limitações, compatibilidade e status de execução

## Versões efetivamente usadas

| Componente | Versão | Onde |
|---|---|---|
| Windows 10 Pro 22H2, Ryzen 5 3400G, 16 GB RAM | — | máquina de desenvolvimento |
| Python | 3.12.10 | os dois ambientes virtuais |
| Java (Eclipse Temurin JDK) | 17.0.20.1 | Spark local |
| pyspark / delta-spark | 4.0.4 / 4.0.1 | ambiente `spark-local` |
| pandas / pyarrow / pytest | 3.1.0 / 26.0.0 / 9.1.1 | ambiente `spark-local` |
| databricks-connect / databricks-sdk | 17.3.15 / 0.157.0 | ambiente `databricks` |
| Databricks CLI | 1.22.1 | autenticação OAuth |
| Databricks Free Edition: serverless | DBR `19.12.x-aarch64-photon-scala2.13` (informado por `current_version()`) | execução remota |

Compatibilidade: Databricks Connect 17.3 exige Python 3.12 e é compatível com serverless
environment v4/v5; versões do Connect **iguais ou anteriores** ao runtime do servidor são
suportadas (documentação: *Databricks Connect usage requirements*). O DBR do servidor (19.x) é
mais novo que o cliente (17.3), o que é a combinação suportada.

## O que foi executado x preparado

| Recurso | Status |
|---|---|
| Gerador de dados, testes Python puros | **Executado** neste PC |
| Regras da Silver, métricas, reconciliação e Gold com Spark local **em memória** | **Executado** neste PC (testes `spark`) |
| Pipeline completa Bronze → Silver → Gold, MERGE, quarentena, governança | **Executado** no Databricks Free Edition (serverless, Unity Catalog) |
| Idempotência (reprocessamento sem duplicar) | **Executado** no Databricks (volume padrão e teste de integração `tests/integration/test_pipeline_databricks.py`) |
| Delta: histórico, time travel, schema enforcement, mergeSchema | **Executado** no Databricks (`docs/results/delta_demo.json`) |
| Benchmark CSV x Parquet | **Executado**: local (pandas/pyarrow) e Spark no Databricks |
| Unity Catalog: catálogo, schemas, volume, tabelas gerenciadas | **Executado** |
| Unity Catalog: GRANTs para grupos | **Preparado, não executado** (sem grupos no Free Edition) |
| Pipeline local com Delta (tabelas por caminho) | **Executado** no GitHub Actions (Linux, [execução nº 1](https://github.com/robertluiscp/loyalty-commerce-lakehouse/actions/runs/42095188100)); não executado no Windows |
| Notebooks no workspace do Databricks | **Preparados** (mesmo código dos módulos); a execução validada foi via Databricks Connect |
| Dashboard | HTML estático gerado a partir das tabelas Gold reais; Metabase/AI-BI: instruções em `dashboard.md` |

## Limitações do ambiente

1. **Spark local no Windows não grava arquivos** sem `winutils.exe`/`hadoop.dll` (erro em `o32.parquet`
   verificado). Esses binários vêm de repositórios de terceiros e não foram instalados. Alternativas:
   WSL2, Linux/macOS, GitHub Actions ou executar no Databricks.
2. **Free Edition:** apenas serverless; cota diária de uso (fair use); sem console de conta, SCIM
   ou grupos; uso não comercial. O `cache()`/`persist()` não é suportado no serverless; por isso o
   estágio da Silver é materializado em `silver.stg_*`.
3. **Latência do Databricks Connect:** cada ação (count, write, MERGE, tableExists) é uma ida e volta
   ao serverless. A pipeline completa leva vários minutos mesmo com poucos dados; o gargalo é a
   latência, não o processamento. Rodar como notebook/job no workspace tende a reduzir isso (não medido).
4. **Pastas sincronizadas (OneDrive e similares):** podem bloquear a remoção de diretórios no Windows,
   e uma exclusão se propaga para a nuvem. Mantenha o repositório fora delas e use o GitHub como cópia.
5. **Testes Spark locais são lentos** (minutos no total, no hardware acima) por causa do planejamento
   de DataFrames com muitas regras numa JVM local de 1 núcleo de execução.

6. **Time travel tem prazo:** no Databricks, ler uma versão além do período de retenção (7 dias por
   padrão) falha com `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION`, o que foi
   observado neste projeto. A versão continua em `DESCRIBE HISTORY`, mas seus dados não são mais
   legíveis. Time travel não substitui backup nem histórico de longo prazo; aqui a origem dos lotes
   fica na Bronze.

## Limitações de modelagem (simplificações documentadas)

* Devoluções são atribuídas à data da venda, não à data da devolução.
* Expirações vêm da origem; a pipeline não recalcula a política de validade (FIFO de lotes de pontos).
* Silver é SCD tipo 1 (estado atual); o histórico de estados fica no Delta (time travel) e na Bronze.
* Gold é full refresh; em volumes maiores, usar MERGE/`replaceWhere` por partição de data.
* Atribuição de campanha é "código aplicado + toque nos 7 dias"; não é modelo multi-touch
  nem medida causal. ROI não é calculado (sem custos).
* Registros em quarentena não são reprocessados automaticamente quando uma dimensão chega
  depois (late-arriving dimension): é preciso reexecutar o lote.
* A data de referência é fixa (`2025-07-01`) para reprodutibilidade; em produção seria a data da execução.
* Os volumes e distribuições são sintéticos e não representam nenhum negócio real.

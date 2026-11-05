# Resultados de execuções reais

Arquivos gerados pelos scripts do projeto (não editados à mão):

| Arquivo | Gerado por | Conteúdo |
|---|---|---|
| `gold_snapshot.json` | `scripts/build_dashboard.py` | principais números da Gold na execução indicada (`run_id`) |
| `ledger_comparison.json` | `scripts/build_dashboard.py` | anomalias injetadas x detectadas, por camada |
| `benchmark_local.json` | `python -m loyalty_lakehouse.benchmark --mode local` | CSV x Parquet com pandas/pyarrow neste PC |
| `benchmark_databricks.json` | `python -m loyalty_lakehouse.benchmark --mode spark --env databricks` | CSV x Parquet x Delta com Spark serverless |
| `delta_demo.json` | `python -m loyalty_lakehouse.delta_features --env databricks` | histórico, time travel, schema enforcement/evolution |
| `sql_checks.json` | `scripts/run_sql_checks.py` | execução de cada consulta de `sql/` |
| `test_report.txt` | `pytest` | saída resumida dos testes |

Regenerar os dados e reexecutar a pipeline produz os mesmos números (seed e data de referência fixas),
exceto tempos de execução e identificadores de execução.

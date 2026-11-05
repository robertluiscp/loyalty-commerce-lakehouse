# data/

| Pasta | Conteúdo | Versionado? |
|---|---|---|
| `raw/` | reservado para arquivos de origem fornecidos manualmente (não usado: tudo é gerado) | só o README |
| `generated/` | saída do gerador: `landing/`, `anomaly_ledger.csv`, `generation_report.json` | só o README |
| `lakehouse/` | tabelas Delta do modo local (criada pela pipeline) | não |
| `benchmark/` | arquivos temporários do benchmark local com Spark | não |

**Todos os dados são sintéticos**, gerados com seed fixa (`config/local.yaml` -> `generator.seed`).
Nenhum dado pessoal, interno ou de empresa real é usado. Para regenerar:

```bash
python scripts/generate_data.py            # volume padrão (~30 mil compras, ~24 MB)
python scripts/generate_data.py --profile small
```

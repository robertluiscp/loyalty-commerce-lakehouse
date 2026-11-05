# data/generated/ (não versionado)

Saída de `scripts/generate_data.py`:

```
landing/
  _manifests/<lote>.json                      # arquivos esperados + linhas por arquivo
  <entidade>/batch_id=<lote>/<entidade>.csv   # customers, partners, campaigns, channels, rewards,
                                              # purchases, redemptions
  <entidade>/batch_id=<lote>/<entidade>.jsonl # points_transactions, campaign_events
anomaly_ledger.csv        # uma linha por anomalia injetada (tipo, registro, lote, detecção esperada)
generation_report.json    # parâmetros, seed, linhas por arquivo, anomalias e cenários
```

Lotes: `B001_20250501` (extração no corte de 01/05/2025) e `B002_20250701` (novos registros e
mudanças de status até 30/06/2025). As anomalias são **artificiais** e servem só para testar a pipeline.

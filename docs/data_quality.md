# Qualidade de dados

## Camadas de controle

| Onde | O que verifica | Resultado |
|---|---|---|
| Bronze | manifesto (arquivo ausente, contagem), colunas ausentes/extras, linhas malformadas | incidentes + `reconciliation_audit` (SOURCE_TO_BRONZE_ROWCOUNT) |
| Silver | tipos, formatos, domínios, valores financeiros, integridade referencial, duplicidade, transições de status | `silver.quarantine` (motivos) + `_corrections` |
| Reconciliação | coerência entre compras, pontos e resgates; saldo negativo | `governance.reconciliation_audit` |
| Avaliação | taxas por entidade/lote com limites; verificações globais | `governance.data_quality_results` + incidentes |

## Catálogo de motivos de quarentena

| Motivo | Entidades | Significado |
|---|---|---|
| MISSING_ID / INVALID_ID_FORMAT | todas | chave ausente ou fora do padrão |
| INVALID_TIMESTAMP / MISSING_TIMESTAMP / INVALID_DATE | todas | data/hora não parseável no formato do contrato (ISO-8601 com fuso) |
| INCONSISTENT_STATUS_TIMESTAMP | purchases | status atualizado antes da compra |
| INVALID_AMOUNT_FORMAT / NEGATIVE_AMOUNT / ZERO_GROSS_AMOUNT / DISCOUNT_EXCEEDS_GROSS / NET_AMOUNT_MISMATCH | purchases | política financeira |
| INVALID_CURRENCY | purchases | moeda diferente de BRL |
| INVALID_STATUS / INVALID_TYPE / INVALID_EVENT_TYPE / INVALID_SEGMENT / INVALID_REGION / INVALID_BOOLEAN | várias | valor fora do domínio |
| UNKNOWN_CUSTOMER / UNKNOWN_PARTNER / UNKNOWN_CHANNEL / UNKNOWN_CAMPAIGN / UNKNOWN_REWARD / UNKNOWN_PURCHASE | fatos | chave estrangeira inexistente |
| ORPHAN_CREDIT / ORPHAN_DEBIT | points | referência a compra/resgate inexistente |
| MISSING_PURCHASE_REF / MISSING_REDEMPTION_REF | points, events | referência obrigatória ausente |
| DUPLICATE_RECORD / DUPLICATE_KEY_CONFLICT | todas | duplicidade no lote |
| DUPLICATE_CREDIT | points | segundo crédito para a mesma compra |
| INVALID_STATUS_TRANSITION / CONFLICTING_VERSION | purchases, points, redemptions | transição proibida / mesma versão com conteúdo diferente |
| IMMUTABLE_RECORD_CHANGED | campaign_events | evento reenviado com conteúdo diferente |
| INVALID_POINTS_AMOUNT / INVALID_POINTS / INVALID_MULTIPLIER | points, rewards, campaigns | valores de pontos fora da regra |
| COMPLETED_WITHOUT_COMPLETION_TS | points, redemptions | concluído sem horário de conclusão |
| MISSING_ADJUSTMENT_REASON / MISSING_OPERATOR | points | ajuste sem rastreabilidade |
| MALFORMED_RECORD | points, events | JSON truncado (preservado em `_corrupt_record` na Bronze e em `raw_record` na quarentena) |

## Verificações e limites

Configuráveis em `config/local.yaml` → `quality_thresholds`.

| Verificação | Escopo | Limite | Severidade |
|---|---|---|---|
| duplicate_rate | entidade × lote | ≤ 1% | ERROR |
| invalid_rate | entidade × lote | ≤ 2% | ERROR |
| referential_integrity | entidade × lote | ≥ 98% | ERROR |
| quarantined_records, corrected_records | entidade × lote | informativo | INFO |
| points_inconsistency_rate | global | ≤ 1% | ERROR |
| negative_balance_customers | global | ≤ 0 | WARNING |
| pending_overdue_credits | global | ≤ 0 | WARNING |
| campaigns_with_duplicate_tracking | global | informativo | INFO |

Com os dados padrão, as verificações de saldo negativo e créditos vencidos **falham de propósito**
(há anomalias injetadas) e abrem incidentes WARNING. É o comportamento esperado: o pipeline não
"esconde" problemas para ficar verde.

## Prova de detecção (ledger x pipeline)

O gerador registra cada anomalia em `anomaly_ledger.csv` com a detecção esperada (por exemplo,
`silver_quarantine:DUPLICATE_CREDIT`, `reconciliation:NEGATIVE_BALANCE`). A função
`quality.compare_with_ledger` compara esperado x detectado, inclusive **falsos positivos**
(quarentena sem anomalia correspondente), contando registros distintos: um mesmo registro
rejeitado em dois lotes conta uma vez. O teste de integração exige 100% de correspondência.
Resultado da execução real: `docs/results/ledger_comparison.json` (resumo no README).

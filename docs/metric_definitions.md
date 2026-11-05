# Definições de métricas

Data de referência fixa: `rules.reference_timestamp` (2025-07-01 00:00 -03:00). "Venda concluída" =
compra COMPLETED ou RETURNED. Taxas retornam **nulo** (não zero) quando o denominador é zero
(`business_metrics.safe_rate`). A coluna "Onde" indica a tabela Gold e a consulta SQL. Os valores
nas colunas de interpretação são apenas exemplos de leitura, não resultados medidos.

## 1. Compras

| Métrica | Objetivo | Fórmula (numerador / denominador) | Grão e dimensões | Exclusões | Limitações | Interpretação (exemplo) | Onde |
|---|---|---|---|---|---|---|---|
| Compras concluídas | volume de vendas | count(compras COMPLETED + RETURNED) | dia × parceiro × canal; mês; segmento | canceladas, pendentes, quarentena | devolução conta na data da venda | "3.000 vendas em março" | kpi_daily_sales, Q01 |
| Faturamento bruto | valor antes de descontos | Σ gross_amount das vendas concluídas | idem | idem | — | base para medir peso dos descontos | Q01 |
| Descontos concedidos | custo comercial direto | Σ discount_amount das vendas concluídas | idem | idem | não inclui custo de pontos | "descontos = 4% do bruto" | Q01 |
| Faturamento líquido | receita comercial do período | Σ (gross − discount) das vendas concluídas | idem | idem | inclui as que depois foram devolvidas | métrica principal de vendas | Q01 |
| Valor realizado | receita que permaneceu | Σ net_amount das COMPLETED (= líquido − devoluções) | idem | devolvidas, canceladas, pendentes | devoluções futuras ainda podem reduzir | diferença para o líquido = devoluções | Q01 |
| Ticket médio | valor médio por venda | faturamento líquido / compras concluídas | mês; parceiro; segmento; canal | idem | recalcular em cada agregação (não fazer média de médias) | "R$ 250 em média por venda" | Q01-Q04, Q13 |
| Variação vs período anterior | tendência | (líquido_m − líquido_{m−1}) / líquido_{m−1} × 100 | mês (Q15: semana) | primeiro mês = nulo | meses incompletos distorcem | "+5,3% vs mês anterior" | kpi_monthly_sales |
| Desempenho por parceiro | concentração e mix | medidas acima + share = líquido_parceiro / líquido_total_mês | mês × parceiro | idem | parceiro inativo pode ter poucas vendas | "top 3 parceiros = 40% do líquido" | kpi_partner_monthly, Q02, Q16 |
| Desempenho por categoria / canal | mix | medidas aditivas somadas por categoria/canal; ticket recalculado | categoria; canal | idem | `buyers` não é aditivo | — | Q03, Q04 |

## 2. Pontos

| Métrica | Fórmula | Denominador / população | Exclusões | Interpretação |
|---|---|---|---|---|
| Pontos creditados | Σ points_amount de CREDIT efetivos (COMPLETED até a referência) | — | pendentes, rejeitados, cancelados, quarentena | passivo gerado pelo programa |
| Pontos debitados | Σ DEBIT efetivos | — | idem | consumo em resgates |
| Pontos expirados | Σ EXPIRATION efetivos | — | idem | eventos vêm da origem |
| Ajustes | Σ ADJUSTMENT efetivos (com sinal) | — | idem | estornos de devolução são negativos |
| Saldo | créditos − débitos − expirados + ajustes, acumulado de todo o histórico até a referência | por cliente; total | idem | não é fluxo do mês |
| % créditos no prazo | COMPLETED_ON_TIME / (COMPLETED_ON_TIME + COMPLETED_LATE) | **apenas créditos concluídos até a referência** | pendentes, rejeitados, cancelados | "92% dos créditos concluídos saíram em até 72 h" |
| Créditos pendentes vencidos | count(PENDING_OVERDUE) | créditos não concluídos na referência | — | fila operacional a tratar |
| Créditos duplicados | count distinto (quarentena com DUPLICATE_CREDIT) | — | — | duplicidades barradas antes do saldo |
| Taxa de inconsistência de pontos | divergências (MISSING_CREDIT + CREDIT_FOR_NON_COMPLETED_PURCHASE + POINTS_AMOUNT_MISMATCH + RETURN_REVERSAL_MISMATCH + REDEMPTION_DEBIT_MISMATCH) / (vendas concluídas + resgates concluídos + créditos de compras não vendidas) | cada divergência pertence a exatamente uma unidade dessa população | NEGATIVE_BALANCE (medido à parte, por cliente) | "0,08% dos eventos auditados divergem" |
| Clientes com saldo negativo | count(clientes com menor saldo acumulado < 0) | clientes com movimentações efetivas | — | indica resgate sem saldo |

Onde: `kpi_points_summary`, `kpi_points_daily`, `kpi_credit_sla_daily`, `customer_snapshot`; SQL Q05-Q07.

## 3. Resgates

| Métrica | Fórmula | Denominador | Exclusões | Onde |
|---|---|---|---|---|
| Resgates solicitados | count(todos os resgates válidos) | — | quarentena | kpi_redemption_summary, Q08 |
| Resgates concluídos | count(status COMPLETED) | — | REJECTED, CANCELLED, REQUESTED | idem |
| Taxa de conclusão | concluídos / (concluídos + rejeitados + cancelados) | **somente finalizados** (pendentes fora) | REQUESTED | idem |
| Pontos usados em resgates concluídos | Σ points_used dos COMPLETED | — | demais status | idem |
| Tempo médio de conclusão | média(completion − redemption) em horas, COMPLETED | resgates concluídos | demais | idem |
| Rejeitados / cancelados | count por status | — | — | kpi_redemptions_daily |
| Resgates com divergência de saldo | count(REDEMPTION_DEBIT_MISMATCH) | resgates concluídos | — | fact_redemptions.has_debit_divergence |

## 4. Campanhas

| Métrica | Fórmula | Observação |
|---|---|---|
| Clientes únicos expostos | countDistinct(customer_id) com EXPOSURE | exposições repetidas contam 1 vez |
| Clientes únicos convertidos | countDistinct(customer_id) com ≥ 1 compra ATTRIBUTED | regra de atribuição em business_rules.md §5 |
| Taxa de conversão | convertidos / expostos | nulo se ninguém foi exposto |
| Compras atribuídas | count(compras ATTRIBUTED) | uma campanha por compra |
| Faturamento atribuído | Σ net_sales das compras ATTRIBUTED (e realizado) | não é efeito causal |
| Por canal e segmento | expostos/convertidos por canal de exposição e segmento | cliente exposto em 2 canais aparece nos 2 (não somar) |
| Comparação entre campanhas | ranking pelas métricas acima | campanhas com vigências/segmentos diferentes não são diretamente comparáveis |
| Rastreamento duplicado | eventos CONVERSION − compras distintas nesses eventos | mede qualidade do tracking |
| ROI | **não calculado** | sem custo confiável; `roi = NULL` + `roi_note` |

Onde: `kpi_campaign_performance`, `kpi_campaign_segment_channel`; SQL Q09-Q10.

## 5. Clientes

| Métrica | Fórmula | Observação |
|---|---|---|
| Clientes ativos (por atividade) | count clientes com ≥ 1 atividade do tipo no mês: venda, crédito efetivo, solicitação de resgate, CLICK/CONVERSION | quatro indicadores separados + `active_any_activity` (união documentada) |
| Clientes recorrentes | ≥ 2 vendas concluídas em ≥ 2 dias distintos no mês | taxa = recorrentes / compradores ativos |
| Frequência de compra | vendas concluídas / compradores ativos | por mês e por segmento |
| Ticket médio por segmento | líquido / vendas concluídas por segmento | Q13 |
| Coorte | mês da primeira venda concluída | coortes recentes têm menos meses observados |
| Retenção por coorte | clientes da coorte com venda no mês k / tamanho da coorte | M0 = 100% por definição |
| Inativos | sem venda concluída nos últimos 90 dias (configurável) | **não** é churn; janela e regra explícitas |

Onde: `kpi_active_customers_monthly`, `kpi_cohort_retention`, `customer_snapshot`; SQL Q11-Q14.

## 6. Qualidade (população elegível explícita)

| Métrica | Numerador | Denominador (população) | Limite |
|---|---|---|---|
| Taxa de duplicidade | linhas do lote em quarentena por DUPLICATE_RECORD / DUPLICATE_KEY_CONFLICT / DUPLICATE_CREDIT | linhas do lote na Bronze da entidade | ≤ 1% |
| Taxa de registros inválidos | linhas em quarentena exceto duplicidades | idem | ≤ 2% |
| Integridade referencial | linhas sem motivo UNKNOWN_* / ORPHAN_* | idem | ≥ 98% |
| Créditos sem compra válida | ORPHAN_CREDIT + MISSING_PURCHASE_REF | — | informativo |
| Saldos inconsistentes | clientes com NEGATIVE_BALANCE | clientes com movimentações | ≤ 0 (gera incidente) |
| Registros rejeitados | linhas na quarentena | — | informativo |
| Incidentes por categoria | count incidentes | execução | — |
| % verificações aprovadas | verificações com `passed = true` | verificações **com limite** (informativas fora) | — |

Onde: `governance.data_quality_results`, `governance.data_quality_incidents`, `kpi_quality_summary`; SQL DQ01-DQ07.

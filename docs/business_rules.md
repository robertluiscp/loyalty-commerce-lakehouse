# Regras de negócio

> Programa de fidelidade **fictício**. As regras abaixo foram definidas para este projeto e não
> reproduzem regras de nenhuma empresa real. Cada regra aponta para o código que a implementa e
> para o teste que a verifica.

## 1. Compras (`silver.purchases`)

| Regra | Implementação | Teste |
|---|---|---|
| `purchase_id` obrigatório, formato `PU` + 8 dígitos, único na Silver | `silver.parse_purchases`, `stage` (window por chave) | `test_invalid_purchases_*`, `test_duplicates_within_batch` |
| Cliente, parceiro, canal e campanha (se houver) devem existir | `flag_exists` (broadcast join) | `UNKNOWN_*` em `test_invalid_purchases_*` |
| `gross_amount >= 0`, `discount_amount >= 0`, `discount <= gross`, `net = gross - discount`, `gross > 0` | `parse_purchases` | `NEGATIVE_AMOUNT`, `DISCOUNT_EXCEEDS_GROSS`, `NET_AMOUNT_MISMATCH` |
| Moeda única: BRL | `INVALID_CURRENCY` | idem |
| Status ∈ {PENDING, COMPLETED, CANCELLED, RETURNED}; códigos são normalizados (trim + maiúsculas) e a correção é registrada | `_status` + `_corrections` | `test_status_formatting_is_corrected_and_recorded` |
| Transições válidas: PENDING→COMPLETED, PENDING→CANCELLED, COMPLETED→RETURNED. Qualquer outra (ex.: CANCELLED→COMPLETED) vai para quarentena e o estado atual é mantido | `schemas.PURCHASE_TRANSITIONS`, `stage` | `test_incremental_actions_against_current_state` |
| `status_updated_at` não pode ser anterior a `purchase_timestamp` | `INCONSISTENT_STATUS_TIMESTAMP` | parametrizado |
| Data de negócio (`purchase_date`) = data no fuso `America/Sao_Paulo` | `tz_date` | `test_valid_purchase_is_inserted_with_local_business_date` |

**Política financeira** (`business_metrics.purchase_measures`):

| Medida | Definição |
|---|---|
| Venda concluída | status COMPLETED **ou** RETURNED (a venda aconteceu) |
| Faturamento bruto (`gross_sales`) | soma de `gross_amount` das vendas concluídas |
| Descontos (`discounts`) | soma de `discount_amount` das vendas concluídas |
| Faturamento líquido (`net_sales`) | bruto − descontos das vendas concluídas |
| Devoluções (`returns_amount`) | valor líquido das compras RETURNED |
| Valor realizado (`realized_revenue`) | líquido das COMPLETED = `net_sales − returns_amount` |
| Canceladas / pendentes | não entram em nenhuma medida financeira; são contadas à parte |

A devolução é atribuída à **data da venda** (simplificação documentada em `limitations.md`).

## 2. Movimentações de pontos (`silver.points_transactions`)

**Sinal por tipo** (`signed_points`): `CREDIT +`, `DEBIT −`, `EXPIRATION −`, `ADJUSTMENT` = valor com
sinal informado pela origem. CREDIT/DEBIT/EXPIRATION exigem `points_amount > 0`; ADJUSTMENT exige `≠ 0`.

**Status que afetam o saldo:** somente `COMPLETED` com `actual_completion_timestamp <= data de
referência` (`effective_flag`). `PENDING` não afeta o saldo; `REJECTED` e `CANCELLED` nunca afetam.

| Regra | Motivo de rejeição |
|---|---|
| Crédito `reason_code = PURCHASE` exige `purchase_id` | `MISSING_PURCHASE_REF` |
| Exceções documentadas sem compra: `WELCOME_BONUS`, `CAMPAIGN_BONUS` | — |
| `purchase_id` informado deve existir na Silver | `ORPHAN_CREDIT` |
| Um único crédito PURCHASE (não rejeitado/cancelado) por compra; o mais antigo (timestamp, id) é o canônico, considerando Silver + lote | `DUPLICATE_CREDIT` |
| Débito exige `redemption_id` existente | `MISSING_REDEMPTION_REF`, `ORPHAN_DEBIT` |
| Ajuste exige `reason_code`; ajustes `MANUAL_*` exigem `operator_id` (rastreabilidade) | `MISSING_ADJUSTMENT_REASON`, `MISSING_OPERATOR` |
| COMPLETED exige `actual_completion_timestamp` | `COMPLETED_WITHOUT_COMPLETION_TS` |
| Linha JSON malformada | `MALFORMED_RECORD` |
| Transições: PENDING→COMPLETED/REJECTED/CANCELLED | `INVALID_STATUS_TRANSITION` |

**Pontuação esperada** (reconciliação): `floor(net_amount × points_per_currency_unit × multiplicador)`,
multiplicador da campanha MULTIPLIER se a compra tem o código e está na vigência; senão 1.

**Devolução:** o programa fictício estorna integralmente o crédito concluído (`ADJUSTMENT`,
`reason_code = PURCHASE_RETURN`, valor negativo). Se a devolução ocorre antes de o crédito ser
concluído, o crédito é CANCELADO e não há estorno.

**Saldo:** acumulado de todas as movimentações efetivas até a data de referência (não é "créditos
do mês − resgates do mês": pontos vêm de períodos anteriores, expiram e sofrem ajustes). O menor
saldo acumulado (window por cliente ordenada pela conclusão) identifica saldo negativo.

**Expiração:** os eventos `EXPIRATION` vêm da origem; a pipeline não recalcula a política de
validade (365 dias, informada em `expiration_date`). Ver `limitations.md`.

## 3. Prazo de crédito (SLA)

Configurável: `rules.credit_sla_hours` (padrão 72 h). Prazo previsto = `expected_completion_timestamp`
da origem; se ausente num crédito, é derivado (`transaction_timestamp + SLA`) e a correção é registrada.
Classificação na data de referência (`classify_credit_sla`):

| Categoria | Condição |
|---|---|
| COMPLETED_ON_TIME | concluído até a referência e `actual <= expected` |
| COMPLETED_LATE | concluído até a referência e `actual > expected` |
| PENDING_WITHIN_SLA | não concluído na referência e `referência <= expected` |
| PENDING_OVERDUE | não concluído na referência e `referência > expected` |
| NOT_APPLICABLE | REJECTED ou CANCELLED |

Um crédito concluído **depois** da data de referência é tratado como pendente nela. Pendentes
nunca entram no percentual "no prazo".

## 4. Resgates (`silver.redemptions`)

Status: REQUESTED → COMPLETED | REJECTED | CANCELLED. Só COMPLETED conta como resgate concluído.
O resgate concluído gera um `DEBIT` no razão de pontos. **O saldo é calculado apenas a partir do
razão de pontos**; a tabela de resgates é usada para indicadores de resgate. Assim o mesmo consumo
de pontos nunca é contado duas vezes. A reconciliação exige exatamente um DEBIT concluído com os
mesmos pontos para cada resgate concluído.

## 5. Campanhas

* **Exposição:** evento `EXPOSURE`. Exposições repetidas contam uma vez em métricas de clientes únicos.
* **Interação:** `CLICK` (e `CONVERSION` do rastreamento) para "cliente ativo em campanha".
* **Atribuição (regra única, uma campanha por compra):** a compra tem `campaign_id = C`, foi vendida
  (COMPLETED/RETURNED), a data está na vigência de C e o cliente teve EXPOSURE ou CLICK de C nos
  `attribution_window_days` (7) dias anteriores. Demais casos: `TAGGED_WITHOUT_EXPOSURE`,
  `TAGGED_OUTSIDE_PERIOD`, `NOT_SOLD`, `NO_CAMPAIGN`.
* **Conversão:** cliente único com ≥ 1 compra atribuída. Taxa = convertidos / expostos (únicos).
* Eventos `CONVERSION` do rastreamento são **comparados** com as compras (duplicidades são medidas),
  mas não definem a atribuição.
* **ROI não é calculado** (não há custo da campanha). **Não há afirmação de causalidade**: sem grupo
  de controle, diferenças entre expostos e não expostos não medem o efeito da campanha.

## 6. Clientes ativos e recorrentes

Atividades medidas separadamente por mês: compra (venda concluída), crédito de pontos efetivo,
solicitação de resgate e interação com campanha (CLICK/CONVERSION). `active_any_activity` é a
união documentada; não substitui os indicadores específicos.

* **Recorrente no mês:** ≥ 2 vendas concluídas em ≥ 2 dias distintos do mesmo mês.
* **Inativo:** sem venda concluída nos últimos `inactivity_window_days` (90) antes da referência
  (`INACTIVE_90D`). Não é rotulado como churn.
* **Coorte:** mês da primeira venda concluída; retenção no mês k = clientes da coorte com venda no
  mês `coorte + k` / tamanho da coorte.

## 7. Políticas de correção, rejeição e quarentena

| Situação | Política |
|---|---|
| Formatação de códigos (espaços, minúsculas) | **corrigir** e registrar em `_corrections` |
| Prazo previsto ausente em crédito | **derivar** pela regra de SLA e registrar |
| Tipo inválido, valor/ID inválido, referência inexistente, transição proibida | **quarentena** (`silver.quarantine`) com todos os motivos, payload original, arquivo e lote |
| Cópia exata dentro do lote | manter 1, demais em quarentena (`DUPLICATE_RECORD`) |
| Mesma chave com conteúdo diferente no lote | manter a versão mais recente, demais em quarentena (`DUPLICATE_KEY_CONFLICT`) |
| Reenvio idêntico em outro lote | **ignorar** (`UNCHANGED`), sem duplicar |
| Versão mais antiga que a atual | **ignorar** (`STALE`) |
| Divergências entre entidades (pontos x compras x resgates) | **não alteram** a Silver; vão para `governance.reconciliation_audit` e para o backlog operacional |

Nada é excluído silenciosamente: a Bronze guarda o valor original, e a quarentena guarda o motivo.

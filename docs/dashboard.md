# Dashboard: especificação e montagem

Há duas formas de visualizar:

1. **Gerado automaticamente** (executado): `python scripts/build_dashboard.py --env databricks` lê as
   tabelas Gold/governance reais e grava `docs/dashboard/index.html` (Plotly, abre no navegador,
   sem licença). Capturas de tela devem sair desse arquivo e ir para `docs/screenshots/`.
2. **Montagem manual** em **Databricks AI/BI Dashboards** (disponível no menu do workspace, usando o
   SQL warehouse) ou **Metabase** (open source). Abaixo, páginas, gráficos e consultas.

## Páginas

| Página | Visual | Fonte (tabela / consulta) | Tipo de gráfico |
|---|---|---|---|
| 1. Visão executiva | Compras concluídas, faturamento líquido, ticket médio | `kpi_monthly_sales` (Q01), soma do período | cartões |
| | Clientes compradores (último mês) | `kpi_active_customers_monthly` (Q11) | cartão |
| | Pontos creditados, % créditos no prazo | `kpi_points_summary` (Q05) | cartões |
| | Resgates concluídos | `kpi_redemption_summary` (Q08) | cartão |
| | Alertas de qualidade | `data_quality_incidents` (DQ05) | cartão + lista |
| | Faturamento líquido por mês | Q01 | colunas |
| 2. Operação de pontos | Créditos pendentes no prazo / vencidos | `operational_backlog` (issue_group = POINTS) | cartões |
| | Créditos por categoria de prazo | `kpi_credit_sla_daily` (Q07) | colunas |
| | Duplicidades, divergências de saldo | `kpi_points_summary`, R01, R03 | cartões + tabela |
| | Distribuição dos tipos de movimentação | Q06 | colunas |
| 3. Campanhas | Expostos, convertidos, taxa de conversão | `kpi_campaign_performance` (Q09) | tabela + colunas |
| | Compras e faturamento atribuídos | Q09 | colunas |
| | Conversão por segmento e canal | Q10 | tabela dinâmica |
| 4. Clientes | Ativos por tipo de atividade | Q11 | linhas (1 eixo) |
| | Frequência de compra, recorrência | Q11 | linhas |
| | Retenção por coorte | Q12 | mapa de calor (rampa de 1 cor) |
| | Ticket por segmento | Q13 | colunas |
| 5. Qualidade | Registros processados e rejeitados | DQ06, DQ03 | tabela + barras horizontais |
| | Taxa de duplicidade por lote | DQ07 | tabela |
| | Incidentes por categoria | DQ05 | barras horizontais |
| | Histórico de execuções | DQ06 | tabela/colunas |

Regras de visualização adotadas: um eixo por gráfico (sem eixo duplo); contagens distintas
(`buyers`, `exposed_customers`) nunca somadas entre linhas; ROI exibido como "não calculado".

## Montagem no Databricks AI/BI (Free Edition)

Passos não executados neste projeto; confira a interface do seu workspace.

1. Workspace → **Dashboards** → *Create dashboard*.
2. Aba **Data** → *Create from SQL* → cole cada consulta de `sql/business_kpis.sql` / `sql/data_quality.sql`
   (uma por dataset, ex.: Q01, Q05, Q09, Q11, Q12, DQ03, DQ05).
3. Aba **Canvas** → adicione visualizações conforme a tabela acima; crie 5 páginas.
4. Publique com *embed credentials* desligado (cada leitor usa as próprias permissões do UC).

## Montagem no Metabase (local)

O Metabase precisa de uma conexão com o Databricks (driver Databricks/Spark SQL ou via SQL
warehouse JDBC) **ou** de uma cópia das tabelas Gold num banco local. Alternativa simples e sem
custo: exportar as tabelas Gold agregadas para CSV/Parquet e carregar em um banco local (por
exemplo, PostgreSQL ou DuckDB) que o Metabase leia. Essa etapa **não foi executada** neste projeto.

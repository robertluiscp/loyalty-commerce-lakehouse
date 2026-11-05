# Material de apresentação (GitHub, LinkedIn e entrevistas)

> Use com honestidade: este é um **projeto pessoal de portfólio** com dados sintéticos. Ele
> demonstra que você sabe estruturar e explicar um Lakehouse; não equivale a experiência de
> produção com essas ferramentas. Se a sua experiência corporativa for em outra stack, diga isso.

## 1. Descrição curta (campo "About" do GitHub)

Lakehouse de fidelidade e comércio digital com dados sintéticos: PySpark, Delta Lake (MERGE,
time travel), Unity Catalog e arquitetura Medallion no Databricks Free Edition, com qualidade,
reconciliação de pontos, KPIs e testes automatizados.

## 2. Resumo para o LinkedIn (post)

Concluí um projeto pessoal de engenharia de dados: o **Loyalty Commerce Lakehouse**, que simula a
plataforma de um programa de pontos fictício, com compras, parceiros, campanhas e resgates.

O que eu construí:
- um gerador de dados sintéticos reproduzível (seed fixa), com anomalias controladas e registradas
  num "ledger" para testar a pipeline;
- uma pipeline Bronze → Silver → Gold em PySpark, rodando no Databricks Free Edition (serverless)
  com tabelas Delta gerenciadas pelo Unity Catalog;
- ingestão idempotente por lote, validação com quarentena rastreável e MERGE incremental (reprocessar
  um lote não duplica nada);
- reconciliação entre compras, pontos e resgates, com saldo acumulado por window function e
  detecção de saldo negativo e de créditos fora do prazo;
- KPIs de vendas, pontos, resgates, campanhas, coortes e qualidade, todos com numerador e
  denominador documentados;
- testes automatizados (pytest) e um benchmark medido de CSV x Parquet x Delta.

Na execução real, todas as anomalias injetadas foram detectadas na camada esperada, sem falsos
positivos. Também documentei o que o plano gratuito não permite (grupos e GRANTs, por exemplo) e
deixei os scripts prontos para um ambiente compatível.

Todos os dados são sintéticos. Código e documentação: https://github.com/robertluiscp/loyalty-commerce-lakehouse

#dataengineering #databricks #pyspark #deltalake #lakehouse

## 3. Apresentação para entrevista (~2 minutos)

"Eu quis praticar o ciclo completo de um Lakehouse no domínio de fidelidade e comércio. Como não
podia usar dados reais, escrevi um gerador que simula dois lotes de extração e injeta anomalias
conhecidas, cada uma registrada com a camada que deveria detectá-la. Isso me deu um 'gabarito' para
provar que a pipeline funciona.

Na Bronze eu guardo tudo como texto, com o arquivo de origem e o lote, porque não quero corrigir
nada silenciosamente. Na Silver tipo, valido e deduplico. Cada linha recebe uma ação: inserir,
atualizar, ignorar porque é reenvio idêntico, ignorar porque é versão antiga, ou quarentena. Só as
inserções e atualizações entram num MERGE do Delta, e por isso reprocessar é idempotente.

A parte de negócio mais interessante foi a reconciliação de pontos. O saldo vem só do razão de
movimentações, para não contar o resgate duas vezes, e é acumulado com uma window por cliente.
Assim eu detecto se o saldo ficou negativo em algum momento, mesmo que o saldo final seja positivo.
Na Gold monto o modelo dimensional e os KPIs, sempre documentando o denominador de cada taxa.

Rodei tudo no Databricks Free Edition com Unity Catalog. O que o plano não permite, como criar
grupos para os GRANTs, ficou como script documentado e não marcado como executado."

## 4. Perguntas técnicas prováveis e respostas

**1. Por que a Bronze guarda tudo como string?**
Para preservar o valor original e não perder informação numa conversão automática. Se `gross_amount`
vier "abc" ou uma data vier "2025-02-30", a Bronze guarda isso; a Silver tenta converter com
`try_cast`/`try_to_timestamp`, e a falha vira um motivo de quarentena rastreável até o arquivo e o lote.
Inferência de schema poderia transformar o valor em nulo sem avisar.

**2. Como você garante que reprocessar não duplica?**
Em três níveis. (a) Na Bronze, cada lote é gravado com `replaceWhere _batch_id = 'X'`: reprocessar
substitui só aquele lote. (b) Na Silver, comparo cada linha com o estado atual pela chave de negócio
e pelo hash do conteúdo: reenvio idêntico vira `UNCHANGED` e versão mais antiga vira `STALE`. Só
`INSERT`/`UPDATE` vão para o `MERGE`, que ainda tem a condição `s._content_hash <> t._content_hash`.
(c) A quarentena do lote também é substituída com `replaceWhere`. O teste de integração reexecuta
o lote 2 e verifica zero inserções e atualizações.

**3. Onde há shuffle e como você reduziu custo?**
Há shuffle na deduplicação (window por chave), no join do lote com o estado atual, na detecção de
crédito duplicado e no saldo acumulado (window por cliente). Para a integridade referencial uso
`broadcast` das dimensões pequenas, o que evita o shuffle da tabela de fatos. Não particionei as
tabelas porque, com dezenas de milhares de linhas, isso só criaria arquivos pequenos. Não uso
`collect()` em dados de fatos, só em agregados. Como o serverless não tem `cache()`, materializo o
estágio da Silver numa tabela temporária para não recomputar o plano.

**4. Qual a diferença entre um diretório Parquet e uma tabela Delta?**
Parquet é só o formato colunar dos arquivos. Delta acrescenta um log transacional (`_delta_log`) que
registra cada commit, o que dá atomicidade, MERGE, schema enforcement e time travel. No projeto eu
mostro isso: gravando duas vezes em modo overwrite, o Parquet perde a versão anterior, enquanto o
Delta permite ler `versionAsOf 0`. Uma coluna nova sem `mergeSchema` é rejeitada pelo Delta. E uma
tabela Delta não fica automaticamente governada pelo Unity Catalog: isso depende de ela ser
registrada no catálogo (tabela gerenciada ou externa).

**5. Como você definiu "crédito no prazo" e por que o denominador importa?**
O prazo previsto vem da origem ou é derivado como transação + 72 h (configurável). Na data de
referência, cada crédito cai em uma categoria: concluído no prazo, concluído atrasado, pendente
dentro do prazo, pendente vencido, ou não aplicável (rejeitado/cancelado). O percentual no prazo
usa apenas os créditos **concluídos** no denominador. Se eu incluísse os pendentes, a taxa cairia
sem que nenhum crédito tivesse atrasado de fato. Os pendentes vencidos são um indicador separado.

## 5. Explicação simples dos conceitos

* **Apache Spark:** motor que divide o processamento de dados em tarefas executadas em paralelo
  por várias máquinas (ou núcleos). Você descreve a transformação e ele planeja a execução.
* **PySpark:** a API Python do Spark (DataFrames, SQL, window functions).
* **Parquet:** formato de arquivo colunar e comprimido. Ler 2 de 15 colunas lê só essas 2.
* **Delta Lake:** Parquet + log de transações, o que traz ACID, MERGE (upsert), histórico de
  versões, time travel e validação de schema.
* **Unity Catalog:** camada de governança do Databricks, com namespace `catálogo.schema.tabela`,
  permissões (GRANT), linhagem e auditoria centralizadas.
* **Arquitetura Medallion:** organizar os dados em camadas de qualidade crescente: Bronze (bruto e
  rastreável), Silver (limpo e validado) e Gold (pronto para negócio).

## 6. Decisões técnicas e justificativas

| Decisão | Por quê |
|---|---|
| Ledger de anomalias no gerador | serve de "gabarito" e prova a detecção sem depender de inspeção manual |
| Ação por linha antes do MERGE | torna explícito o que é novo, atualizado, reenviado, velho ou inválido |
| Quarentena com array de motivos | uma linha pode ter vários problemas; nada é descartado em silêncio |
| Reconciliação separada da validação | uma divergência entre sistemas não é um registro malformado |
| Saldo só pelo razão de pontos | evita dupla contagem de resgates |
| Gold como função pura (`compute_gold`) | permite testar os KPIs com DataFrames pequenos em memória |
| Full refresh na Gold, MERGE na Silver | simplicidade onde é barato e incrementalidade onde importa |
| Data de referência fixa | resultados reproduzíveis nos testes e na documentação |
| ROI não calculado | sem dados de custo, qualquer ROI seria inventado |

## 7. Limitações e melhorias futuras

Limitações: ver [limitations.md](limitations.md). Melhorias possíveis:
- orquestrar com Databricks Jobs / Lakeflow Declarative Pipelines (expectations nativas);
- Auto Loader para ingestão incremental de arquivos;
- SCD tipo 2 para clientes e campanhas;
- Gold incremental por partição de data em volumes maiores;
- política de expiração FIFO calculada pela pipeline;
- reprocessamento automático da quarentena quando a dimensão chegar depois;
- dashboard publicado no Databricks AI/BI;
- grupos e GRANTs num workspace com console de conta.

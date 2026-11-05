# Benchmark CSV x Parquet (x Delta): metodologia

## Pergunta

Para os mesmos dados de compras, quanto mudam **tamanho**, **tempo de escrita**, **tempo de
leitura completa** e **tempo de uma consulta que usa só 2 colunas** entre CSV e Parquet (e Delta)?

## Desenho

| Item | Modo local | Modo Spark (Databricks) |
|---|---|---|
| Motor | pandas + pyarrow | Spark serverless via Databricks Connect |
| Base | `purchases.csv` dos 2 lotes (landing) replicada 10x | `silver.purchases` replicada 10x (`crossJoin(range(10))`) |
| Formatos | CSV, Parquet (snappy) | CSV (com cabeçalho), Parquet, Delta |
| Leitura completa | `read_csv` / `read_table().to_pandas()` | `format("noop")` (lê e desserializa tudo, sem custo de escrita) |
| Leitura parcial | 2 colunas (`partner_id`, `net_amount`) + soma por parceiro | idem com `groupBy().agg().collect()` |
| Repetições | 5, mediana | 3, mediana |
| Schema na leitura CSV | inferido pelo pandas | **explícito** (inferir exigiria uma varredura extra e seria injusto) |
| Tamanho | bytes em disco | bytes no Volume (Delta inclui `_delta_log`) e, em Delta, também o tamanho da versão atual (`DESCRIBE DETAIL`) |

Comandos:

```bash
python -m loyalty_lakehouse.benchmark --mode local --replicate 10 --repeats 5 --output docs/results/benchmark_local.json
python -m loyalty_lakehouse.benchmark --mode spark --env databricks --replicate 10 --repeats 3 --output docs/results/benchmark_databricks.json
```

## Cuidados de interpretação

* Os dois modos **não são comparáveis entre si** (motores, máquinas e redes diferentes).
* No modo local as leituras são "a quente": o arquivo acabou de ser escrito e está no cache do
  sistema operacional. Leituras "a frio" do disco seriam mais lentas, sobretudo para o CSV, que é
  bem maior.
* No Databricks Connect, cada medição inclui ida e volta de rede e planejamento remoto, além de
  possível cache do serverless entre repetições. Diferenças pequenas são ruído.
* Parquet ganha em tamanho e leitura parcial por ser **colunar e comprimido** (lê só as colunas
  necessárias - *column pruning* - e guarda estatísticas por row group). O CSV precisa ler e
  converter todos os bytes de texto.
* Delta = Parquet + log transacional: tamanho e leitura parecidos com Parquet; o ganho é
  ACID, MERGE, time travel e schema enforcement, não velocidade bruta.
* Sobrescrever uma tabela Delta **não apaga** os arquivos antigos (são retidos para time travel até
  um `VACUUM`). Por isso o diretório do benchmark cresce a cada repetição, e o tamanho que importa
  para comparar com o Parquet é o da versão atual.
* Resultados variam com volume, compressão, hardware, cache e configuração. Não generalize
  números de 300 mil linhas para bilhões.

Resultados medidos: seção "Benchmark" do README e arquivos em `docs/results/`.

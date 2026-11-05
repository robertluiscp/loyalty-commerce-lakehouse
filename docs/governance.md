# Governança

## Catálogo x schema

* **Catálogo** (`loyalty_lakehouse`): nível mais alto do namespace de 3 níveis do Unity Catalog
  (`catálogo.schema.objeto`). Isola o projeto e é a fronteira natural para permissões amplas.
* **Schema** (`bronze`, `silver`, `gold`, `governance`): agrupa tabelas, views e volumes de uma camada.
  Permitir leitura só da `gold` a analistas é um GRANT no schema, sem listar tabela por tabela.
* **Volume** (`bronze.landing`): objeto do UC para arquivos não tabulares (CSV/JSON de origem).

## Organização das camadas e acesso pretendido

| Schema | Conteúdo | Quem escreve | Quem lê |
|---|---|---|---|
| bronze | dados brutos + volume landing | pipeline (engenharia) | engenharia |
| silver | estado atual validado + quarentena | pipeline | engenharia; auditoria (só quarentena) |
| gold | modelo dimensional e KPIs | pipeline | análise, dashboards |
| governance | logs, qualidade, incidentes, reconciliação | pipeline | engenharia, auditoria |

Os GRANTs correspondentes estão em `sql/unity_catalog_setup.sql` (seção 3), seguindo o **princípio
do menor privilégio**: cada grupo recebe apenas `USE CATALOG`, `USE SCHEMA` e `SELECT`/`MODIFY` no
que precisa; ninguém recebe `ALL PRIVILEGES`; analistas não têm acesso a dados brutos.

## O que foi efetivamente executado (Free Edition)

| Item | Status |
|---|---|
| `CREATE CATALOG loyalty_lakehouse`, 4 schemas, volume `bronze.landing` | **Executado** (o Free Edition permitiu criar catálogo) |
| Tabelas Delta gerenciadas pelo UC (todas as camadas) | **Executado** pela pipeline |
| Consultas de metadados (`SHOW`, `information_schema.tables`) | **Executado** (`scripts/run_sql_checks.py`, ver `docs/results/sql_checks.json`) |
| `SHOW GRANTS` no catálogo | **Executado**; retornou vazio (o dono do catálogo tem acesso por ownership) |
| GRANTs para grupos | **Não executado**: o Free Edition não dá acesso ao console da conta/SCIM para criar grupos. Script pronto com placeholders |
| Lineage nativo (Catalog Explorer / `system.access.table_lineage`) | **Não verificado** por este projeto. A disponibilidade de system tables no Free Edition não foi confirmada; a linhagem está documentada manualmente em `data_lineage.md` |
| Auditoria nativa (`system.access.audit`) | **Não verificado** pelo mesmo motivo |

## Registros do projeto x mecanismos nativos

O schema `governance` é uma **escolha de organização deste projeto**, não um componente do Unity
Catalog. Ele guarda o que o *pipeline* sabe (execuções, contagens, verificações, divergências).
Os mecanismos nativos da plataforma são outra coisa:

| Necessidade | Mecanismo nativo (plataforma) | Neste projeto |
|---|---|---|
| Quem pode ler/escrever | privilégios do Unity Catalog (GRANT/REVOKE, ownership) | script de GRANTs |
| De onde vem cada tabela | lineage automático do UC | `data_lineage.md` + `_source_file`/`_batch_id` |
| Quem acessou o quê | system tables de auditoria | não implementado |
| O pipeline rodou? quanto processou? | — | `governance.pipeline_execution_log` |
| Os dados estão corretos? | (Lakeflow expectations, em pipelines declarativos) | `data_quality_results`, `data_quality_incidents`, `reconciliation_audit` |

## Segredos e dados sensíveis

* Nenhuma credencial no repositório: autenticação via OAuth (`databricks auth login`), com o token
  guardado pelo Databricks CLI no armazenamento seguro do sistema; `~/.databrickscfg` só guarda o
  host e o tipo de autenticação e não é versionado.
* `config/databricks.yaml` (personalização local) está no `.gitignore`.
* Não há dados pessoais: IDs sintéticos (`C000001`), sem nome, e-mail, telefone ou documento.

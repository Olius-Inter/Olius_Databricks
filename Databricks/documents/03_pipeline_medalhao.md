# Pipeline medalhão — aplicação da Aula 5 ao OLIUS

## O que mudou após ler a aula

O material mostra dois momentos diferentes: notebooks comuns (páginas 6–14) e
notebooks adaptados para uma pipeline declarativa (páginas 16–24). Na página 20,
gravações manuais são substituídas por `@dp.materialized_view`.

Os notebooks anteriores eram executores comuns: liam os dados com `toPandas()` e
gravavam resultados com `saveAsTable()`. Eles não poderiam ser adicionados sem
adaptação à pipeline mostrada na aula. Agora os mesmos dois arquivos declaram
materialized views e retornam planos Spark; o Databricks gerencia a atualização.

O tratamento e os cálculos permanecem em pandas por `applyInPandas`. Essa API
executa funções pandas como parte de uma transformação Spark, sem coletar no
driver durante o planejamento. O lote das tabelas relacionadas usa um grupo
único para preservar as verificações cruzadas. Isso atende à massa acadêmica,
com limites explícitos; não é uma solução distribuída para milhões de registros.

## Arquivos usados

- `Databricks/silver_note.py`: fonte de transformação da pipeline.
- `Databricks/gold_note.py`: outra fonte da **mesma** pipeline.
- `Databricks/01_refresh.py`: tarefa Notebook do Job, antes da pipeline, caso o
  refresh do PostgreSQL deva fazer parte da automação. Não é fonte de transformação.

`BI/` não é dependência. Não é necessário um notebook bronze para copiar o CSV
do exemplo da aula: neste projeto a bronze já é o catálogo federado, schema `dm`.
Não há migração de repositório nem exportação/push para GitHub nesta entrega.

## 1. Verificar a bronze

No editor SQL, substitua o nome do catálogo pelo existente no seu workspace:

```sql
SELECT * FROM `NOME_CATALOGO_FEDERADO`.dm.refresh_state;
SELECT * FROM `NOME_CATALOGO_FEDERADO`.dm.fato_coleta LIMIT 5;
SELECT * FROM `NOME_CATALOGO_FEDERADO`.dm.fato_entrega_pev LIMIT 5;
```

`refresh_state` deve conter uma linha com `snapshot_id` e `refreshed_at`. As 13
materialized views precisam estar visíveis para a identidade que executa a pipeline.
Se um objeto não estiver visível, revise a federação/permissões antes de executar.

O refresh do PostgreSQL deve terminar antes da leitura. Não execute outro refresh
manualmente ou por um Job concorrente durante a pipeline: a federação faz leituras
distintas e não compartilha uma transação PostgreSQL entre todas as tabelas.

## 2. Escolher onde publicar

O exemplo da aula usa um catálogo e schemas bronze/silver/gold. Você pode fazer
igual usando um catálogo **gravável** já existente, por exemplo `workspace`, e os
schemas `silver` e `gold`. Não use o catálogo federado como destino gravável.

Execute no editor SQL, fora dos notebooks de transformação:

```sql
-- Exemplo: troque workspace pelo catálogo gravável que você possui.
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
```

Se preferir os catálogos `silver` e `gold` separados, mantenha os padrões dos
notebooks e prepare `silver.olius` / `gold.olius`. Os dois arranjos são suportados.

Se já executou os notebooks antigos e eles criaram views nesses mesmos nomes,
escolha **novos schemas**, como `workspace.silver_pipeline` e
`workspace.gold_pipeline`. A pipeline deve criar os próprios objetos; não apague
nem tente sobrescrever automaticamente views anteriores ou tabelas de outro dono.

A identidade de execução precisa de leitura da origem, uso dos catálogos/schemas
e criação de tabelas/materialized views nos destinos. A conexão do PostgreSQL
continua usando o reader; nenhuma permissão de escrita nele é necessária.

## 3. Criar a pipeline no Databricks

1. Entre em **Jobs & Pipelines** e crie uma **ETL Pipeline / Lakeflow pipeline**.
   Os rótulos podem variar com a versão da interface. Não escolha apenas um Job
   de duas tarefas Notebook, pois esse não é o mecanismo declarativo da aula.
2. Dê o nome `Olius_Medalhao`.
3. Use Unity Catalog e o modo atual de publicação (não o antigo modo `LIVE`).
4. Escolha o catálogo gravável e o schema padrão, por exemplo `workspace` / `gold`.
   Os nomes completos definidos pelo código direcionam silver e gold aos destinos
   configurados, independentemente desse padrão.
5. Use modo **Triggered**, para processar uma atualização e encerrar. Durante a
   preparação, pode usar o modo de desenvolvimento disponível no workspace.
6. Adicione **os dois notebooks atualizados** como fontes de transformação, na
   seção de código/All files/transformations, conforme a interface. Use os arquivos
   deste repositório ou importe-os como notebooks Python. Não adicione cópias
   antigas junto das novas, pois isso duplicaria as definições dos datasets.
7. Não coloque `01_refresh.py` ou os arquivos de `BI/` nas transformações.

Não é preciso executar silver manualmente antes de gold. Ambas são fontes da
mesma pipeline; o framework identifica a ordem pelas referências entre datasets.

## 4. Preencher Configuration

Nos ajustes da pipeline, abra **Configuration / configurações avançadas**. O código
Python lê essas chaves com `spark.conf.get`; elas substituem os widgets anteriores.
Não confunda com os novos parâmetros nomeados destinados ao SQL.

Exemplo para um catálogo gravável com schemas, como na aula:

```json
{
  "olius.bronze_catalog": "NOME_CATALOGO_FEDERADO",
  "olius.bronze_schema": "dm",
  "olius.silver_catalog": "workspace",
  "olius.silver_schema": "silver",
  "olius.gold_catalog": "workspace",
  "olius.gold_schema": "gold",
  "olius.reference_date": "",
  "olius.max_rows_per_table": "200000",
  "olius.max_total_rows": "500000",
  "olius.max_output_rows": "2000000",
  "spark.sql.session.timeZone": "UTC"
}
```

Troque `workspace` se o catálogo gravável tiver outro nome. Deixe
`olius.reference_date` vazio para usar a data corrente de São Paulo. Para uma
reprodução, use `AAAA-MM-DD`. A gold herda a mesma data da silver.

Use um ambiente atual com pandas **2 ou superior**, numpy e pyarrow compatíveis
com o runtime. Esses pacotes normalmente integram o ambiente Spark; se precisar
ajustar pandas, faça isso nas dependências da pipeline, sem `%pip` dentro das
definições dos datasets. O parâmetro `format='mixed'` exige pandas 2+.

## 5. Executar e conferir

1. Confirme que o Data Mart do PostgreSQL foi atualizado.
2. Use **Validate**, se disponível, para verificar referências e permissões.
3. Clique em **Run pipeline / Start** para iniciar a atualização completa.
4. Espere a atualização inteira concluir com sucesso antes de atualizar dashboards.

O grafo conterá intermediárias privadas de tratamento/cálculo e as saídas públicas:

```text
Catálogo federado.dm (13 conjuntos + refresh_state)
  → _silver_pandas_lote (privada)
  → silver: dimensões, fatos, ponte, ranking, qualidade, quarentena e pipeline_state
  → _gold_pandas_lote (privada)
  → gold: KPIs, séries, estatísticas, detalhamentos e pipeline_state
```

As privadas são auxiliares internas, não tabelas para dashboards. As saídas públicas
são materialized views controladas pela pipeline; não há tabelas `_dados_*`, UUIDs
de append nem o antigo ponteiro de publicação manual. `pipeline_state` agora serve
somente para consultar a origem e a referência processadas.

As views são atualizadas individualmente pelo framework. Isso não constitui uma
transação única entre todas as saídas; consumidores devem aguardar o sucesso da
atualização completa. Uma falha pode deixar partes da atualização aplicadas.

Com a configuração do exemplo, confira no editor SQL:

```sql
SELECT * FROM workspace.silver.qualidade_dados ORDER BY tabela;
SELECT tabela, chave, motivos FROM workspace.silver.quarentena;
SELECT * FROM workspace.gold.pipeline_state;

SELECT metrica, perfil, valor, unidade
FROM workspace.gold.kpis
ORDER BY perfil, metrica;

SELECT periodo, perfil, quantidade_eventos, litros
FROM workspace.gold.serie_mensal
WHERE periodo_futuro = false
ORDER BY periodo, perfil;
```

Rejeições são registradas na silver e refletidas nos indicadores de qualidade da
gold. Colunas ausentes, controle ausente, limites de volume excedidos ou contagens
incoerentes entre silver e seu relatório causam falha, em vez de publicar uma amostra.

## 6. Encadear com o refresh em um Job

Se o refresh já é executado por um Job, acrescente a tarefa de pipeline ao mesmo
Job, em vez de criar outro agendamento independente:

```text
refresh_dm [Notebook: 01_refresh.py]
  → medalhao [ETL Pipeline: Olius_Medalhao; depende de refresh_dm / All succeeded]
  → atualização do dashboard [opcional; depende do sucesso de medalhao]
```

Configure concorrência máxima 1 e o fuso `America/Sao_Paulo`. Defina a periodicidade
depois de medir uma execução. Este repositório não cria o Job nem um agendamento
automaticamente. O notebook de refresh usa as secrets já descritas no guia anterior.

As páginas 41–57 da aula incluem exportação para GitHub e um Job que executa essa
exportação após o pipeline. Esse passo é específico do exemplo do professor e não
é necessário para nossos dashboards consultarem a gold diretamente. Se a atividade
exigir explicitamente exportação, será uma tarefa separada, com destinos/autorização
próprios; não coloque tokens ou push nas transformações declarativas.

## Validação e limites

```powershell
python -m unittest discover -s Databricks/tests -p test_medallion.py -v
```

A suíte verifica as funções pandas de ponta a ponta, KPIs, timezone, quarentena,
duplicações, pontos negativos legítimos, pontos ausentes, limites, contratos e a
declaração preguiçosa dos 39 datasets (incluindo 2 privados) com um substituto Spark.
Ela não certifica a federação, permissões, Arrow ou o engine Lakeflow real. A
homologação no workspace ainda precisa ser feita com os passos acima.

Os lotes são completos e as UDFs pandas opacas para otimizações incrementais;
espere recomputação dos cálculos, não uma atualização incremental garantida. O
limite total protege o processamento, mas o grupo inteiro e suas cópias precisam
caber na memória de um worker. Para grandes volumes, será necessário redesenhar
particionamento/joins e agregações com operações distribuídas.

Referências oficiais:
- https://docs.databricks.com/aws/en/ldp/developer/python-ref
- https://docs.databricks.com/aws/en/ldp/target-schema
- https://docs.databricks.com/aws/en/ldp/parameters
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html

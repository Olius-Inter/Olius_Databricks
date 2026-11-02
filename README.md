# OLIUS — Databricks e Business Intelligence

Repositório da camada analítica do **OLIUS**, com notebooks Databricks para
tratamento e cálculo de indicadores, atualização do Data Mart e um pipeline
Python para publicar KPIs em JSON e CSV.

Os arquivos de BI e Databricks foram migrados do
[DB_Segundo](https://github.com/Olius-Inter/DB_Segundo). A estrutura, as rotinas,
as permissões e as migrations do PostgreSQL continuam naquele repositório.

## Arquitetura

```text
PostgreSQL — dados operacionais
    │
    └── dm.refresh_data_mart()
          │
          └── Data Mart: schema dm
                ├── Catálogo federado no Databricks — bronze
                │     └── silver_note.py — tratamento e qualidade
                │           └── gold_note.py — indicadores e agregações
                │                 └── Dashboards / consultas SQL
                │
                └── BI/pipeline.py — alternativa Python
                      └── JSON e CSV para visualizações
```

A bronze é a conexão federada com o Data Mart PostgreSQL. Silver e gold são
materialized views gerenciadas por uma mesma pipeline declarativa no Databricks.
O processamento Python em `BI/` é uma alternativa de consumo do Data Mart e
não é dependência dos notebooks silver/gold.

## Estrutura

| Caminho | Responsabilidade |
| --- | --- |
| [`Databricks/01_refresh.py`](Databricks/01_refresh.py) | Atualiza o Data Mart PostgreSQL e informa o snapshot confirmado. |
| [`Databricks/silver_note.py`](Databricks/silver_note.py) | Valida tipos, domínios, duplicações e referências; produz qualidade e quarentena. |
| [`Databricks/gold_note.py`](Databricks/gold_note.py) | Calcula KPIs, séries, estatísticas e detalhamentos a partir da silver. |
| [`Databricks/documents/`](Databricks/documents/) | Guias de conexão, execução e configuração da pipeline. |
| [`Databricks/tests/`](Databricks/tests/) | Testes locais do refresh e das transformações. |
| [`BI/pipeline.py`](BI/pipeline.py) | Consulta um snapshot consistente de `dm` e publica JSON/CSV. |
| [`BI/carga_nuvem_adicional.sql`](BI/carga_nuvem_adicional.sql) | Carga adicional de demonstração, executada manualmente no PostgreSQL. |
| [`BI/README.md`](BI/README.md) | Contrato dos indicadores, carga de demonstração e formatos de saída. |
| [`SECURITY_REVIEW.md`](SECURITY_REVIEW.md) | Revisão de segurança, verificações realizadas e pendências. |

## Pré-requisitos

- PostgreSQL com Data Mart, procedure `dm.refresh_data_mart()`, controle
  `dm.refresh_state` e permissões preparados conforme o `DB_Segundo`.
- Workspace Databricks com Unity Catalog, conexão federada ao schema `dm` e
  acesso a um catálogo gravável para publicar silver e gold.
- Identidade de execução com leitura da origem e permissões de uso/criação
  necessárias nos destinos.
- Ambiente da pipeline com pandas 2+, NumPy e PyArrow compatíveis com o runtime.
- Para o refresh: conexão de rede ao PostgreSQL, psycopg e secrets configuradas
  conforme o [guia de conexão](Databricks/documents/01_execution_guide.md).

As contas têm funções distintas: `olius_bi_reader` consulta os dados,
`olius_bi_refresh` atualiza o Data Mart e `olius_dm_admin` é destinada à manutenção.

## Configurar a pipeline Databricks

1. Verifique se o catálogo federado expõe as views de `dm` e o registro de
   controle em `dm.refresh_state`.
2. Prepare os schemas de destino em um catálogo gravável. Exemplo:

   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
   CREATE SCHEMA IF NOT EXISTS workspace.gold;
   ```

3. Crie uma pipeline ETL/Lakeflow em modo **Triggered** e adicione
   `Databricks/silver_note.py` e `Databricks/gold_note.py` como fontes de
   transformação da **mesma pipeline**.
4. Preencha a configuração da pipeline com os nomes do seu ambiente:

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

5. Atualize o Data Mart antes de iniciar a pipeline. Aguarde o sucesso da
   atualização inteira antes de consumir os resultados nos dashboards.

As chaves `olius.*` são lidas com `spark.conf.get` na configuração da pipeline.
Com o exemplo acima, a tabela de KPIs será **`workspace.gold.kpis`**. Sem essas
substituições, os notebooks usam os destinos padrão `silver.olius` e `gold.olius`.
Deixe `olius.reference_date` vazio para usar a data corrente de São Paulo ou
informe `AAAA-MM-DD` para reprodução.

`01_refresh.py` é uma tarefa separada de atualização do banco; não deve ser
adicionado às transformações da pipeline. Os scripts de `BI/` também não são
fontes dessa pipeline.

Consulte o [passo a passo completo](Databricks/documents/03_pipeline_medalhao.md)
para permissões, conflitos com objetos existentes, consultas e limites.

## Configurar o Job

Para encadear a atualização do banco e o processamento analítico:

```text
refresh_dm — Notebook: Databricks/01_refresh.py
    └── medalhao — ETL Pipeline: Olius_Medalhao
          └── consumo/exportação — opcional, após sucesso da pipeline
```

Configure a tarefa `medalhao` para depender do sucesso de `refresh_dm`, limite
as execuções simultâneas a uma e evite refreshes concorrentes no PostgreSQL.
Os notebooks não criam nem agendam o Job automaticamente.

Os parâmetros do notebook de refresh já possuem estes valores padrão:

| Parâmetro | Padrão | Quando alterar |
| --- | --- | --- |
| `secret_catalog` | `workspace` | Quando as secrets estiverem em outro catálogo. |
| `secret_schema` | `olius_bi` | Quando as secrets estiverem em outro schema. |

Com esses valores, não é necessário adicionar parâmetros ao Job. Configure as
cinco secrets `pg-host`, `pg-port`, `pg-database`, `pg-refresh-password` e
`pg-ca-pem` conforme o [guia de execução](Databricks/documents/01_execution_guide.md).
Senhas e tokens não devem ser enviados como parâmetros do Job.

### Exportação da gold para GitHub

Uma exportação deve ser uma tarefa Notebook separada, dependente do sucesso da
pipeline. Este repositório ainda não contém um notebook de exportação nem uma
automação de commit/push. Configure a autenticação e o destino antes de adicionar
essa tarefa ao Job; não coloque gravações externas nas transformações silver/gold.

## Consultar a gold

Para a configuração de exemplo:

```sql
SELECT * FROM workspace.gold.pipeline_state;

SELECT metrica, perfil, valor, unidade
FROM workspace.gold.kpis
ORDER BY perfil, metrica;

SELECT periodo, perfil, quantidade_eventos, litros
FROM workspace.gold.serie_mensal
WHERE periodo_futuro = false
ORDER BY periodo, perfil;
```

Na silver, consulte `workspace.silver.qualidade_dados` para acompanhar o
tratamento. O acesso à quarentena deve ser restrito, pois ela pode conter
registros de origem e dados pessoais.

## Executar o pipeline Python de BI

Execute na raiz do repositório, com Python 3.10+:

```powershell
python -m pip install -r BI/requirements.txt
```

Configure a conexão PostgreSQL fora do código, usando as variáveis libpq.
Para banco remoto, configure `PGSSLMODE=verify-full` e uma CA confiável em
`PGSSLROOTCERT`. Use a conta leitora:

```powershell
python BI/pipeline.py --database SEU_BANCO --user olius_bi_reader
```

A saída padrão fica em `BI/output/`. Leia primeiro `latest.json` e use a pasta
indicada por `directory` para acessar `kpis.json`, `cards.csv`, `mensal.csv`,
`estatisticas.csv` e `participantes.csv`. O ponteiro só é atualizado após a
gravação do conjunto completo.

Esse pipeline lê o Data Mart; ele não executa o refresh. Veja as opções de
Docker, consulta contínua, data e ano no [guia de BI](BI/README.md).

O gerador `load_demo.py` foi removido. A carga SQL adicional já gerada permanece
disponível para execução manual, com dados de demonstração. Sua execução altera
o banco e deve seguir as instruções de [carga](BI/README.md#executar-a-carga).

## Testes locais

Para as transformações pandas, prepare também as dependências locais:

```powershell
python -m pip install "pandas>=2" numpy
python -B -m unittest discover -s Databricks/tests -p 'test_*.py' -v
python -B -m unittest discover -s BI -p 'test_*.py' -v
```

Os testes Databricks usam substitutos para serviços externos; não exigem acesso
à nuvem. Os testes de integração do BI são ignorados quando
`OLIUS_BI_TEST_CONTAINER` não está definido. Só configure essa variável e
`OLIUS_BI_TEST_DATABASE` para uma base de testes compatível com a carga documentada.

Testes locais não homologam rede, permissões, federação, Arrow ou execução real
da pipeline. A conferência no workspace deve seguir o
[guia da pipeline](Databricks/documents/03_pipeline_medalhao.md).

## Segurança e limites

- Armazene credenciais em secrets ou configuração local protegida. O `.gitignore`
  cobre credenciais comuns, caches, backups e diretórios padrão de saída.
- O refresh usa TLS com validação do servidor e certificado CA temporário.
  O pipeline de BI depende da configuração externa de TLS.
- As transformações processam lotes completos com pandas em um único grupo,
  adequado ao volume acadêmico e aos limites configurados. Os dados e suas
  cópias precisam caber na memória de um worker.
- As saídas da pipeline são atualizadas individualmente. Uma falha pode deixar
  parte delas atualizada; consumidores devem aguardar o sucesso da execução inteira.
- A revisão e as pendências conhecidas estão em
  [`SECURITY_REVIEW.md`](SECURITY_REVIEW.md).

## Documentação e licença

- [Visão geral do Data Mart](Databricks/documents/README.md)
- [Conexão e execução do refresh](Databricks/documents/01_execution_guide.md)
- [Refresh e validação](Databricks/documents/02_refresh_and_validation.md)
- [Pipeline medalhão](Databricks/documents/03_pipeline_medalhao.md)
- [Pipeline de BI e contrato dos indicadores](BI/README.md)
- [Banco de dados OLIUS](https://github.com/Olius-Inter/DB_Segundo)

Projeto acadêmico do OLIUS. Consulte a [licença](LICENSE) do repositório.

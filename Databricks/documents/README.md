# Olius — Data Mart federado no Databricks

O fluxo com silver/gold foi adaptado à pipeline declarativa da Aula 5. Consulte
[Pipeline medalhão: configuração e execução](03_pipeline_medalhao.md). Os parágrafos
abaixo documentam o fluxo original de refresh e consulta direta ao dm; com a nova
pipeline, os dashboards podem consultar as materialized views da gold no Databricks.

O PostgreSQL mantém o star schema: 13 views materializadas e dm.refresh_state.
Os dashboards consultam databricks_postgresql_catalog.dm usando olius_bi_reader.
Não há exportação Parquet, upload, Volume de snapshots ou cópia em Delta.

Um único Job executa 01_refresh.py: outra conexão, direta ao PostgreSQL, com
olius_bi_refresh, executa a procedure em REPEATABLE READ. pg_cron não faz parte
do fluxo escolhido. O reader continua apenas leitor.

| Arquivo | Ambiente | Finalidade |
| --- | --- | --- |
| PostgreSQL/08_materialized_views.sql | PostgreSQL, implantação | Instala o Data Mart |
| PostgreSQL/access_control/01_roles.sql e 02_users.sql | PostgreSQL | Papéis e contas sem senhas no Git |
| PostgreSQL/09_refresh_materialized_views.sql | PostgreSQL, manual | Teste/manutenção, não outro agendamento |
| Databricks/01_refresh.py | Notebook de um Job | Atualiza MVs; informa snapshot após COMMIT |
| Databricks/tests/test_refresh.py | Local e CI | Testes sem rede/credenciais |

Dashboards consultam diretamente dm. Esta pasta contém somente o executor do
refresh e seus testes/guias; não cria views de dashboards nem datasets.

Guias:
- [Conexão e Job](02_execution_guide.md).
- [Consistência e homologação](03_refresh_and_validation.md).

O repositório não cria nem agenda Jobs automaticamente e não altera o Aiven/workspace.
A conexão federada e o refresh manual pelo notebook foram confirmados pelo
usuário no ambiente de teste. A execução agendada pelo Job ainda precisa ser
configurada e validada. São caminhos diferentes. A Free Edition restringe saída
de rede e não oferece SLA. Se host/porta ou secrets não estiverem acessíveis,
o Job ainda não estará operacional; revisar o executor antes de agendar.
Não ampliar o reader nem desabilitar TLS para contornar essas restrições.

[Limites da Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

# Conexão e execução

## PostgreSQL e federação

Instalar os scripts principais 01–08, catálogo e controle de acesso conforme
PostgreSQL/access_control/README.md. Não reinstalar 08 no Aiven só para mudar o
modo de acesso: é um script de instalação, não uma migração.

Configurar senhas fora do Git:
- olius_bi_reader: conexão federada existente.
- olius_bi_refresh: conexão Python do Job.
- olius_dm_admin: manutenção; nunca usar no dashboard/Job.

Manter a conexão federada apontando para o banco correto e usando o reader.
No editor SQL Databricks, verificar:

```sql
SELECT * FROM <NOME_DO_CATALOG>.dm.refresh_state;
SELECT * FROM <NOME_DO_CATALOG>.dm.ranking_anual LIMIT 10;
```

Para descobrir objetos novos, um usuário autorizado pode executar:

```sql
REFRESH FOREIGN SCHEMA <NOME_DO_CATALOG>.dm;
```

Isso atualiza metadados, não resultados das MVs.
[Referência](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-foreign).

## Preparar o notebook

Importar 01_refresh.py como notebook Python no workspace ou vinculá-lo ao Git
no Job. Usar ambiente serverless versão 4 ou superior para ler secrets do
Unity Catalog; adicionar psycopg[binary]==3.2.10 e aplicar.
Não instalar pacotes dinamicamente a cada execução.

Criar as cinco secrets no Unity Catalog, catálogo workspace e schema olius_bi:

| Chave | Valor |
| --- | --- |
| pg-host | Host PostgreSQL Aiven, sem protocolo |
| pg-port | Porta do serviço |
| pg-database | Banco OLIUS do segundo ano |
| pg-refresh-password | Senha de olius_bi_refresh |
| pg-ca-pem | Conteúdo completo da CA ca.pem do Aiven |

Se o campo Secret value remover quebras de linha, manter os marcadores BEGIN/END
e todo o conteúdo do certificado. O notebook reconstrói as linhas do PEM e
valida o certificado antes de abrir a conexão, sem imprimir seu valor.
Também aceita PEM multilinha, quebras literais barra-n e bundles de certificados.
Não informar o caminho/nome do arquivo no lugar do conteúdo nem desabilitar TLS.

O usuário é fixo no código. Os parâmetros do notebook são secret_catalog
(padrão workspace) e secret_schema (padrão olius_bi); não passar senhas como
parâmetros. Não é necessário criar secret scope de workspace.
A identidade que executa o Job precisa de USE CATALOG, USE SCHEMA e READ SECRET
nas secrets necessárias. Restringir edição/execução do notebook e leitura das
secrets aos responsáveis; não conceder READ SECRET aos consumidores do dashboard.
Conferir permissões herdadas do catálogo/schema. Mascaramento não substitui
controle de acesso. [Secrets no Unity Catalog](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets).

Executar manualmente antes de agendar. O notebook precisa abrir conexão TCP
direta ao host/porta do Aiven, com verify-full. A CA temporária é removida ao
final. Federação funcionar não prova esse acesso no serverless.
Se rede/secrets não estiverem disponíveis, parar e revisar o executor; não
considerar o refresh automático homologado. Conferir snapshot_id emitido com
dm.refresh_state pela conexão federada.

## Criar um único Job

Em Jobs & Pipelines, criar um Job com tarefa Notebook apontando para
01_refresh.py e ambiente preparado. Não é necessário pipeline de ingestão.

Configurar:
- Máximo de execuções simultâneas: 1.
- Queueing desativado, evitando refreshes periódicos acumulados.
- Retry automático: 0 inicialmente; definir política após homologação.
- Timeout da tarefa: 20 minutos (o SQL tem statement_timeout de 15 minutos
  e lock_timeout de 30 segundos).
- Agenda/fuso America/Sao_Paulo conforme necessidade; intervalo maior que a
  duração observada. Não manter pg_cron ou segundo Job para o mesmo refresh.
- Notificação de falhas aos responsáveis, se disponível.

Atualizar o dashboard somente após sucesso da tarefa de refresh, usando o
recurso/task disponível no workspace com dependência All succeeded.
Conferir o snapshot_id emitido pelo notebook com dm.refresh_state na origem.

Não há IDs de dashboard/warehouse ou chamadas automáticas à API neste código.
Um horário independente com margem não garante sucesso do refresh anterior.
Se não houver encadeamento disponível, monitorar e exibir idade dos dados.
[Configuração de Jobs](https://docs.databricks.com/aws/en/jobs/configure-job).

Os dashboards continuam consultando diretamente o schema dm pelo catálogo
federado. Nenhum script adicional de criação de views no Databricks é necessário
para executar o refresh.

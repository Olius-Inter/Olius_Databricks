# Refresh, concorrência e validação

## Três atualizações

1. O Job Python executa dm.refresh_data_mart() no PostgreSQL e confirma as
   MVs e dm.refresh_state na mesma transação.
2. REFRESH FOREIGN SCHEMA sincroniza metadados do catálogo, não recalcula MVs.
3. Atualizar o dashboard reexecuta os datasets/resultados/cache. Não chama a
   procedure automaticamente.

## Consistência

01_refresh.py define REPEATABLE READ antes das consultas. As MVs usam a mesma
fotografia da origem. O contexto psycopg confirma ao sair sem erro e faz
rollback em caso de exceção. O sucesso é emitido somente após COMMIT.

pg_try_advisory_xact_lock(71007,1) na procedure rejeita chamadas concorrentes
que usem o mesmo bloqueio. Não espera nem faz retry. REPEATABLE READ não
deduplica execuções. Após a primeira terminar, uma segunda recalcula tudo
novamente, sem adicionar cópias das linhas. Comandos avulsos que ignorem o
lock não são coordenados por ele.

Se a conexão cair durante COMMIT, o resultado pode ser incerto: conferir
refresh_state antes de repetir. Não existe garantia de exactly-once.
O refresh normal pode bloquear leitores: medir impacto antes de agendar;
não trocar isoladamente por CONCURRENTLY.

snapshot_id muda a cada refresh confirmado. refreshed_at é registrado perto
do fim da procedure, antes do commit, não no carregamento do dashboard.
Gravações após a fotografia entram no próximo refresh.
Consultas independentes de datasets podem atravessar uma atualização e ver
snapshots diferentes; não há transação global do dashboard.

## Homologação

1. Conferir permissões com reader/refresh: reader consulta e não executa a
   procedure; refresh executa e consulta o controle, não as fatos.
2. Testar notebook no serverless: dependência, secrets, rede, TLS e transação.
3. Comparar snapshot anterior, output do Job e controle pelo catálogo federado.
4. Consultar dm.refresh_state e conferir snapshot_id e refreshed_at.
5. Atualizar dashboard após sucesso; comparar contagens/somas com SQL de
   referência e conferir filtros e ordenação do ranking.
6. Testar concorrência/rollback/timeout em banco DESCARTÁVEL, nunca provocar
   falhas deliberadas no Aiven compartilhado.
7. Só então agendar e monitorar falhas, duração e idade dos dados.

Testes locais com mocks verificam ordem dos comandos, estados inválidos,
falhas e propagação ao contexto de transação. Não comprovam commit real,
conectividade, grants do Aiven ou execução SQL no Databricks.
A regressão real PostgreSQL permanece em
PostgreSQL/tests/data_mart_access_regression.sql.

## Monitoramento e manutenção

Monitorar refreshed_at diretamente em dm.refresh_state.
Validar idade não prova que um snapshot veio de uma execução específica:
para isso, comparar snapshot_id emitido pelo Job.

Os antigos scripts de exportação, contrato Parquet, landing Delta e publicação
foram removidos desta branch e são recuperáveis pelo histórico Git.
A alteração não apaga objetos/dados remotos. Antes de qualquer limpeza no
workspace, levantar dependências e solicitar autorização específica.

[Transações psycopg](https://www.psycopg.org/psycopg3/docs/basic/transactions.html),
[Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

# Revisão local de segurança — 02/11/2026

Escopo: arquivos atuais de BI e Databricks, documentação, dependências declaradas
e regras de exclusão do Git. Não foram acessados bancos, serviços em nuvem ou
permissões do Databricks. O histórico Git e vulnerabilidades das dependências
instaladas não foram auditados. Ausência de achados não garante ausência de falhas.

## Alteração realizada

Criado `.gitignore` na raiz para ambientes Python, caches, arquivos `.env`,
configurações locais Databricks/PostgreSQL, chaves, certificados, logs, backups e
diretórios padrão de exportação. Exemplos `.env.example` e scripts SQL de carga
continuam visíveis. A regra `BI/.gitignore` existente foi preservada.

Regras verificadas com `git check-ignore --no-index`, incluindo casos que devem
continuar disponíveis para versionamento. Exportações em caminhos personalizados
precisam de regras próprias se forem gravadas dentro do repositório.

## Achados pendentes

1. **Médio — TLS não obrigatório nas conexões diretas do BI.**
   `BI/pipeline.py`, função `read_snapshot`, e `BI/load_demo.py`, função `main`,
   não exigem `sslmode=verify-full`. Dependem da configuração libpq externa.
   Sem configuração explícita, o padrão `prefer` não verifica a identidade do
   servidor e pode aceitar conexão sem TLS. A documentação recomenda a opção
   segura, mas o código não a impõe. Para bancos remotos, exigir validação de
   certificado/hostname e CA confiável, com exceção explícita para uso local.
   Fonte: https://www.postgresql.org/docs/17/libpq-ssl.html

2. **Atenção — usuário administrativo como padrão no modo Docker.**
   `BI/pipeline.py:read_snapshot` usa `postgres` quando nenhum usuário é informado.
   A consulta é somente leitura, mas as credenciais podem ter privilégios maiores
   que os necessários. Usar a conta `olius_bi_reader` nos dashboards; manter
   contas de carga separadas. Os privilégios reais do servidor não foram verificados.

3. **Atenção — tokens QR de demonstração previsíveis.**
   `BI/carga_nuvem_adicional.sql` gera QR com MD5 de identificadores determinísticos
   e um sufixo público. Não são credenciais reais vazadas, mas são previsíveis.
   Se a aplicação aceitar esses QR para autorizar operações, a massa não deve ser
   usada em ambiente real sem isolamento/inativação desses usuários. O marcador
   de senha sem login não comprova que o fluxo de QR também esteja bloqueado.

4. **Higiene — dois caches Python já estão versionados.**
   `Databricks/tests/__pycache__/test_medallion.cpython-314.pyc` e
   `Databricks/tests/__pycache__/test_refresh.cpython-314.pyc` constam em
   `git ls-files`. O `.gitignore` evita novos caches, mas não retira arquivos já
   rastreados. Nenhum arquivo foi removido e nenhuma mudança foi preparada no índice.

5. **Reprodutibilidade — dependências sem versões totalmente fixadas.**
   `BI/requirements.txt` usa intervalo para psycopg e não fixa tzdata. Isso permite
   mudanças entre instalações; não é evidência de vulnerabilidade conhecida.
   Avaliar arquivo de dependências travadas e hashes depois de validar o ambiente.
   Fonte: https://pip.pypa.io/en/stable/topics/repeatable-installs/

## Problema funcional após a migração

`BI/load_demo.py:build_sql` procura `PostgreSQL/data_load/config.json`,
`00_helpers.sql` e `01_seed.sql` dentro deste repositório. Esses arquivos ficaram
em DB_Segundo. A geração de ambas as modalidades de carga falha por arquivo
ausente. Definir explicitamente a origem dessa dependência em uma alteração
posterior; nenhum arquivo adicional foi movido ou copiado nesta revisão.

## Verificações e proteções observadas

- Não foram encontradas credenciais reais em texto claro na inspeção dos arquivos
  atuais e na busca por chaves privadas, tokens AWS/GitHub/Databricks e URLs
  PostgreSQL com senha. Os valores de teste e a massa sintética foram diferenciados.
- Sintaxe dos arquivos Python verificada sem gerar novos caches.
- Databricks: 18 testes existentes passaram, sem conexão à nuvem.
- BI: quatro testes de cálculo passaram; dois de geração de SQL falharam pela
  dependência ausente. Dois testes de integração foram ignorados por falta de
  banco selecionado. A tentativa de executar a suíte inteira foi interrompida
  após ficar sem progresso no teste de publicação em diretório temporário;
  esse teste não foi validado na execução inicial; ver resolução abaixo.
- O notebook de refresh usa secrets, usuário fixo restrito, `verify-full`, CA
  temporária e mensagens externas de erro sem detalhes de conexão.
- Os subprocessos são chamados por listas de argumentos, sem `shell=True`.
- A leitura dos KPIs usa transação somente leitura; as cargas usam transações
  e controles de destino/duplicação. O SQL adicional pronto não passa pela
  confirmação de destino do programa Python ao ser executado manualmente.
- A silver armazena nomes e registros completos rejeitados em quarentena.
  Acesso a essas tabelas deve ser restrito; os grants reais não foram verificados.

Somente `.gitignore` e este relatório foram adicionados. Código da aplicação
preservado; sem CI, Sonar, CodeRabbit, commit ou push.

## Atualização após a revisão

Por solicitação do usuário, `BI/load_demo.py` foi removido, junto com os dois
testes específicos do gerador e seus comandos no README. Os achados e resultados
acima descrevem o estado no momento da revisão. A dependência local em
`PostgreSQL/data_load` deixou de existir; o SQL autocontido foi preservado.

## Resolução do travamento no teste de publicação

A investigação reproduziu `PermissionError: acesso negado` ao criar uma
subpasta no diretório temporário do ambiente restrito de execução. No Windows,
`tempfile.mkdtemp` pode tratar esse erro como colisão de nomes e repetir as
tentativas, causando a aparente paralisação. A causa observada foi a restrição
de acesso do executor, sem evidência de falha na lógica de publicação.

A suíte de BI foi executada novamente com as permissões normais do Windows:
cinco testes passaram, incluindo reprocessamento e preservação do ponteiro
após falha de publicação; dois testes de integração foram ignorados por não
haver banco selecionado. Tempo total: 0,158 segundo. Nenhuma alteração no
código de publicação ou redução de permissões das pastas foi necessária.

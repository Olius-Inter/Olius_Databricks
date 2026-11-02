# Carga de demonstração e pipeline de BI

## Entrega

- `load_demo.py` + `demo_history.sql`: **780 visitas B2B**, **520 entregas PEV** e **825 solicitações**, além de 112 usuários, 30 estabelecimentos, 8 PEVs e 5 motoristas. Histórico dos 12 meses anteriores e do mês atual; nenhum fato físico futuro.
- `addresses.json`: cinco endereços comerciais reais em São Paulo, consultados em **28/10/2026**, com URLs das fontes oficiais. Os 38 registros de endereço reutilizam esses cinco locais. Não são 500 locais distintos: são mais de 500 operações. Cadastros pessoais, vínculos comerciais e condição de PEV são fictícios. Esses locais não foram identificados como pontos de coleta reais.
- `pipeline.py`: lê exclusivamente `dm`, calcula os KPIs e publica JSON/CSV para as visualizações. Não altera as fatos, permissões ou o Job de refresh já existente.

Os endereços ficam em `public.addresses`, com rua, número, bairro, cidade, UF e CEP. Coordenadas permanecem **NULL** para serem obtidas por geocodificação; não há coordenadas inventadas. Os cadastros continuam com nomes `[DEMO]`, documentos sintéticos, e-mails `.invalid` e sem senha de login. Não houve chamada ao MCP do Google Maps nesta validação; a evidência dos endereços está nas fontes oficiais do catálogo.

## Executar a carga

Requer PostgreSQL com os scripts `01_structure.sql` até `08_materialized_views.sql` já instalados, Python 3.10+ e, em conexão direta, `pip install -r BI/requirements.txt`. No Windows, se `ZoneInfo` não localizar o fuso, instale também `tzdata`.

### Banco em nuvem com dados existentes

Use **[carga_nuvem_adicional.sql](carga_nuvem_adicional.sql)**. Abra o arquivo e execute seu conteúdo completo no editor SQL conectado ao banco em nuvem, com uma conta administrativa de carga. Não exige Docker nem banco vazio. Se a tentativa anterior deixou a sessão em transação abortada, execute `ROLLBACK;` antes de iniciar este arquivo.

Essa versão adiciona **outra massa isolada de 1.300 operações** (780 coletas e 520 entregas), 825 solicitações e os cadastros necessários, preservando os registros já existentes. UUIDs, e-mails, documentos sintéticos e nomes de planos/categorias usam valores próprios da massa `BI CLOUD`. Somente os motivos de falha com os códigos oficiais são reutilizados; se estiverem inativos a execução é recusada, sem alterar o catálogo. Contagens e validações são limitadas à própria massa.

As inserções, cálculos e refresh de `dm` ficam em uma transação. Reexecução da mesma massa completa retorna “já existente” sem duplicar operações e sem mudar as datas. Uma massa parcialmente presente ou com contagens alteradas é recusada para revisão. Não remove nem sobrescreve registros; conflitos inesperados de unicidade também causam rollback.

O SQL foi testado localmente sobre a base já preenchida: passou de 112 para 224 usuários, de 780 para 1.560 coletas e de 520 para 1.040 entregas; todos os registros anteriores das 16 tabelas verificadas permaneceram iguais. Segunda execução manteve essas mesmas quantidades. Não foi executado pelo agente no serviço em nuvem.

Para regenerar o arquivo:

```powershell
python BI/load_demo.py --additive --sql-out BI/carga_nuvem_adicional.sql
```

Também pode ser carregado por conexão direta usando as variáveis libpq descritas abaixo e `--additive`. O pipeline Python e o Databricks passam a ler a soma dos dados existentes e da nova massa, após o refresh. O banco local de demonstração agora contém as duas massas usadas no teste de preservação; os números do primeiro processamento registrados ao fim deste guia referem-se à execução original.

### Banco vazio: carga original

Execute na raiz do repositório. O destino precisa estar **vazio** nas tabelas operacionais e de auditoria; a carga recusa dados existentes e não apaga nada. Use uma conta administrativa de carga e mantenha a API sem gravar nesse destino durante a importação. Os scripts existentes da pasta `PostgreSQL/data_load` são reutilizados para os cadastros; não é necessário executar a carga antiga antes.

Exemplo com PostgreSQL no Docker:

```powershell
$env:OLIUS_BI_DEMO_TARGET = 'SEU_CONTAINER/SEU_BANCO_VAZIO'
python BI/load_demo.py --container SEU_CONTAINER --database SEU_BANCO_VAZIO --user postgres
```

Conexão direta usa as variáveis padrão do libpq (`PGHOST`, `PGPORT`, `PGPASSWORD` ou `PGPASSFILE`, `PGSSLMODE`, `PGSSLROOTCERT`). Configure TLS conforme o seu serviço; nenhuma credencial é gravada no código ou nos relatórios.

```powershell
$env:PGHOST = 'HOST_DO_BANCO'
$env:PGPORT = '5432'
$env:PGSSLMODE = 'verify-full'
$env:PGSSLROOTCERT = 'C:\certificados\ca.pem'
$env:OLIUS_BI_DEMO_TARGET = 'HOST_DO_BANCO/SEU_BANCO_VAZIO'
python BI/load_demo.py --database SEU_BANCO_VAZIO --user USUARIO_DE_CARGA
```

Para revisar o SQL antes, sem abrir conexão:

```powershell
python BI/load_demo.py --sql-out coverage/bi/seed.sql
```

A carga inteira e o refresh de `dm` estão na **mesma transação**. Erro desfaz os cadastros e operações dessa carga. Reexecução após sucesso é recusada porque o banco deixou de estar vazio. Não desativa triggers, CHECKs ou chaves estrangeiras.

Os ciclos de assinatura e pedidos históricos são inseridos como fixtures acadêmicas com datas coerentes. Não representam pagamentos reais: **essa carga não simula o faturamento**. Os fatos são registrados por `record_collection` e `record_pev_delivery`; resultado, falhas, pontos, recorrência, saldos e auditoria continuam passando pelas rotinas oficiais. Cada ciclo respeita o plano Empresa (500 L / 8 visitas). Para testar pagamentos, use a carga de jornadas já existente em outra base.

## Executar o pipeline

No Docker (não requer psycopg no host):

```powershell
python BI/pipeline.py --container SEU_CONTAINER --database SEU_BANCO --user postgres
```

Conexão direta (mesmas variáveis libpq, com conta somente leitora de `dm`):

```powershell
python BI/pipeline.py --database SEU_BANCO --user olius_bi_reader
```

Fluxo: novas operações em `public` → `dm.refresh_data_mart()` pelo processo existente → `pipeline.py` → arquivos de visualização. Executar o pipeline sem atualizar o `dm` preserva a fotografia anterior, como esperado. Uma consulta em transação `REPEATABLE READ READ ONLY` lê o identificador e os dados da mesma fotografia. Não chama refresh com a conta leitora.

Depois da tarefa `Databricks/01_refresh.py`, execute o pipeline como tarefa Python dependente do sucesso do refresh, em ambiente com conexão ao PostgreSQL e acesso a um diretório persistente (`--output CAMINHO`). Não configura nem agenda Jobs automaticamente. Pode ser chamado externamente após cada refresh ou executado em modo de consulta contínua:

```powershell
python BI/pipeline.py --database SEU_BANCO --user olius_bi_reader --watch-seconds 60
```

Cada execução verifica `(snapshot_id, data de referência, ano, versão do cálculo)`. Recalcula ao mudar o snapshot **ou o dia**, para manter os cards de hoje corretos mesmo sem novas operações. Reexecução igual não duplica resultados. Erro interrompe o processo com código diferente de zero; o orquestrador deve controlar novas tentativas. Para o volume atual, relê o snapshot inteiro; não há soma incremental que perderia correções ou cancelamentos.

`--year 2025` seleciona a série anual. `--date 2026-10-28` fixa a data de corte para reprodução; o snapshot não pode conter fatos físicos posteriores ao corte. Essa opção não reconstrói estados históricos de solicitações. Os cards de agenda e pendências sempre refletem o snapshot atual e a data de referência, mesmo quando a série anual seleciona outro ano. `--force` republica mesmo sem mudança.

## Contrato dos KPIs

“DP” significa **desvio padrão populacional**. Variância e DP amostrais também são publicados, em campos próprios.

| KPI | Definição |
|---|---|
| Solicitações pendentes | Número com `status_id = PENDING`, independente do mês de criação. |
| Agendadas hoje | Pedidos `APPROVED`, com agendamento na data local e sem coleta vigente; inclui horários de hoje já passados ainda não atendidos. |
| Restantes no mês | Mesmos pedidos em datas de hoje até o fim do mês, inclusive hoje. Atrasos de dias anteriores ficam em `coletas_atrasadas`. Não é saldo contratual de vagas. |
| Coletas ao longo do ano / mensais | Visitas de `dm.fato_coleta` por mês da data física, inclusive visitas malsucedidas e de zero litros. O ano tem sempre 12 linhas. |
| Evolução de litros | Soma de `volume_coletado_litros` por mês B2B, diferença absoluta e percentual para o mês anterior. Sem base anterior ou base zero: percentual NULL. Meses futuros são marcados e não geram variação. |
| Média de litros B2B / DP | Estatísticas dos **totais mensais por estabelecimento** (`perfil=B2B`, `metrica=litros_por_participante`). |
| Média de litros PEV / DP | Estatísticas dos **totais mensais recebidos por PEV** (`perfil=PEV`, `metrica=litros_por_participante`). |
| Média de coletas B2B / DP | Estatísticas da **quantidade mensal de visitas por estabelecimento** (`eventos_por_participante`). |
| Média de coletas PEV / DP | Estatísticas da **quantidade mensal de entregas recebidas por PEV** (`eventos_por_participante`). O modelo não registra retirada física do óleo dos PEVs; esse KPI representa entregas B2C. |

O denominador por participante é o universo da dimensão atual: todos os estabelecimentos / PEVs, incluindo aqueles com zero eventos. Não há histórico de ativação nas dimensões para reconstruir o universo de cada mês; esse critério está explicitamente fixado para comparação. Para analisar apenas participantes com movimento, filtre `participantes.csv` antes de agregar. `litros_por_evento` fornece adicionalmente média e dispersão dos volumes de cada visita/entrega.

Cada grupo publica `n`, total, média, mediana, mínimo, máximo, variância populacional (`Σ(x−μ)²/n`), DP populacional (raiz da variância), desvio médio absoluto em torno da média (`Σ|x−μ|/n`), além das versões amostrais (`n−1`). Sem observações: média/dispersão NULL; uma observação: DP populacional zero e amostral NULL. Zero litros é uma observação válida. Anulações já estão excluídas pelas fatos do `dm`.

Os volumes B2B e PEV são mantidos **separados**: somá-los pode contar o mesmo óleo em etapas diferentes. Meses futuros têm contagem zero e `periodo_futuro=true`; filtre essa marca no gráfico. Estatísticas são calculadas em ponto flutuante de dupla precisão; totais mensais de litros e variações absolutas são arredondados a duas casas.

## Usar nas visualizações

O diretório padrão é `BI/output/` (ignorado no Git). Cada publicação cria uma pasta `run-*` contendo:

| Arquivo | Uso |
|---|---|
| `cards.csv` | Uma linha com os cards de pendências, agenda, ano e mês. |
| `mensal.csv` | 12 meses com quantidades, litros e variação. Eixo X: `periodo`; eixo Y: `quantidade_b2b` ou `litros_b2b`. |
| `estatisticas.csv` | 72 linhas (12 × 2 perfis × 3 métricas). Filtre perfil, métrica e mês para cada card de média/DP. |
| `participantes.csv` | Uma linha por mês/perfil/participante, incluindo zeros; permite auditoria e filtros. |
| `kpis.json` | Todos os resultados e metadados da fotografia. |

Primeiro leia `latest.json` e depois abra os arquivos da pasta indicada em `directory`. O ponteiro só muda depois que todos os arquivos foram gravados; uma falha não publica um conjunto parcial. Não busque o arquivo mais recente por glob, pois uma execução interrompida pode deixar uma pasta incompleta. Publicações antigas permanecem disponíveis.

## Testes e validação local

```powershell
python -m unittest discover -s BI -p test_pipeline.py -v
```

Os testes cobrem fórmulas com valores conhecidos, timezone, pedidos atendidos/cancelados/rejeitados, participantes sem movimento, meses vazios/futuros, fatos duplicados/órfãos/inválidos, filtro anual, reprocessamento e falha de publicação.

Para incluir as duas verificações de integração (somente leitura) contra a base com esta carga:

```powershell
$env:OLIUS_BI_TEST_CONTAINER = 'pgsql'
$env:OLIUS_BI_TEST_DATABASE = 'olius_bi_demo_20261028'
python -m unittest discover -s BI -p 'test_*.py' -v
```

**8 testes aprovados** nessa base. A integração confere contagens, os 13 meses, cronologia dos pedidos/ciclos, cálculos de pontos disponíveis e ausência de GPS inventado. Também compara média, mediana, DP, variância e desvio médio do Python com agregações independentes do PostgreSQL, para os dois perfis e as três métricas.

Em 28/10/2026, estrutura 01–08 e carga executadas na base separada **`olius_bi_demo_20261028`**, container local **`pgsql`**, sem alterar as bases anteriores. A conexão desse banco deve ser selecionada no ChatBot para usar a massa. No primeiro processamento: 15 pendentes, 10 agendadas hoje, 20 restantes no mês, 60 visitas no mês, 600 visitas em 2026 e 22.969,85 L recolhidos em 2026. Os 180 outros registros B2B pertencem a outubro–dezembro de 2025. Resultados reproduzíveis pela data/snapshot da execução; os cards de agenda mudam com o dia.

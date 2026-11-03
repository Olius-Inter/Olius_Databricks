# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — pipeline declarativa da Aula 5
# MAGIC Adicione este notebook à MESMA pipeline que silver_note. As leituras de silver
# MAGIC declaram as dependências; não é necessário ordenar notebooks manualmente.
# MAGIC Os cálculos continuam em pandas, executados por applyInPandas no plano Spark.
# MAGIC O framework cria/atualiza materialized views; não há gravações manuais.
# MAGIC
# MAGIC B2B conta visitas vigentes, inclusive falhas/zero litros. B2C conta entregas
# MAGIC recebidas em PEV; o dm não registra retirada física do PEV. Fluxos não são somados.
# MAGIC DP padrão é populacional; desvio médio é a média de |x-média|. Pontos ausentes
# MAGIC permanecem ausentes; o ranking oficial não é reconstruído pela soma nominal.
# MAGIC Médias por participante usam totais mensais e incluem zeros da dimensão atual.
# MAGIC Cadastros históricos não são reconstruídos. A agenda reflete o estado atual.
# MAGIC Períodos futuros são marcados; variação percentual sem base válida é NULL.
# MAGIC
# MAGIC Publica KPIs, séries, estatísticas, participantes, agenda, falhas, geografia,
# MAGIC detalhamentos, ranking e qualidade. _gold_pandas_lote é uma intermediária privada.
# MAGIC pipeline_state relaciona a gold ao snapshot bronze e à data de referência.
# MAGIC Não há transação atômica entre todas as materialized views: libere consumidores
# MAGIC após o SUCESSO da atualização completa da pipeline, pelo Job.
# MAGIC
# MAGIC Configuração: olius.silver_catalog/schema, olius.gold_catalog/schema,
# MAGIC olius.max_rows_per_table, olius.max_total_rows, olius.max_output_rows.
# MAGIC A data de referência é herdada da silver, garantindo um corte comum às camadas.
# MAGIC Veja documents/03_pipeline_medalhao.md. Não há dependência da pasta BI/.

# COMMAND ----------
import json
import uuid

import numpy as np
import pandas as pd

TIMEZONE = 'America/Sao_Paulo'
SILVER_TABLES = ['dim_estabelecimento', 'dim_cidadao', 'dim_motorista', 'dim_pev',
    'dim_classificacao_coleta', 'dim_status_solicitacao', 'dim_motivo_falha', 'dim_tempo',
    'fato_coleta', 'fato_entrega_pev', 'fato_solicitacao', 'ponte_coleta_motivo',
    'ranking_anual', 'qualidade_dados']
STAT_FIELDS = ['n', 'nulos', 'total', 'media', 'mediana', 'desvio_padrao', 'variancia',
    'desvio_medio', 'desvio_padrao_amostral', 'variancia_amostral', 'minimo', 'maximo',
    'percentil_25', 'percentil_75', 'amplitude', 'coeficiente_variacao_pct']


def describe(values):
    values = pd.to_numeric(values, errors='raise')
    clean = values.dropna().astype(float)
    result = {field: np.nan for field in STAT_FIELDS}
    result.update(n=len(clean), nulos=int(values.isna().sum()))
    if clean.empty:
        return result
    mean = clean.mean()
    result.update(total=clean.sum(), media=mean, mediana=clean.median(),
        desvio_padrao=clean.std(ddof=0), variancia=clean.var(ddof=0),
        desvio_medio=(clean - mean).abs().mean(),
        desvio_padrao_amostral=clean.std(ddof=1), variancia_amostral=clean.var(ddof=1),
        minimo=clean.min(), maximo=clean.max(), percentil_25=clean.quantile(.25),
        percentil_75=clean.quantile(.75), amplitude=clean.max() - clean.min(),
        coeficiente_variacao_pct=clean.std(ddof=0) / abs(mean) * 100 if mean else np.nan)
    return result


def statistics_table(frame, groups, metrics):
    rows = []
    for group, block in frame.groupby(groups, dropna=False, sort=True):
        group = group if isinstance(group, tuple) else (group,)
        for metric in metrics:
            rows.append(dict(zip(groups, group), metrica=metric, **describe(block[metric])))
    return pd.DataFrame(rows, columns=groups + ['metrica'] + STAT_FIELDS)


def prefixed(frame, key, prefix):
    return frame.rename(columns={name: prefix + name for name in frame if name != key})


def dated(frame):
    result = frame.copy()
    result['data_id'] = pd.to_datetime(result['data_id']).dt.normalize()
    result['ano'] = result['data_id'].dt.year
    result['mes'] = result['data_id'].dt.month
    result['periodo'] = result['data_id'].dt.strftime('%Y-%m')
    return result


def event_rollup(frame, groups):
    return frame.groupby(groups, dropna=False).agg(
        quantidade_eventos=('evento_id', 'size'), litros=('litros', 'sum'),
        pontos_nominais_disponiveis=('pontos_nominais', lambda s: s.sum(min_count=1)),
        eventos_sem_calculo=('pontos_nominais', lambda s: s.isna().sum()),
        sucessos=('sucesso', 'sum'), insucessos=('insucesso', 'sum')).reset_index()


def calculate_gold(silver, reference_date=None, max_output_rows=2000000):
    reference = pd.Timestamp(reference_date or pd.Timestamp.now(tz=TIMEZONE).date()).normalize()
    tables = {name: frame.copy() for name, frame in silver.items()}
    for name in ('fato_coleta', 'fato_entrega_pev', 'fato_solicitacao', 'dim_tempo'):
        tables[name]['data_id'] = pd.to_datetime(tables[name]['data_id']).dt.normalize()
    r = tables['fato_solicitacao']
    for field in ('solicitada_em', 'agendada_em', 'chegada_em', 'coletada_em', 'cancelada_em'):
        r[field] = pd.to_datetime(r[field], utc=True, errors='raise')
    c = tables['fato_coleta'].loc[tables['fato_coleta']['data_id'] <= reference].copy()
    d = tables['fato_entrega_pev'].loc[tables['fato_entrega_pev']['data_id'] <= reference].copy()

    # Relacionamentos com validate impedem multiplicar litros e contagens em joins.
    b2b = c.merge(tables['dim_classificacao_coleta'], on='classificacao_id', validate='many_to_one')
    b2b = b2b.merge(prefixed(tables['dim_estabelecimento'], 'estabelecimento_id', 'est_'),
                    on='estabelecimento_id', validate='many_to_one')
    b2b = b2b.merge(prefixed(tables['dim_motorista'], 'motorista_id', 'motorista_'),
                    on='motorista_id', validate='many_to_one')
    b2b = b2b.merge(prefixed(r, 'solicitacao_id', 'pedido_'), on='solicitacao_id', validate='one_to_one')
    b2b = dated(b2b)
    b2b['perfil'] = 'B2B'
    b2b['desvio_volume_litros'] = b2b['volume_coletado_litros'] - b2b['volume_estimado_litros']
    b2b['desvio_volume_pct'] = b2b['desvio_volume_litros'] / b2b['volume_estimado_litros'].replace(0, np.nan) * 100
    b2b['sucesso'] = b2b['resultado'].eq('SUCCESSFUL')
    b2b['insucesso'] = b2b['resultado'].eq('UNSUCCESSFUL')

    b2c = d.merge(prefixed(tables['dim_cidadao'], 'cidadao_id', 'cidadao_'), on='cidadao_id', validate='many_to_one')
    b2c = b2c.merge(prefixed(tables['dim_pev'], 'pev_id', 'pev_'), on='pev_id', validate='many_to_one')
    b2c = dated(b2c)
    b2c['perfil'] = 'B2C'
    requests = dated(r.merge(prefixed(tables['dim_estabelecimento'], 'estabelecimento_id', 'est_'),
                             on='estabelecimento_id', validate='many_to_one'))
    for field in ('solicitada_em', 'agendada_em', 'chegada_em', 'coletada_em', 'cancelada_em'):
        requests[field + '_data_local'] = requests[field].dt.tz_convert(TIMEZONE).dt.tz_localize(None).dt.normalize()
    requests['desvio_chegada_absoluto_minutos'] = requests['desvio_chegada_minutos'].abs()
    requests['atendida'] = requests['coletada_em'].notna()
    requests['aguardando_coleta'] = requests['status_id'].isin(['PENDING', 'APPROVED']) & ~requests['atendida']

    event_columns = ['evento_id', 'data_id', 'ano', 'mes', 'periodo', 'perfil', 'litros', 'pontos_nominais',
                     'sucesso', 'insucesso', 'estado', 'cidade', 'bairro']
    b_events = b2b.rename(columns={'coleta_id': 'evento_id', 'volume_coletado_litros': 'litros',
                                  'est_estado': 'estado', 'est_cidade': 'cidade', 'est_bairro': 'bairro'})
    d_events = b2c.rename(columns={'entrega_id': 'evento_id', 'volume_entregue_litros': 'litros',
                                  'pev_estado': 'estado', 'pev_cidade': 'cidade', 'pev_bairro': 'bairro'}).copy()
    # Entrega não possui classificação de sucesso/falha B2B; somas não são taxa B2C.
    d_events['sucesso'], d_events['insucesso'] = False, False
    events = pd.concat([b_events[event_columns], d_events[event_columns]], ignore_index=True)
    calendar = dated(tables['dim_tempo'])
    if calendar.empty:
        raise ValueError('Silver sem calendário válido')
    start = min(calendar['data_id'].min(), reference.replace(month=1, day=1))
    end = max(calendar['data_id'].max(), reference.replace(month=12, day=31))
    months = pd.date_range(start.replace(day=1), end.replace(day=1), freq='MS').strftime('%Y-%m').tolist()
    observed_months = [m for m in months if m <= reference.strftime('%Y-%m')]

    monthly = event_rollup(events, ['periodo', 'perfil'])
    month_index = pd.MultiIndex.from_product([months, ['B2B', 'B2C']], names=['periodo', 'perfil'])
    monthly = monthly.set_index(['periodo', 'perfil']).reindex(month_index).reset_index()
    for field in ('quantidade_eventos', 'litros', 'eventos_sem_calculo', 'sucessos', 'insucessos'):
        monthly[field] = monthly[field].fillna(0)
    monthly.loc[monthly['quantidade_eventos'].eq(0), 'pontos_nominais_disponiveis'] = 0
    monthly['ano'] = monthly['periodo'].str[:4].astype(int)
    monthly['mes'] = monthly['periodo'].str[5:].astype(int)
    monthly['periodo_futuro'] = monthly['periodo'] > reference.strftime('%Y-%m')
    monthly = monthly.sort_values(['perfil', 'periodo']).reset_index(drop=True)
    monthly['litros_mes_anterior'] = monthly.groupby('perfil')['litros'].shift()
    monthly['variacao_litros'] = monthly['litros'] - monthly['litros_mes_anterior']
    monthly['variacao_litros_pct'] = monthly['variacao_litros'] / monthly['litros_mes_anterior'].replace(0, np.nan) * 100
    monthly['eventos_mes_anterior'] = monthly.groupby('perfil')['quantidade_eventos'].shift()
    monthly['variacao_eventos_pct'] = ((monthly['quantidade_eventos'] - monthly['eventos_mes_anterior'])
                                      / monthly['eventos_mes_anterior'].replace(0, np.nan) * 100)
    monthly.loc[monthly['periodo_futuro'], ['variacao_litros', 'variacao_litros_pct', 'variacao_eventos_pct']] = np.nan
    monthly['litros_acumulados_ano'] = monthly.groupby(['perfil', 'ano'])['litros'].cumsum()
    monthly['eventos_acumulados_ano'] = monthly.groupby(['perfil', 'ano'])['quantidade_eventos'].cumsum()
    monthly['taxa_sucesso_b2b_pct'] = (monthly['sucessos'] / monthly['quantidade_eventos'].replace(0, np.nan) * 100).where(monthly['perfil'].eq('B2B'))
    daily = event_rollup(events, ['data_id', 'perfil'])
    day_index = pd.MultiIndex.from_product([pd.date_range(start, reference), ['B2B', 'B2C']], names=['data_id', 'perfil'])
    daily = daily.set_index(['data_id', 'perfil']).reindex(day_index).reset_index()
    for field in ('quantidade_eventos', 'litros', 'eventos_sem_calculo', 'sucessos', 'insucessos'):
        daily[field] = daily[field].fillna(0)
    daily.loc[daily['quantidade_eventos'].eq(0), 'pontos_nominais_disponiveis'] = 0
    annual = monthly.loc[~monthly['periodo_futuro']].groupby(['ano', 'perfil']).agg(
        quantidade_eventos=('quantidade_eventos', 'sum'), litros=('litros', 'sum'),
        pontos_nominais_disponiveis=('pontos_nominais_disponiveis', lambda s: s.sum(min_count=1)),
        eventos_sem_calculo=('eventos_sem_calculo', 'sum'), sucessos=('sucessos', 'sum'), insucessos=('insucessos', 'sum')).reset_index()

    participants = []
    participant_inputs = [
        ('B2B', 'B2B', tables['dim_estabelecimento'], 'estabelecimento_id', b_events),
        ('PEV', 'B2C', tables['dim_pev'], 'pev_id', d_events),
        ('CIDADAO', 'B2C', tables['dim_cidadao'], 'cidadao_id', d_events),
        ('MOTORISTA', 'B2B', tables['dim_motorista'], 'motorista_id', b_events),
    ]
    required_grid = sum(len(dim) * len(observed_months) for _, _, dim, _, _ in participant_inputs)
    if required_grid > max_output_rows:
        raise MemoryError('Grade participante × mês excede max_output_rows; nenhuma publicação foi feita')
    for kind, profile, dimension, key, source in participant_inputs:
        totals = event_rollup(source, ['periodo', key])
        index = pd.MultiIndex.from_product([observed_months, dimension[key]], names=['periodo', key])
        totals = totals.set_index(['periodo', key]).reindex(index).reset_index().rename(columns={key: 'participante_id'})
        for field in ('quantidade_eventos', 'litros', 'eventos_sem_calculo', 'sucessos', 'insucessos'):
            totals[field] = totals[field].fillna(0)
        totals.loc[totals['quantidade_eventos'].eq(0), 'pontos_nominais_disponiveis'] = 0
        totals['tipo_participante'], totals['perfil'] = kind, profile
        totals['ano'] = totals['periodo'].str[:4].astype(int)
        totals['mes'] = totals['periodo'].str[5:].astype(int)
        participants.append(totals)
    participants = pd.concat(participants, ignore_index=True)
    participant_stats = statistics_table(participants, ['periodo', 'perfil', 'tipo_participante'], ['litros', 'quantidade_eventos', 'pontos_nominais_disponiveis'])

    event_stats = []
    for level, group_columns in [('MES', ['periodo', 'perfil']), ('ANO', ['ano', 'perfil']), ('HISTORICO', ['perfil'])]:
        block = statistics_table(events, group_columns, ['litros', 'pontos_nominais'])
        block['granularidade'] = level
        if 'periodo' not in block:
            block['periodo'] = block['ano'].astype(str) if 'ano' in block else 'HISTORICO'
        event_stats.append(block.drop(columns='ano', errors='ignore'))
    event_stats = pd.concat(event_stats, ignore_index=True)
    # Volumes apresentados/estimados e desvios só existem no fluxo B2B.
    extra = statistics_table(b2b, ['periodo', 'perfil'],
        ['volume_coletado_litros', 'volume_apresentado_litros', 'volume_estimado_litros', 'desvio_volume_litros', 'desvio_volume_pct'])
    extra['granularidade'] = 'MES'
    event_stats = pd.concat([event_stats, extra], ignore_index=True)
    request_metrics = ['volume_estimado_litros', 'volume_franquia_perdida_litros', 'vagas_perdidas',
        'quantidade_reagendamentos', 'tempo_ate_coleta_horas', 'desvio_chegada_minutos', 'desvio_chegada_absoluto_minutos']
    request_stats = statistics_table(requests, ['periodo', 'status_id'], request_metrics)

    request_summary = requests.groupby(['periodo', 'status_id'], dropna=False).agg(
        quantidade_solicitacoes=('solicitacao_id', 'size'), estimativa_litros=('volume_estimado_litros', 'sum'),
        franquia_perdida_litros=('volume_franquia_perdida_litros', 'sum'), vagas_perdidas=('vagas_perdidas', 'sum'),
        reagendamentos=('quantidade_reagendamentos', 'sum'), atendidas=('atendida', 'sum'),
        tempo_medio_ate_coleta_horas=('tempo_ate_coleta_horas', 'mean'),
        desvio_medio_chegada_minutos=('desvio_chegada_minutos', 'mean')).reset_index()
    request_summary['participacao_status_pct'] = (request_summary['quantidade_solicitacoes']
        / request_summary.groupby('periodo')['quantidade_solicitacoes'].transform('sum').replace(0, np.nan) * 100)
    agenda = requests.loc[requests['aguardando_coleta']].copy()
    agenda['situacao_agenda'] = np.select([
        agenda['agendada_em'].isna(), agenda['agendada_em_data_local'] < reference,
        agenda['agendada_em_data_local'].eq(reference)], ['SEM_AGENDAMENTO', 'ATRASADA', 'HOJE'], default='FUTURA')
    bridge = tables['ponte_coleta_motivo'].merge(b2b[['coleta_id', 'periodo']], on='coleta_id', validate='many_to_one')
    bridge = bridge.merge(tables['dim_motivo_falha'], on='motivo_id', validate='many_to_one')
    reasons = bridge.groupby(['periodo', 'motivo_id', 'codigo', 'nome', 'descricao'], dropna=False).agg(
        quantidade_coletas=('coleta_id', 'nunique')).reset_index()
    classifications = b2b.groupby(['periodo', 'resultado', 'condicao_oleo', 'ocorrencia_comprometedora'], dropna=False).agg(
        quantidade_coletas=('coleta_id', 'size'), litros=('volume_coletado_litros', 'sum')).reset_index()
    geography = event_rollup(events, ['periodo', 'perfil', 'estado', 'cidade', 'bairro'])
    establishment_types = b2b.groupby(['periodo', 'est_tipo_estabelecimento'], dropna=False).agg(
        quantidade_coletas=('coleta_id', 'size'), litros=('volume_coletado_litros', 'sum'),
        estabelecimentos_com_movimento=('estabelecimento_id', 'nunique')).reset_index()

    # Cards: todas as medidas com unidade, denominador e data de referência.
    kpis = []

    def card(name, value, unit, definition, profile='B2B'):
        kpis.append(dict(metrica=name, perfil=profile, valor=value, unidade=unit,
                         definicao=definition, data_referencia=reference))

    approved = requests['status_id'].eq('APPROVED') & requests['coletada_em'].isna() & requests['agendada_em'].notna()
    scheduled_day = requests['agendada_em_data_local']
    month_end = reference + pd.offsets.MonthEnd(0)
    card('solicitacoes_pendentes', requests['status_id'].eq('PENDING').sum(), 'solicitacoes', 'Status PENDING no snapshot atual')
    card('coletas_agendadas_hoje', (approved & scheduled_day.eq(reference)).sum(), 'coletas', 'APPROVED, sem coleta, data agendada igual a hoje')
    card('coletas_restantes_mes', (approved & scheduled_day.between(reference, month_end)).sum(), 'coletas', 'Datas de hoje até o fim do mês; não é franquia restante')
    card('coletas_atrasadas', (approved & (scheduled_day < reference)).sum(), 'coletas', 'Agenda anterior a hoje, ainda sem coleta')
    current_period = reference.strftime('%Y-%m')
    for profile, source in [('B2B', b2b), ('B2C', b2c)]:
        volume = 'volume_coletado_litros' if profile == 'B2B' else 'volume_entregue_litros'
        in_month = source['periodo'].eq(current_period)
        in_year = source['ano'].eq(reference.year)
        for suffix, mask in [('mes', in_month), ('ano', in_year)]:
            card('coletas_' + suffix if profile == 'B2B' else 'entregas_pev_' + suffix,
                 mask.sum(), 'eventos', 'Visitas B2B / entregas B2C vigentes pela data física', profile)
            card('litros_' + suffix, source.loc[mask, volume].sum(), 'litros', 'Volume físico do perfil, sem somar os dois fluxos', profile)
        card('eventos_sem_calculo', source['pontos_nominais'].isna().sum(), 'eventos', 'Pontos nominais indisponíveis; não substituídos por zero', profile)
    for kind, profile in [('B2B', 'B2B'), ('PEV', 'B2C')]:
        block = participants.loc[participants['periodo'].eq(current_period) & participants['tipo_participante'].eq(kind)]
        for metric in ('litros', 'quantidade_eventos'):
            stats = describe(block[metric])
            for measure in ('media', 'mediana', 'desvio_padrao', 'variancia', 'desvio_medio'):
                card(f'{measure}_{metric}_por_{kind.lower()}', stats[measure], 'litros' if metric == 'litros' else 'eventos',
                     'Totais mensais por participante da dimensão atual, incluindo zeros', profile)
    month_b2b = b2b.loc[b2b['periodo'].eq(current_period)]
    card('taxa_sucesso_mes', month_b2b['sucesso'].mean() * 100 if len(month_b2b) else np.nan,
         'percentual', 'Visitas SUCCESSFUL / todas as visitas B2B do mês × 100')
    card('tempo_medio_ate_coleta_horas', requests['tempo_ate_coleta_horas'].mean(), 'horas', 'Média entre solicitações com coleta e tempo disponível')
    quality = tables['qualidade_dados']
    card('linhas_rejeitadas_silver', quality['linhas_rejeitadas'].sum(), 'linhas', 'Exclui duplicatas idênticas contabilizadas separadamente', 'QUALIDADE')
    card('duplicatas_removidas_silver', quality['duplicadas_identicas'].sum(), 'linhas', 'Cópias idênticas removidas na silver', 'QUALIDADE')
    cards = pd.DataFrame(kpis)
    catalog = cards[['metrica', 'perfil', 'unidade', 'definicao']].drop_duplicates().reset_index(drop=True)

    outputs = dict(detalhe_b2b=b2b, detalhe_b2c=b2c, detalhe_solicitacoes=requests,
        serie_diaria=daily, serie_mensal=monthly, serie_anual=annual,
        participantes_mensais=participants, estatisticas_eventos=event_stats,
        estatisticas_participantes=participant_stats, estatisticas_solicitacoes=request_stats,
        solicitacoes_mensais=request_summary, agenda_coletas=agenda,
        motivos_falha_mensais=reasons, classificacoes_mensais=classifications,
        geografia_mensal=geography, tipos_estabelecimento_mensais=establishment_types,
        kpis=cards, ranking_anual=tables['ranking_anual'], qualidade_silver=quality, catalogo_metricas=catalog)
    # Invariantes de reconciliação, sem somar B2B com B2C.
    for profile in ('B2B', 'B2C'):
        expected = events.loc[events['perfil'].eq(profile)]
        actual = monthly.loc[monthly['perfil'].eq(profile)]
        if int(actual['quantidade_eventos'].sum()) != len(expected) or not np.isclose(actual['litros'].sum(), expected['litros'].sum()):
            raise AssertionError('Reconciliação mensal falhou para ' + profile)
    if any(len(frame) > max_output_rows for frame in outputs.values()):
        raise MemoryError('Uma saída gold excedeu max_output_rows; publicação interrompida')
    return outputs

# COMMAND ----------
# Ponte de persistência autocontida: este notebook pode migrar sozinho.
def quoted(value):
    if not value or '\x00' in value:
        raise ValueError('Nome de objeto inválido')
    return '`' + value.replace('`', '``') + '`'


def full_name(catalog, schema, table):
    return '.'.join(quoted(value) for value in (catalog, schema, table))


def gold_schema(frame):
    # Números são DOUBLE na camada agregada, inclusive contagens. A silver mantém
    # LONG para identidades numéricas/contadores; UUIDs permanecem STRING em ambas.
    numeric_names = set(STAT_FIELDS + ['valor', 'pontos_nominais', 'pontos_anuais', 'posicao', 'ano', 'mes',
        'linhas_bronze', 'linhas_silver', 'duplicadas_identicas', 'linhas_rejeitadas', 'percentual_aproveitamento'])
    numeric_prefixes = ('volume_', 'quantidade_', 'tempo_', 'desvio_', 'vagas_', 'pontos_',
        'litros', 'eventos_', 'variacao_', 'taxa_', 'franquia_', 'estimativa_', 'reagendamentos',
        'atendidas', 'sucessos', 'insucessos', 'estabelecimentos_com_', 'participacao_')
    specs = {}
    for name, dtype in frame.dtypes.items():
        base_name = name.removeprefix('pedido_')
        if pd.api.types.is_bool_dtype(dtype) or name in ('periodo_futuro', 'sucesso', 'insucesso', 'atendida', 'aguardando_coleta'):
            kind = 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            kind = 'TIMESTAMP'
        elif pd.api.types.is_numeric_dtype(dtype) or base_name in numeric_names or base_name.startswith(numeric_prefixes):
            kind = 'DOUBLE'
        else:
            kind = 'STRING'
        specs[name] = kind
    return specs



GOLD_INPUT_SCHEMAS = {'dim_estabelecimento': {'estabelecimento_id': 'STRING',
                         'nome': 'STRING',
                         'tipo_estabelecimento': 'STRING',
                         'estado': 'STRING',
                         'cidade': 'STRING',
                         'bairro': 'STRING',
                         'latitude': 'DOUBLE',
                         'longitude': 'DOUBLE',
                         'is_pev': 'BOOLEAN',
                         'status': 'STRING'},
 'dim_cidadao': {'cidadao_id': 'STRING', 'nome': 'STRING', 'status': 'STRING'},
 'dim_motorista': {'motorista_id': 'STRING', 'nome': 'STRING', 'status': 'STRING'},
 'dim_pev': {'pev_id': 'STRING',
             'status': 'STRING',
             'estado': 'STRING',
             'cidade': 'STRING',
             'bairro': 'STRING',
             'latitude': 'DOUBLE',
             'longitude': 'DOUBLE'},
 'dim_classificacao_coleta': {'classificacao_id': 'STRING',
                              'resultado': 'STRING',
                              'condicao_oleo': 'STRING',
                              'ocorrencia_comprometedora': 'BOOLEAN'},
 'dim_status_solicitacao': {'status_id': 'STRING'},
 'dim_motivo_falha': {'motivo_id': 'STRING', 'codigo': 'STRING', 'nome': 'STRING', 'descricao': 'STRING'},
 'dim_tempo': {'data_id': 'DATE',
               'ano': 'LONG',
               'trimestre': 'LONG',
               'mes': 'LONG',
               'dia': 'LONG',
               'dia_semana': 'LONG'},
 'fato_coleta': {'coleta_id': 'STRING',
                 'data_id': 'DATE',
                 'estabelecimento_id': 'STRING',
                 'motorista_id': 'STRING',
                 'classificacao_id': 'STRING',
                 'solicitacao_id': 'STRING',
                 'volume_coletado_litros': 'DOUBLE',
                 'volume_apresentado_litros': 'DOUBLE',
                 'volume_estimado_litros': 'DOUBLE',
                 'pontos_nominais': 'LONG',
                 'calculo_vigente_disponivel': 'BOOLEAN',
                 'quantidade_coletas': 'LONG'},
 'fato_entrega_pev': {'entrega_id': 'STRING',
                      'data_id': 'DATE',
                      'cidadao_id': 'STRING',
                      'pev_id': 'STRING',
                      'volume_entregue_litros': 'DOUBLE',
                      'pontos_nominais': 'LONG',
                      'calculo_vigente_disponivel': 'BOOLEAN',
                      'quantidade_entregas': 'LONG'},
 'fato_solicitacao': {'solicitacao_id': 'STRING',
                      'data_id': 'DATE',
                      'estabelecimento_id': 'STRING',
                      'status_id': 'STRING',
                      'solicitada_em': 'TIMESTAMP',
                      'agendada_em': 'TIMESTAMP',
                      'chegada_em': 'TIMESTAMP',
                      'coletada_em': 'TIMESTAMP',
                      'cancelada_em': 'TIMESTAMP',
                      'volume_estimado_litros': 'DOUBLE',
                      'volume_franquia_perdida_litros': 'DOUBLE',
                      'vagas_perdidas': 'LONG',
                      'quantidade_solicitacoes': 'LONG',
                      'quantidade_reagendamentos': 'LONG',
                      'tempo_ate_coleta_horas': 'DOUBLE',
                      'desvio_chegada_minutos': 'DOUBLE',
                      'iniciativa_cancelamento': 'STRING',
                      'politica_cancelamento': 'STRING'},
 'ponte_coleta_motivo': {'coleta_id': 'STRING', 'motivo_id': 'STRING'},
 'ranking_anual': {'perfil': 'STRING',
                   'ano': 'LONG',
                   'participante_id': 'STRING',
                   'pontos_anuais': 'DOUBLE',
                   'posicao': 'LONG'},
 'qualidade_dados': {'tabela': 'STRING',
                     'linhas_bronze': 'LONG',
                     'linhas_silver': 'LONG',
                     'duplicadas_identicas': 'LONG',
                     'linhas_rejeitadas': 'LONG',
                     'percentual_aproveitamento': 'DOUBLE'}}
GOLD_OUTPUT_SCHEMAS = {'detalhe_b2b': {'coleta_id': 'STRING',
                 'data_id': 'TIMESTAMP',
                 'estabelecimento_id': 'STRING',
                 'motorista_id': 'STRING',
                 'classificacao_id': 'STRING',
                 'solicitacao_id': 'STRING',
                 'volume_coletado_litros': 'DOUBLE',
                 'volume_apresentado_litros': 'DOUBLE',
                 'volume_estimado_litros': 'DOUBLE',
                 'pontos_nominais': 'DOUBLE',
                 'calculo_vigente_disponivel': 'BOOLEAN',
                 'quantidade_coletas': 'DOUBLE',
                 'resultado': 'STRING',
                 'condicao_oleo': 'STRING',
                 'ocorrencia_comprometedora': 'BOOLEAN',
                 'est_nome': 'STRING',
                 'est_tipo_estabelecimento': 'STRING',
                 'est_estado': 'STRING',
                 'est_cidade': 'STRING',
                 'est_bairro': 'STRING',
                 'est_latitude': 'DOUBLE',
                 'est_longitude': 'DOUBLE',
                 'est_is_pev': 'BOOLEAN',
                 'est_status': 'STRING',
                 'motorista_nome': 'STRING',
                 'motorista_status': 'STRING',
                 'pedido_data_id': 'TIMESTAMP',
                 'pedido_estabelecimento_id': 'STRING',
                 'pedido_status_id': 'STRING',
                 'pedido_solicitada_em': 'TIMESTAMP',
                 'pedido_agendada_em': 'TIMESTAMP',
                 'pedido_chegada_em': 'TIMESTAMP',
                 'pedido_coletada_em': 'TIMESTAMP',
                 'pedido_cancelada_em': 'TIMESTAMP',
                 'pedido_volume_estimado_litros': 'DOUBLE',
                 'pedido_volume_franquia_perdida_litros': 'DOUBLE',
                 'pedido_vagas_perdidas': 'DOUBLE',
                 'pedido_quantidade_solicitacoes': 'DOUBLE',
                 'pedido_quantidade_reagendamentos': 'DOUBLE',
                 'pedido_tempo_ate_coleta_horas': 'DOUBLE',
                 'pedido_desvio_chegada_minutos': 'DOUBLE',
                 'pedido_iniciativa_cancelamento': 'STRING',
                 'pedido_politica_cancelamento': 'STRING',
                 'ano': 'DOUBLE',
                 'mes': 'DOUBLE',
                 'periodo': 'STRING',
                 'perfil': 'STRING',
                 'desvio_volume_litros': 'DOUBLE',
                 'desvio_volume_pct': 'DOUBLE',
                 'sucesso': 'BOOLEAN',
                 'insucesso': 'BOOLEAN'},
 'detalhe_b2c': {'entrega_id': 'STRING',
                 'data_id': 'TIMESTAMP',
                 'cidadao_id': 'STRING',
                 'pev_id': 'STRING',
                 'volume_entregue_litros': 'DOUBLE',
                 'pontos_nominais': 'DOUBLE',
                 'calculo_vigente_disponivel': 'BOOLEAN',
                 'quantidade_entregas': 'DOUBLE',
                 'cidadao_nome': 'STRING',
                 'cidadao_status': 'STRING',
                 'pev_status': 'STRING',
                 'pev_estado': 'STRING',
                 'pev_cidade': 'STRING',
                 'pev_bairro': 'STRING',
                 'pev_latitude': 'DOUBLE',
                 'pev_longitude': 'DOUBLE',
                 'ano': 'DOUBLE',
                 'mes': 'DOUBLE',
                 'periodo': 'STRING',
                 'perfil': 'STRING'},
 'detalhe_solicitacoes': {'solicitacao_id': 'STRING',
                          'data_id': 'TIMESTAMP',
                          'estabelecimento_id': 'STRING',
                          'status_id': 'STRING',
                          'solicitada_em': 'TIMESTAMP',
                          'agendada_em': 'TIMESTAMP',
                          'chegada_em': 'TIMESTAMP',
                          'coletada_em': 'TIMESTAMP',
                          'cancelada_em': 'TIMESTAMP',
                          'volume_estimado_litros': 'DOUBLE',
                          'volume_franquia_perdida_litros': 'DOUBLE',
                          'vagas_perdidas': 'DOUBLE',
                          'quantidade_solicitacoes': 'DOUBLE',
                          'quantidade_reagendamentos': 'DOUBLE',
                          'tempo_ate_coleta_horas': 'DOUBLE',
                          'desvio_chegada_minutos': 'DOUBLE',
                          'iniciativa_cancelamento': 'STRING',
                          'politica_cancelamento': 'STRING',
                          'est_nome': 'STRING',
                          'est_tipo_estabelecimento': 'STRING',
                          'est_estado': 'STRING',
                          'est_cidade': 'STRING',
                          'est_bairro': 'STRING',
                          'est_latitude': 'DOUBLE',
                          'est_longitude': 'DOUBLE',
                          'est_is_pev': 'BOOLEAN',
                          'est_status': 'STRING',
                          'ano': 'DOUBLE',
                          'mes': 'DOUBLE',
                          'periodo': 'STRING',
                          'solicitada_em_data_local': 'TIMESTAMP',
                          'agendada_em_data_local': 'TIMESTAMP',
                          'chegada_em_data_local': 'TIMESTAMP',
                          'coletada_em_data_local': 'TIMESTAMP',
                          'cancelada_em_data_local': 'TIMESTAMP',
                          'desvio_chegada_absoluto_minutos': 'DOUBLE',
                          'atendida': 'BOOLEAN',
                          'aguardando_coleta': 'BOOLEAN'},
 'serie_diaria': {'data_id': 'TIMESTAMP',
                  'perfil': 'STRING',
                  'quantidade_eventos': 'DOUBLE',
                  'litros': 'DOUBLE',
                  'pontos_nominais_disponiveis': 'DOUBLE',
                  'eventos_sem_calculo': 'DOUBLE',
                  'sucessos': 'DOUBLE',
                  'insucessos': 'DOUBLE'},
 'serie_mensal': {'periodo': 'STRING',
                  'perfil': 'STRING',
                  'quantidade_eventos': 'DOUBLE',
                  'litros': 'DOUBLE',
                  'pontos_nominais_disponiveis': 'DOUBLE',
                  'eventos_sem_calculo': 'DOUBLE',
                  'sucessos': 'DOUBLE',
                  'insucessos': 'DOUBLE',
                  'ano': 'DOUBLE',
                  'mes': 'DOUBLE',
                  'periodo_futuro': 'BOOLEAN',
                  'litros_mes_anterior': 'DOUBLE',
                  'variacao_litros': 'DOUBLE',
                  'variacao_litros_pct': 'DOUBLE',
                  'eventos_mes_anterior': 'DOUBLE',
                  'variacao_eventos_pct': 'DOUBLE',
                  'litros_acumulados_ano': 'DOUBLE',
                  'eventos_acumulados_ano': 'DOUBLE',
                  'taxa_sucesso_b2b_pct': 'DOUBLE'},
 'serie_anual': {'ano': 'DOUBLE',
                 'perfil': 'STRING',
                 'quantidade_eventos': 'DOUBLE',
                 'litros': 'DOUBLE',
                 'pontos_nominais_disponiveis': 'DOUBLE',
                 'eventos_sem_calculo': 'DOUBLE',
                 'sucessos': 'DOUBLE',
                 'insucessos': 'DOUBLE'},
 'participantes_mensais': {'periodo': 'STRING',
                           'participante_id': 'STRING',
                           'quantidade_eventos': 'DOUBLE',
                           'litros': 'DOUBLE',
                           'pontos_nominais_disponiveis': 'DOUBLE',
                           'eventos_sem_calculo': 'DOUBLE',
                           'sucessos': 'DOUBLE',
                           'insucessos': 'DOUBLE',
                           'tipo_participante': 'STRING',
                           'perfil': 'STRING',
                           'ano': 'DOUBLE',
                           'mes': 'DOUBLE'},
 'estatisticas_eventos': {'periodo': 'STRING',
                          'perfil': 'STRING',
                          'metrica': 'STRING',
                          'n': 'DOUBLE',
                          'nulos': 'DOUBLE',
                          'total': 'DOUBLE',
                          'media': 'DOUBLE',
                          'mediana': 'DOUBLE',
                          'desvio_padrao': 'DOUBLE',
                          'variancia': 'DOUBLE',
                          'desvio_medio': 'DOUBLE',
                          'desvio_padrao_amostral': 'DOUBLE',
                          'variancia_amostral': 'DOUBLE',
                          'minimo': 'DOUBLE',
                          'maximo': 'DOUBLE',
                          'percentil_25': 'DOUBLE',
                          'percentil_75': 'DOUBLE',
                          'amplitude': 'DOUBLE',
                          'coeficiente_variacao_pct': 'DOUBLE',
                          'granularidade': 'STRING'},
 'estatisticas_participantes': {'periodo': 'STRING',
                                'perfil': 'STRING',
                                'tipo_participante': 'STRING',
                                'metrica': 'STRING',
                                'n': 'DOUBLE',
                                'nulos': 'DOUBLE',
                                'total': 'DOUBLE',
                                'media': 'DOUBLE',
                                'mediana': 'DOUBLE',
                                'desvio_padrao': 'DOUBLE',
                                'variancia': 'DOUBLE',
                                'desvio_medio': 'DOUBLE',
                                'desvio_padrao_amostral': 'DOUBLE',
                                'variancia_amostral': 'DOUBLE',
                                'minimo': 'DOUBLE',
                                'maximo': 'DOUBLE',
                                'percentil_25': 'DOUBLE',
                                'percentil_75': 'DOUBLE',
                                'amplitude': 'DOUBLE',
                                'coeficiente_variacao_pct': 'DOUBLE'},
 'estatisticas_solicitacoes': {'periodo': 'STRING',
                               'status_id': 'STRING',
                               'metrica': 'STRING',
                               'n': 'DOUBLE',
                               'nulos': 'DOUBLE',
                               'total': 'DOUBLE',
                               'media': 'DOUBLE',
                               'mediana': 'DOUBLE',
                               'desvio_padrao': 'DOUBLE',
                               'variancia': 'DOUBLE',
                               'desvio_medio': 'DOUBLE',
                               'desvio_padrao_amostral': 'DOUBLE',
                               'variancia_amostral': 'DOUBLE',
                               'minimo': 'DOUBLE',
                               'maximo': 'DOUBLE',
                               'percentil_25': 'DOUBLE',
                               'percentil_75': 'DOUBLE',
                               'amplitude': 'DOUBLE',
                               'coeficiente_variacao_pct': 'DOUBLE'},
 'solicitacoes_mensais': {'periodo': 'STRING',
                          'status_id': 'STRING',
                          'quantidade_solicitacoes': 'DOUBLE',
                          'estimativa_litros': 'DOUBLE',
                          'franquia_perdida_litros': 'DOUBLE',
                          'vagas_perdidas': 'DOUBLE',
                          'reagendamentos': 'DOUBLE',
                          'atendidas': 'DOUBLE',
                          'tempo_medio_ate_coleta_horas': 'DOUBLE',
                          'desvio_medio_chegada_minutos': 'DOUBLE',
                          'participacao_status_pct': 'DOUBLE'},
 'agenda_coletas': {'solicitacao_id': 'STRING',
                    'data_id': 'TIMESTAMP',
                    'estabelecimento_id': 'STRING',
                    'status_id': 'STRING',
                    'solicitada_em': 'TIMESTAMP',
                    'agendada_em': 'TIMESTAMP',
                    'chegada_em': 'TIMESTAMP',
                    'coletada_em': 'TIMESTAMP',
                    'cancelada_em': 'TIMESTAMP',
                    'volume_estimado_litros': 'DOUBLE',
                    'volume_franquia_perdida_litros': 'DOUBLE',
                    'vagas_perdidas': 'DOUBLE',
                    'quantidade_solicitacoes': 'DOUBLE',
                    'quantidade_reagendamentos': 'DOUBLE',
                    'tempo_ate_coleta_horas': 'DOUBLE',
                    'desvio_chegada_minutos': 'DOUBLE',
                    'iniciativa_cancelamento': 'STRING',
                    'politica_cancelamento': 'STRING',
                    'est_nome': 'STRING',
                    'est_tipo_estabelecimento': 'STRING',
                    'est_estado': 'STRING',
                    'est_cidade': 'STRING',
                    'est_bairro': 'STRING',
                    'est_latitude': 'DOUBLE',
                    'est_longitude': 'DOUBLE',
                    'est_is_pev': 'BOOLEAN',
                    'est_status': 'STRING',
                    'ano': 'DOUBLE',
                    'mes': 'DOUBLE',
                    'periodo': 'STRING',
                    'solicitada_em_data_local': 'TIMESTAMP',
                    'agendada_em_data_local': 'TIMESTAMP',
                    'chegada_em_data_local': 'TIMESTAMP',
                    'coletada_em_data_local': 'TIMESTAMP',
                    'cancelada_em_data_local': 'TIMESTAMP',
                    'desvio_chegada_absoluto_minutos': 'DOUBLE',
                    'atendida': 'BOOLEAN',
                    'aguardando_coleta': 'BOOLEAN',
                    'situacao_agenda': 'STRING'},
 'motivos_falha_mensais': {'periodo': 'STRING',
                           'motivo_id': 'STRING',
                           'codigo': 'STRING',
                           'nome': 'STRING',
                           'descricao': 'STRING',
                           'quantidade_coletas': 'DOUBLE'},
 'classificacoes_mensais': {'periodo': 'STRING',
                            'resultado': 'STRING',
                            'condicao_oleo': 'STRING',
                            'ocorrencia_comprometedora': 'BOOLEAN',
                            'quantidade_coletas': 'DOUBLE',
                            'litros': 'DOUBLE'},
 'geografia_mensal': {'periodo': 'STRING',
                      'perfil': 'STRING',
                      'estado': 'STRING',
                      'cidade': 'STRING',
                      'bairro': 'STRING',
                      'quantidade_eventos': 'DOUBLE',
                      'litros': 'DOUBLE',
                      'pontos_nominais_disponiveis': 'DOUBLE',
                      'eventos_sem_calculo': 'DOUBLE',
                      'sucessos': 'DOUBLE',
                      'insucessos': 'DOUBLE'},
 'tipos_estabelecimento_mensais': {'periodo': 'STRING',
                                   'est_tipo_estabelecimento': 'STRING',
                                   'quantidade_coletas': 'DOUBLE',
                                   'litros': 'DOUBLE',
                                   'estabelecimentos_com_movimento': 'DOUBLE'},
 'kpis': {'metrica': 'STRING',
          'perfil': 'STRING',
          'valor': 'DOUBLE',
          'unidade': 'STRING',
          'definicao': 'STRING',
          'data_referencia': 'TIMESTAMP'},
 'ranking_anual': {'perfil': 'STRING',
                   'ano': 'DOUBLE',
                   'participante_id': 'STRING',
                   'pontos_anuais': 'DOUBLE',
                   'posicao': 'DOUBLE'},
 'qualidade_silver': {'tabela': 'STRING',
                      'linhas_bronze': 'DOUBLE',
                      'linhas_silver': 'DOUBLE',
                      'duplicadas_identicas': 'DOUBLE',
                      'linhas_rejeitadas': 'DOUBLE',
                      'percentual_aproveitamento': 'DOUBLE'},
 'catalogo_metricas': {'metrica': 'STRING', 'perfil': 'STRING', 'unidade': 'STRING', 'definicao': 'STRING'},
 'pipeline_state': {'snapshot_bronze': 'STRING',
                    'data_referencia': 'STRING',
                    'linhas_quarentena': 'LONG',
                    'versao': 'STRING'}}

# COMMAND ----------
def gold_json_records(frame, schema):
    def scalar(value, kind):
        if pd.isna(value):
            return None
        if kind == 'DOUBLE':
            return float(value)
        if kind == 'LONG':
            return int(value)
        if kind == 'BOOLEAN':
            return bool(value)
        if kind == 'TIMESTAMP':
            stamp = pd.Timestamp(value)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize('UTC')
            return stamp.isoformat()
        return str(value)
    return [json.dumps({field: scalar(row[field], kind) for field, kind in schema.items()},
                       ensure_ascii=False, allow_nan=False) for row in frame.to_dict('records')]


def gold_envelope(batch, max_rows=200000, max_total_rows=500000, max_output_rows=2000000):
    """Reconstitui as tabelas tipadas da silver e calcula as 20 saídas em pandas."""
    present = batch.loc[batch['registro_json'].notna()]
    if len(present) > max_total_rows:
        raise MemoryError('Lote silver excedeu olius.max_total_rows')
    controls = present.loc[present['tabela'].eq('pipeline_state'), 'registro_json']
    if len(controls) != 1:
        raise ValueError('Silver sem pipeline_state único')
    state = json.loads(controls.iloc[0])
    reference = pd.Timestamp(state['data_referencia']).date().isoformat()
    tables = {}
    for name in SILVER_TABLES:
        subset = present.loc[present['tabela'].eq(name), 'registro_json']
        if len(subset) > max_rows:
            raise MemoryError(name + ': excedeu olius.max_rows_per_table')
        frame = pd.DataFrame([json.loads(value) for value in subset], columns=list(GOLD_INPUT_SCHEMAS[name]))
        for field, kind in GOLD_INPUT_SCHEMAS[name].items():
            if kind in ('LONG', 'DOUBLE'):
                frame[field] = pd.to_numeric(frame[field], errors='raise').astype('Int64' if kind == 'LONG' else 'Float64')
            elif kind == 'BOOLEAN':
                frame[field] = frame[field].astype('boolean')
            elif kind in ('DATE', 'TIMESTAMP'):
                frame[field] = pd.to_datetime(frame[field], utc=(kind == 'TIMESTAMP'), errors='raise', format='mixed')
            else:
                frame[field] = frame[field].astype('string')
        tables[name] = frame
    quality = tables['qualidade_dados'].set_index('tabela')
    for name in SILVER_TABLES:
        if name != 'qualidade_dados' and (name not in quality.index or int(quality.loc[name, 'linhas_silver']) != len(tables[name])):
            raise ValueError(name + ': contagem incompatível com qualidade_dados')
    outputs = calculate_gold(tables, reference, max_output_rows)
    outputs['pipeline_state'] = pd.DataFrame([dict(snapshot_bronze=state['snapshot_bronze'],
        data_referencia=reference, linhas_quarentena=state['linhas_quarentena'], versao='2-declarativa')])
    rows = [(name, value) for name, frame in outputs.items()
            for value in gold_json_records(frame, GOLD_OUTPUT_SCHEMAS[name])]
    if len(rows) > max_output_rows:
        raise MemoryError('Conjunto das saídas gold excedeu olius.max_output_rows')
    return pd.DataFrame(rows, columns=['tabela', 'registro_json'])


def register_gold_pipeline(spark_session):
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    config = lambda key, default: spark_session.conf.get('olius.' + key, default).strip()
    source_catalog, source_schema = config('silver_catalog', 'silver'), config('silver_schema', 'olius')
    catalog, schema = config('gold_catalog', 'gold'), config('gold_schema', 'olius')
    if (source_catalog.lower(), source_schema.lower()) == (catalog.lower(), schema.lower()):
        raise ValueError('Silver e gold precisam usar destinos diferentes')
    max_rows = int(config('max_rows_per_table', '200000'))
    max_total = int(config('max_total_rows', '500000'))
    max_output = int(config('max_output_rows', '2000000'))
    if min(max_rows, max_total, max_output) < 1:
        raise ValueError('Limites devem ser positivos')
    internal = full_name(catalog, schema, '_gold_pandas_lote')

    @dp.materialized_view(name=internal, private=True, comment='Cálculos pandas sobre o lote silver')
    def gold_pandas_lote():
        parts = []
        for name in SILVER_TABLES + ['pipeline_state']:
            source = spark_session.read.table(full_name(source_catalog, source_schema, name)).limit(max_rows + 1)
            parts.append(source.select(F.lit(name).alias('tabela'),
                F.to_json(F.struct('*'), options={'ignoreNullFields': 'false'}).alias('registro_json')))
        combined = parts[0]
        for part in parts[1:]:
            combined = combined.unionByName(part)
        def process(batch):
            return gold_envelope(batch, max_rows, max_total, max_output)
        return combined.withColumn('_lote', F.lit(1)).groupBy('_lote').applyInPandas(
            process, schema='tabela STRING, registro_json STRING')

    def declare_output(name, fields):
        ddl = ', '.join(f'{quoted(field)} {kind}' for field, kind in fields.items())
        @dp.materialized_view(name=full_name(catalog, schema, name), comment='Gold calculada com pandas: ' + name)
        def gold_output():
            source = spark_session.read.table(internal).where(F.col('tabela') == F.lit(name))
            return source.select(F.from_json('registro_json', ddl).alias('registro')).select('registro.*')
        return gold_output
    return [declare_output(name, fields) for name, fields in GOLD_OUTPUT_SCHEMAS.items()]


if 'spark' in globals():
    register_gold_pipeline(spark)  # noqa: F821 — sessão gerenciada pela pipeline

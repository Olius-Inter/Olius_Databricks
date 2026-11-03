# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — pipeline declarativa da Aula 5
# MAGIC Adicione este notebook e gold_note como fontes de UMA pipeline ETL no
# MAGIC Databricks. A bronze já é o catálogo federado que expõe o schema dm.
# MAGIC O framework controla materialização, dependências e atualização dos resultados.
# MAGIC Não execute como notebook comum nem use gravações manuais dentro da pipeline.
# MAGIC
# MAGIC As regras continuam em pandas. applyInPandas executa a transformação como
# MAGIC parte do plano Spark, sem trazer dados ao driver. Para validar relações entre
# MAGIC os 13 conjuntos, o lote é processado em um único grupo: é uma solução acadêmica
# MAGIC de volume limitado, não uma implementação distribuída para grandes massas.
# MAGIC Limites não autorizam amostragem: lote acima do limite falha sem publicar cortes.
# MAGIC
# MAGIC Tipos, nulos, domínios, volumes, duplicações, referências e regras temporais
# MAGIC são conferidos novamente. Negativos legítimos (penalidade e chegada antecipada)
# MAGIC são preservados. Rejeitados e cópias idênticas ficam em quarentena.
# MAGIC A materialized view privada _silver_pandas_lote é apenas intermediária.
# MAGIC Não existem mais _dados_* ou ponteiros de publicação escritos pelo notebook.
# MAGIC pipeline_state agora é metadado declarativo da origem processada.
# MAGIC
# MAGIC Configuração: olius.bronze_catalog, olius.bronze_schema, olius.silver_catalog,
# MAGIC olius.silver_schema, olius.reference_date, olius.max_rows_per_table,
# MAGIC olius.max_total_rows. Veja documents/03_pipeline_medalhao.md para os passos.
# MAGIC Execute refresh no PostgreSQL antes da pipeline e evite refresh concorrente:
# MAGIC leituras federadas distintas não oferecem uma transação PostgreSQL compartilhada.
# MAGIC Referências: https://docs.databricks.com/aws/en/ldp/developer/python-ref
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html

# COMMAND ----------
import json
import math
import re
import uuid
from datetime import date, datetime

import numpy as np
import pandas as pd


def column(kind, nullable=False, minimum=None, maximum=None, choices=None):
    return dict(kind=kind, nullable=nullable, minimum=minimum, maximum=maximum, choices=choices)


UUID = column("uuid")
TEXT = column("text")
DATE = column("date")
BOOL = column("bool")
VOLUME = column("float", minimum=0)
COUNT = column("int", minimum=0)
ONE = column("int", minimum=1, maximum=1)
OPTIONAL_TIME = column("timestamp", nullable=True)

# Contrato completo do PostgreSQL/08_materialized_views.sql. !NULL é verificado
# mesmo que a origem tenha constraints; alterações de contrato não viram colunas vazias.
CONTRACTS = {
    "dim_estabelecimento": (['estabelecimento_id'], dict(
        estabelecimento_id=UUID, nome=TEXT, tipo_estabelecimento=TEXT, estado=TEXT,
        cidade=TEXT, bairro=TEXT, latitude=column('float', True, -90, 90),
        longitude=column('float', True, -180, 180), is_pev=BOOL,
        status=column('text', choices=['ACTIVE', 'INACTIVE']))),
    "dim_cidadao": (['cidadao_id'], dict(cidadao_id=UUID, nome=TEXT,
        status=column('text', choices=['ACTIVE', 'INACTIVE']))),
    "dim_motorista": (['motorista_id'], dict(motorista_id=UUID, nome=TEXT,
        status=column('text', choices=['ACTIVE', 'INACTIVE']))),
    "dim_pev": (['pev_id'], dict(pev_id=UUID,
        status=column('text', choices=['PENDING', 'APPROVED', 'INACTIVE', 'REJECTED']),
        estado=TEXT, cidade=TEXT, bairro=TEXT, latitude=column('float', True, -90, 90),
        longitude=column('float', True, -180, 180))),
    "dim_classificacao_coleta": (['classificacao_id'], dict(classificacao_id=TEXT,
        resultado=column('text', choices=['SUCCESSFUL', 'UNSUCCESSFUL']),
        condicao_oleo=column('text', choices=['ACCEPTABLE', 'UNACCEPTABLE', 'NOT_ASSESSED']),
        ocorrencia_comprometedora=BOOL)),
    "dim_status_solicitacao": (['status_id'], dict(
        status_id=column('text', choices=['PENDING', 'APPROVED', 'REJECTED', 'CANCELLED']))),
    "dim_motivo_falha": (['motivo_id'], dict(motivo_id=UUID, codigo=TEXT, nome=TEXT,
        descricao=column('text', True))),
    "dim_tempo": (['data_id'], dict(data_id=DATE, ano=column('int', minimum=1, maximum=9999),
        trimestre=column('int', minimum=1, maximum=4), mes=column('int', minimum=1, maximum=12),
        dia=column('int', minimum=1, maximum=31), dia_semana=column('int', minimum=1, maximum=7))),
    "fato_coleta": (['coleta_id'], dict(coleta_id=UUID, data_id=DATE,
        estabelecimento_id=UUID, motorista_id=UUID, classificacao_id=TEXT, solicitacao_id=UUID,
        volume_coletado_litros=VOLUME, volume_apresentado_litros=VOLUME, volume_estimado_litros=VOLUME,
        pontos_nominais=column('int', True), calculo_vigente_disponivel=BOOL, quantidade_coletas=ONE)),
    "fato_entrega_pev": (['entrega_id'], dict(entrega_id=UUID, data_id=DATE, cidadao_id=UUID, pev_id=UUID,
        volume_entregue_litros=column('float', minimum=0.01), pontos_nominais=column('int', True, 0),
        calculo_vigente_disponivel=BOOL, quantidade_entregas=ONE)),
    "fato_solicitacao": (['solicitacao_id'], dict(solicitacao_id=UUID, data_id=DATE,
        estabelecimento_id=UUID, status_id=TEXT, solicitada_em=column('timestamp'),
        agendada_em=OPTIONAL_TIME, chegada_em=OPTIONAL_TIME, coletada_em=OPTIONAL_TIME,
        cancelada_em=OPTIONAL_TIME, volume_estimado_litros=column('float', minimum=0.01),
        volume_franquia_perdida_litros=VOLUME, vagas_perdidas=COUNT, quantidade_solicitacoes=ONE,
        quantidade_reagendamentos=COUNT, tempo_ate_coleta_horas=column('float', True, 0),
        desvio_chegada_minutos=column('float', True),
        iniciativa_cancelamento=column('text', True, choices=['ESTABLISHMENT', 'OPERATION']),
        politica_cancelamento=column('text', True, choices=['FREE', 'LATE_FORFEITURE']))),
    "ponte_coleta_motivo": (['coleta_id', 'motivo_id'], dict(coleta_id=UUID, motivo_id=UUID)),
    "ranking_anual": (['perfil', 'ano', 'participante_id'], dict(
        perfil=column('text', choices=['B2B', 'B2C']), ano=column('int', minimum=1, maximum=9999),
        participante_id=UUID, pontos_anuais=column('float', minimum=0), posicao=column('int', minimum=1))),
}

# COMMAND ----------
def raw_json(row):
    return json.dumps({k: None if pd.isna(v) else str(v) for k, v in row.items()}, ensure_ascii=False)


def clean_table(name, raw):
    """Retorna linhas tipadas, quarentena e contagem das duplicatas exatas."""
    keys, specs = CONTRACTS[name]
    missing = set(specs) - set(raw.columns)
    if missing:
        raise ValueError(f'{name}: colunas ausentes: {sorted(missing)}')
    original = raw[list(specs)].copy().reset_index(drop=True)
    frame = original.copy()
    errors = pd.Series('', index=frame.index, dtype='string')

    def mark(mask, reason):
        mask = pd.Series(mask, index=frame.index).fillna(False).astype(bool)
        errors.loc[mask] = errors.loc[mask] + reason + ';'

    for field, spec in specs.items():
        source = frame[field]
        missing_value = source.isna() | source.astype('string').str.strip().eq('').fillna(False)
        kind = spec['kind']
        if kind in ('float', 'int'):
            numeric = pd.to_numeric(source.mask(missing_value), errors='coerce')
            invalid = numeric.isna() & ~missing_value
            invalid |= source.map(lambda v: isinstance(v, (bool, np.bool_)))
            finite = numeric.map(lambda v: pd.isna(v) or math.isfinite(float(v)))
            invalid |= ~finite
            numeric = numeric.mask(~finite)
            if kind == 'int':
                fractional = numeric.notna() & numeric.mod(1).ne(0)
                overflow = numeric.notna() & ((numeric < -(2**63)) | (numeric >= 2**63))
                invalid |= fractional | overflow
                numeric = numeric.mask(fractional | overflow)
                frame[field] = numeric.astype('Int64')
            else:
                frame[field] = numeric.astype('Float64')
            mark(invalid, field + ':datatype_numerico_invalido')
            if spec['minimum'] is not None:
                mark(frame[field] < spec['minimum'], field + ':abaixo_do_minimo')
            if spec['maximum'] is not None:
                mark(frame[field] > spec['maximum'], field + ':acima_do_maximo')
            if 'volume_' in field:
                mark((frame[field] * 100 - (frame[field] * 100).round()).abs() > 0.000001,
                     field + ':mais_de_duas_casas_decimais')
        elif kind == 'bool':
            normalized = source.astype('string').str.strip().str.lower()
            frame[field] = normalized.map({'true': True, 'false': False, 't': True, 'f': False,
                                           '1': True, '0': False}).astype('boolean')
            mark(frame[field].isna() & ~missing_value, field + ':datatype_booleano_invalido')
        elif kind in ('date', 'timestamp'):
            valid_type = source.map(lambda v: pd.isna(v) or isinstance(v, (str, date, datetime, pd.Timestamp)))
            parsed = pd.to_datetime(source.where(valid_type & ~missing_value), errors='coerce', utc=True, format='mixed')
            if kind == 'date':
                mark(parsed.notna() & parsed.ne(parsed.dt.normalize()), field + ':data_com_horario')
                frame[field] = parsed.dt.tz_localize(None).dt.normalize()
            else:
                # Spark faz a leitura em UTC; timestamps sem tz recebidos dele são UTC.
                frame[field] = parsed
            mark(frame[field].isna() & ~missing_value, field + ':datatype_data_invalido')
        else:
            valid_type = source.map(lambda v: pd.isna(v) or isinstance(v, (str, uuid.UUID)))
            normalized = source.astype('string').str.strip().mask(missing_value)
            if spec['choices']:
                normalized = normalized.str.upper()
                mark(normalized.notna() & ~normalized.isin(spec['choices']), field + ':dominio_invalido')
            if kind == 'uuid':
                normalized = normalized.str.lower()
                mark(normalized.notna() & ~normalized.str.fullmatch(
                    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', na=False), field + ':uuid_invalido')
            frame[field] = normalized
            mark(~valid_type, field + ':datatype_texto_invalido')
        if not spec['nullable']:
            mark(frame[field].isna(), field + ':nulo_obrigatorio')

    if name == 'dim_tempo':
        d = frame['data_id'].dt
        for field, expected in dict(ano=d.year, trimestre=d.quarter, mes=d.month, dia=d.day, dia_semana=d.dayofweek + 1).items():
            mark(frame[field].ne(expected), field + ':calendario_incoerente')
    if name == 'dim_estabelecimento' or name == 'dim_pev':
        frame['estado'] = frame['estado'].str.upper()
        mark(~frame['estado'].isin('AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO'.split()), 'estado:uf_invalida')
        mark(frame['latitude'].isna() != frame['longitude'].isna(), 'coordenadas:par_incompleto')
    if name == 'dim_classificacao_coleta':
        expected = (frame['resultado'] + '|' + frame['condicao_oleo'] + '|'
                    + frame['ocorrencia_comprometedora'].astype('string').str.lower())
        mark(frame['classificacao_id'].ne(expected), 'classificacao_id:componentes_incoerentes')
    if name in ('fato_coleta', 'fato_entrega_pev'):
        mark(frame['calculo_vigente_disponivel'].ne(frame['pontos_nominais'].notna()), 'pontos:disponibilidade_incoerente')
    if name == 'fato_coleta':
        mark(frame['volume_coletado_litros'] > frame['volume_apresentado_litros'], 'volume:coletado_maior_que_apresentado')
        mark(frame['volume_estimado_litros'] <= 0, 'volume_estimado:deve_ser_positivo')
    if name == 'fato_solicitacao':
        local_request_date = frame['solicitada_em'].dt.tz_convert('America/Sao_Paulo').dt.tz_localize(None).dt.normalize()
        mark(frame['data_id'].ne(local_request_date), 'data_id:diferente_da_data_local_da_solicitacao')
        for field in ('agendada_em', 'chegada_em', 'coletada_em', 'cancelada_em'):
            mark(frame[field].notna() & (frame[field] < frame['solicitada_em']), field + ':anterior_a_solicitacao')
        mark(frame['status_id'].eq('APPROVED') & frame['agendada_em'].isna(), 'aprovada:sem_agendamento')
        mark(frame['coletada_em'].notna() & ~frame['status_id'].eq('APPROVED'), 'coletada:status_incompativel')
        cancelled = frame['status_id'].eq('CANCELLED')
        mark(cancelled.ne(frame['cancelada_em'].notna()), 'cancelamento:data_incoerente')
        mark(cancelled & (frame['politica_cancelamento'].isna() | frame['iniciativa_cancelamento'].isna()), 'cancelamento:sem_politica_ou_iniciativa')
        late = frame['politica_cancelamento'].eq('LATE_FORFEITURE')
        free = frame['politica_cancelamento'].eq('FREE')
        mark(late & (frame['iniciativa_cancelamento'].ne('ESTABLISHMENT')
             | frame['vagas_perdidas'].ne(1)
             | frame['volume_franquia_perdida_litros'].ne(frame['volume_estimado_litros'])), 'cancelamento:perda_tardia_incoerente')
        mark(free & (frame['vagas_perdidas'].ne(0) | frame['volume_franquia_perdida_litros'].ne(0)), 'cancelamento:gratuito_com_perda')
        mark(~cancelled & (frame['politica_cancelamento'].notna() | frame['iniciativa_cancelamento'].notna()
                          | frame['vagas_perdidas'].ne(0) | frame['volume_franquia_perdida_litros'].ne(0)), 'cancelamento:campos_indevidos')
        mark(frame['coletada_em'].isna().ne(frame['tempo_ate_coleta_horas'].isna()), 'tempo_coleta:presenca_incoerente')
        lead = (frame['coletada_em'] - frame['solicitada_em']).dt.total_seconds() / 3600
        delay = (frame['chegada_em'] - frame['agendada_em']).dt.total_seconds() / 60
        mark((lead - frame['tempo_ate_coleta_horas']).abs() > 0.00011, 'tempo_coleta:valor_incoerente')
        mark(delay.isna().ne(frame['desvio_chegada_minutos'].isna())
             | ((delay - frame['desvio_chegada_minutos']).abs() > 0.00011), 'desvio_chegada:valor_incoerente')

    quarantine = []

    def record(index, reason):
        quarantine.append(dict(tabela=name, linha_origem=int(index),
            chave='|'.join(str(frame.loc[index, key]) for key in keys), motivos=reason,
            registro_json=raw_json(original.loc[index])))

    for index in errors.index[errors.ne('')]:
        record(index, str(errors.loc[index]))
    frame = frame.loc[errors.eq('')].copy()
    exact = frame.duplicated(keep='first')
    for index in frame.index[exact]:
        record(index, 'duplicata_identica')
    exact_count = int(exact.sum())
    frame = frame.loc[~exact].copy()
    conflicts = frame.duplicated(keys, keep=False)
    for index in frame.index[conflicts]:
        record(index, 'chave_duplicada_com_conteudo_divergente')
    return frame.loc[~conflicts].copy(), quarantine, exact_count, original

# COMMAND ----------
def clean_data_mart(raw_tables, reference=None):
    reference = pd.Timestamp(reference or pd.Timestamp.now(tz='America/Sao_Paulo').date())
    clean, originals, quarantine, duplicate_counts = {}, {}, [], {}
    for name in CONTRACTS:
        clean[name], rejected, duplicate_counts[name], originals[name] = clean_table(name, raw_tables[name])
        quarantine.extend(rejected)

    def reject(name, mask, reason):
        mask = pd.Series(mask, index=clean[name].index).fillna(False).astype(bool)
        keys = CONTRACTS[name][0]
        for index, row in clean[name].loc[mask].iterrows():
            quarantine.append(dict(tabela=name, linha_origem=int(index),
                chave='|'.join(str(row[key]) for key in keys), motivos=reason,
                registro_json=raw_json(originals[name].loc[index])))
        clean[name] = clean[name].loc[~mask].copy()
        return int(mask.sum())

    # Primeiro remove fatos físicos futuros. Datas futuras no calendário e agenda são válidas.
    for name in ('fato_coleta', 'fato_entrega_pev'):
        reject(name, clean[name]['data_id'] > reference, 'data_fisica_futura')
    now = pd.Timestamp.now(tz='UTC')
    for field in ('solicitada_em', 'chegada_em', 'coletada_em', 'cancelada_em'):
        reject('fato_solicitacao', clean['fato_solicitacao'][field] > now, field + ':ocorrencia_futura')

    relationships = [
        ('fato_solicitacao', 'estabelecimento_id', 'dim_estabelecimento', 'estabelecimento_id'),
        ('fato_solicitacao', 'status_id', 'dim_status_solicitacao', 'status_id'),
        ('fato_solicitacao', 'data_id', 'dim_tempo', 'data_id'),
        ('fato_coleta', 'estabelecimento_id', 'dim_estabelecimento', 'estabelecimento_id'),
        ('fato_coleta', 'motorista_id', 'dim_motorista', 'motorista_id'),
        ('fato_coleta', 'classificacao_id', 'dim_classificacao_coleta', 'classificacao_id'),
        ('fato_coleta', 'solicitacao_id', 'fato_solicitacao', 'solicitacao_id'),
        ('fato_coleta', 'data_id', 'dim_tempo', 'data_id'),
        ('fato_entrega_pev', 'cidadao_id', 'dim_cidadao', 'cidadao_id'),
        ('fato_entrega_pev', 'pev_id', 'dim_pev', 'pev_id'),
        ('fato_entrega_pev', 'data_id', 'dim_tempo', 'data_id'),
        ('ponte_coleta_motivo', 'coleta_id', 'fato_coleta', 'coleta_id'),
        ('ponte_coleta_motivo', 'motivo_id', 'dim_motivo_falha', 'motivo_id'),
    ]
    # Cascata até estabilidade: rejeitar um pai não pode deixar fatos órfãos na silver.
    while True:
        removed = 0
        for child, key, parent, parent_key in relationships:
            removed += reject(child, ~clean[child][key].isin(clean[parent][parent_key]), f'{key}:referencia_ausente_em_{parent}')
        c = clean['fato_coleta']
        r = clean['fato_solicitacao'].set_index('solicitacao_id')
        req = c['solicitacao_id'].map(r['estabelecimento_id'])
        removed += reject('fato_coleta', c['estabelecimento_id'].ne(req), 'coleta:estabelecimento_diferente_do_pedido')
        c = clean['fato_coleta']
        removed += reject('fato_coleta', c['solicitacao_id'].duplicated(keep=False), 'solicitacao:mais_de_uma_coleta')
        if removed == 0:
            break

    classification = clean['dim_classificacao_coleta'].set_index('classificacao_id')
    c = clean['fato_coleta']
    outcome = c['classificacao_id'].map(classification['resultado'])
    oil = c['classificacao_id'].map(classification['condicao_oleo'])
    occurrence = c['classificacao_id'].map(classification['ocorrencia_comprometedora'])
    expected_success = (c['volume_coletado_litros'].between(c['volume_estimado_litros'] * .9, c['volume_estimado_litros'] * 1.1)
                        & oil.eq('ACCEPTABLE') & occurrence.eq(False))
    inconsistent = outcome.eq('SUCCESSFUL').ne(expected_success)
    inconsistent |= c['volume_apresentado_litros'].eq(0).ne(oil.eq('NOT_ASSESSED'))
    # Penalidade -50 é legítima; qualquer outro ponto negativo é incoerente com a regra.
    inconsistent |= outcome.eq('UNSUCCESSFUL') & c['pontos_nominais'].notna() & c['pontos_nominais'].ne(-50)
    base = np.floor(c['volume_coletado_litros']) + 50
    bonus = c['pontos_nominais'] - base
    inconsistent |= outcome.eq('SUCCESSFUL') & c['pontos_nominais'].notna() & (~bonus.between(0, 100) | bonus.mod(10).ne(0))
    reject('fato_coleta', inconsistent, 'coleta:classificacao_ou_pontuacao_incoerente')
    d = clean['fato_entrega_pev']
    reject('fato_entrega_pev', d['pontos_nominais'].notna() & d['pontos_nominais'].ne(np.floor(d['volume_entregue_litros'])), 'entrega:pontuacao_incoerente')
    # Solicitações preservam o estado informado; se a coleta foi rejeitada não podem
    # entrar na agenda como se nunca tivesse ocorrido uma visita.
    r = clean['fato_solicitacao']
    known = clean['fato_coleta'].set_index('solicitacao_id')
    collected = r['solicitacao_id'].isin(known.index)
    reject('fato_solicitacao', r['coletada_em'].notna().ne(collected), 'solicitacao:coleta_ausente_ou_sem_data')
    c = clean['fato_coleta']
    r = clean['fato_solicitacao'].set_index('solicitacao_id')
    reject('fato_coleta', ~c['solicitacao_id'].isin(r.index), 'solicitacao_rejeitada_na_validacao_cruzada')
    c = clean['fato_coleta']
    r = clean['fato_solicitacao'].set_index('solicitacao_id')
    # reindex preserva o dtype datetime até quando ambos os lados estão vazios.
    timestamp = pd.Series(r['coletada_em'].reindex(c['solicitacao_id']).array, index=c.index)
    local_day = timestamp.dt.tz_convert('America/Sao_Paulo').dt.tz_localize(None).dt.normalize()
    reject('fato_coleta', c['data_id'].ne(local_day), 'coleta:data_diferente_da_solicitacao')
    # Ranking aprovado não é recalculado a partir da soma de pontos nominais.
    ranking = clean['ranking_anual']
    invalid_owner = ((ranking['perfil'].eq('B2B') & ~ranking['participante_id'].isin(clean['dim_estabelecimento']['estabelecimento_id']))
                     | (ranking['perfil'].eq('B2C') & ~ranking['participante_id'].isin(clean['dim_cidadao']['cidadao_id'])))
    reject('ranking_anual', invalid_owner, 'ranking:participante_ausente')
    # Propaga novamente após regras cruzadas (inclusive remoção de solicitações).
    while True:
        removed = 0
        for child, key, parent, parent_key in relationships:
            removed += reject(child, ~clean[child][key].isin(clean[parent][parent_key]), f'{key}:referencia_rejeitada')
        r = clean['fato_solicitacao']
        removed += reject('fato_solicitacao', r['coletada_em'].notna() & ~r['solicitacao_id'].isin(clean['fato_coleta']['solicitacao_id']), 'coleta_rejeitada_na_validacao_cruzada')
        if removed == 0:
            break

    quality = []
    for name in CONTRACTS:
        original_count = len(raw_tables[name])
        valid_count = len(clean[name])
        duplicates = duplicate_counts[name]
        quality.append(dict(tabela=name, linhas_bronze=original_count, linhas_silver=valid_count,
            duplicadas_identicas=duplicates, linhas_rejeitadas=original_count - valid_count - duplicates,
            percentual_aproveitamento=100 * valid_count / original_count if original_count else None))
    clean['qualidade_dados'] = pd.DataFrame(quality)
    clean['quarentena'] = pd.DataFrame(quarantine, columns=['tabela', 'linha_origem', 'chave', 'motivos', 'registro_json'])
    return clean

# COMMAND ----------
def quoted(value):
    if not value or '\x00' in value:
        raise ValueError('Nome de catálogo/schema/tabela inválido')
    return '`' + value.replace('`', '``') + '`'


def full_name(catalog, schema, table):
    return '.'.join(quoted(v) for v in (catalog, schema, table))


def pandas_schema(frame):
    """Esquema explícito, inclusive para DataFrames vazios ou só com NULL."""
    result = {}
    for name, dtype in frame.dtypes.items():
        if pd.api.types.is_bool_dtype(dtype):
            kind = 'BOOLEAN'
        elif pd.api.types.is_integer_dtype(dtype):
            kind = 'LONG'
        elif pd.api.types.is_numeric_dtype(dtype):
            kind = 'DOUBLE'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            kind = 'TIMESTAMP'
        else:
            kind = 'STRING'
        result[name] = kind
    return result



# COMMAND ----------
_SILVER_KIND = {'uuid': 'STRING', 'text': 'STRING', 'float': 'DOUBLE', 'int': 'LONG',
                'bool': 'BOOLEAN', 'date': 'DATE', 'timestamp': 'TIMESTAMP'}
SILVER_OUTPUT_SCHEMAS = {name: {field: _SILVER_KIND[spec['kind']] for field, spec in specs.items()}
                         for name, (_, specs) in CONTRACTS.items()}
SILVER_OUTPUT_SCHEMAS.update({
    'qualidade_dados': dict(tabela='STRING', linhas_bronze='LONG', linhas_silver='LONG',
        duplicadas_identicas='LONG', linhas_rejeitadas='LONG', percentual_aproveitamento='DOUBLE'),
    'quarentena': dict(tabela='STRING', linha_origem='LONG', chave='STRING', motivos='STRING', registro_json='STRING'),
    'pipeline_state': dict(snapshot_bronze='STRING', bronze_atualizada_em='STRING', data_referencia='DATE',
        bronze_catalog='STRING', bronze_schema='STRING', linhas_quarentena='LONG', versao='STRING'),
})


def silver_json_records(frame, schema):
    def scalar(value, kind):
        if pd.isna(value):
            return None
        if kind == 'DATE':
            return pd.Timestamp(value).date().isoformat()
        if kind == 'TIMESTAMP':
            stamp = pd.Timestamp(value)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize('UTC')
            return stamp.isoformat()
        if kind == 'LONG':
            return int(value)
        if kind == 'DOUBLE':
            return float(value)
        if kind == 'BOOLEAN':
            return bool(value)
        return str(value)
    return [json.dumps({field: scalar(row[field], kind) for field, kind in schema.items()},
                       ensure_ascii=False, allow_nan=False) for row in frame.to_dict('records')]


def silver_envelope(batch, max_rows=200000, max_total_rows=500000):
    """Callback puro pandas: nenhum Spark, efeito externo ou gravação."""
    present = batch.loc[batch['registro_json'].notna()]
    if len(present) > max_total_rows:
        raise MemoryError('Lote bronze excedeu olius.max_total_rows')
    controls = present.loc[present['tabela'].eq('__controle_bronze__'), 'registro_json']
    if len(controls) != 1:
        raise ValueError('dm.refresh_state precisa de exatamente um registro')
    control = json.loads(controls.iloc[0])
    if control.get('singleton') is not True:
        raise ValueError('Controle bronze inválido')
    snapshot = str(uuid.UUID(str(control['snapshot_id'])))
    updated = pd.to_datetime(control['refreshed_at'], utc=True, errors='raise')
    if pd.isna(updated):
        raise ValueError('Controle bronze sem refreshed_at')
    reference = pd.Timestamp(control['data_referencia']).date().isoformat()
    raw_tables = {}
    for name, (_, specs) in CONTRACTS.items():
        subset = present.loc[present['tabela'].eq(name), 'registro_json']
        if len(subset) > max_rows:
            raise MemoryError(f'{name}: excedeu olius.max_rows_per_table; dados não foram amostrados')
        raw_tables[name] = pd.DataFrame([json.loads(value) for value in subset], columns=list(specs))
    tables = clean_data_mart(raw_tables, reference)
    tables['pipeline_state'] = pd.DataFrame([dict(snapshot_bronze=snapshot,
        bronze_atualizada_em=updated.isoformat(), data_referencia=reference,
        bronze_catalog=control['bronze_catalog'], bronze_schema=control['bronze_schema'],
        linhas_quarentena=len(tables['quarentena']), versao='2-declarativa')])
    rows = [(name, value) for name, frame in tables.items()
            for value in silver_json_records(frame, SILVER_OUTPUT_SCHEMAS[name])]
    return pd.DataFrame(rows, columns=['tabela', 'registro_json'])


def register_silver_pipeline(spark_session):
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    config = lambda key, default: spark_session.conf.get('olius.' + key, default).strip()
    source_catalog, source_schema = config('bronze_catalog', 'data mart'), config('bronze_schema', 'dm')
    catalog, schema = config('silver_catalog', 'silver'), config('silver_schema', 'olius')
    if (source_catalog.lower(), source_schema.lower()) == (catalog.lower(), schema.lower()):
        raise ValueError('Origem bronze e destino silver precisam ser diferentes')
    max_rows, max_total = int(config('max_rows_per_table', '200000')), int(config('max_total_rows', '500000'))
    if min(max_rows, max_total) < 1:
        raise ValueError('Limites devem ser positivos')
    reference = config('reference_date', '')
    if reference and pd.Timestamp(reference).strftime('%Y-%m-%d') != reference:
        raise ValueError('olius.reference_date deve ser AAAA-MM-DD')
    internal = full_name(catalog, schema, '_silver_pandas_lote')

    @dp.materialized_view(name=internal, private=True,
                          comment='Tratamento pandas do lote completo; volume acadêmico limitado')
    def silver_pandas_lote():
        parts = []
        for name in CONTRACTS:
            source = spark_session.read.table(full_name(source_catalog, source_schema, name)).select(
                *list(CONTRACTS[name][1])).limit(max_rows + 1)
            parts.append(source.select(F.lit(name).alias('tabela'),
                F.to_json(F.struct('*'), options={'ignoreNullFields': 'false'}).alias('registro_json')))
        state = spark_session.read.table(full_name(source_catalog, source_schema, 'refresh_state')).limit(2)
        ref = F.to_date(F.lit(reference)) if reference else F.to_date(F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo'))
        state = state.withColumn('data_referencia', ref).withColumn('bronze_catalog', F.lit(source_catalog)).withColumn('bronze_schema', F.lit(source_schema))
        parts.append(state.select(F.lit('__controle_bronze__').alias('tabela'),
            F.to_json(F.struct('*'), options={'ignoreNullFields': 'false'}).alias('registro_json')))
        combined = parts[0]
        for part in parts[1:]:
            combined = combined.unionByName(part)
        def process(batch):
            return silver_envelope(batch, max_rows, max_total)
        return combined.withColumn('_lote', F.lit(1)).groupBy('_lote').applyInPandas(
            process, schema='tabela STRING, registro_json STRING')

    def declare_output(name, fields):
        ddl = ', '.join(f'{quoted(field)} {kind}' for field, kind in fields.items())
        @dp.materialized_view(name=full_name(catalog, schema, name), comment='Silver validada com pandas: ' + name)
        def silver_output():
            source = spark_session.read.table(internal).where(F.col('tabela') == F.lit(name))
            return source.select(F.from_json('registro_json', ddl).alias('registro')).select('registro.*')
        return silver_output
    return [declare_output(name, fields) for name, fields in SILVER_OUTPUT_SCHEMAS.items()]


if 'spark' in globals():
    register_silver_pipeline(spark)  # noqa: F821 — sessão gerenciada pela pipeline

"""Regras pandas e declarações da pipeline; sem nuvem, Spark instalado ou segredos."""
import ast
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch
from uuid import UUID

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


silver = load('silver_note')
gold = load('gold_note')


def uid(number):
    return str(UUID(int=number))


def fixture():
    raw = {name: pd.DataFrame(columns=list(specs)) for name, (_, specs) in silver.CONTRACTS.items()}
    def rows(name, values):
        raw[name] = pd.DataFrame(values, columns=list(silver.CONTRACTS[name][1]))
    rows('dim_estabelecimento', [dict(estabelecimento_id=uid(i), nome='Est ' + str(i), tipo_estabelecimento='Restaurante',
        estado='SP', cidade='São Paulo', bairro='Centro', latitude=None, longitude=None, is_pev=False, status='ACTIVE') for i in (1, 2, 3)])
    rows('dim_cidadao', [dict(cidadao_id=uid(4), nome='Cidadão', status='ACTIVE')])
    rows('dim_motorista', [dict(motorista_id=uid(5), nome='Motorista', status='ACTIVE')])
    rows('dim_pev', [dict(pev_id=uid(6), status='APPROVED', estado='SP', cidade='São Paulo', bairro='Centro', latitude=None, longitude=None)])
    rows('dim_classificacao_coleta', [dict(classificacao_id=r + '|' + oil + '|false', resultado=r, condicao_oleo=oil,
        ocorrencia_comprometedora=False) for r, oil in [('SUCCESSFUL', 'ACCEPTABLE'), ('UNSUCCESSFUL', 'NOT_ASSESSED')]])
    rows('dim_status_solicitacao', [dict(status_id=s) for s in ['PENDING', 'APPROVED', 'REJECTED', 'CANCELLED']])
    rows('dim_motivo_falha', [dict(motivo_id=uid(7), codigo='VOLUME_OUT_OF_TOLERANCE', nome='Volume', descricao=None)])
    rows('dim_tempo', [dict(data_id=d.date().isoformat(), ano=2026, trimestre=4, mes=10, dia=d.day, dia_semana=d.dayofweek + 1)
                      for d in pd.date_range('2026-10-01', '2026-10-31')])
    requests = []
    for i in (1, 2, 3, 4):
        day = '2026-10-0' + str(i) if i <= 2 else '2026-10-30'
        completed = i <= 2
        status = 'APPROVED' if i != 3 else 'PENDING'
        requests.append(dict(solicitacao_id=uid(10+i), data_id=day, estabelecimento_id=uid(min(i,3)),
            status_id=status, solicitada_em=day+'T12:00:00Z', agendada_em=(day+'T12:00:00Z' if completed else
            ('2026-11-01T01:00:00Z' if i==4 else None)), chegada_em=day+'T12:00:00Z' if completed else None,
            coletada_em=day+'T12:00:00Z' if completed else None, cancelada_em=None, volume_estimado_litros=10,
            volume_franquia_perdida_litros=0, vagas_perdidas=0, quantidade_solicitacoes=1, quantidade_reagendamentos=0,
            tempo_ate_coleta_horas=0 if completed else None, desvio_chegada_minutos=0 if completed else None,
            iniciativa_cancelamento=None, politica_cancelamento=None))
    rows('fato_solicitacao', requests)
    rows('fato_coleta', [dict(coleta_id=uid(20+i), data_id='2026-10-0'+str(i), estabelecimento_id=uid(i),
        motorista_id=uid(5), classificacao_id=('SUCCESSFUL|ACCEPTABLE|false' if i==1 else 'UNSUCCESSFUL|NOT_ASSESSED|false'),
        solicitacao_id=uid(10+i), volume_coletado_litros=10 if i==1 else 0,
        volume_apresentado_litros=10 if i==1 else 0, volume_estimado_litros=10,
        pontos_nominais=60 if i==1 else -50, calculo_vigente_disponivel=True, quantidade_coletas=1) for i in (1,2)])
    rows('fato_entrega_pev', [dict(entrega_id=uid(30), data_id='2026-10-01', cidadao_id=uid(4), pev_id=uid(6),
        volume_entregue_litros=5, pontos_nominais=5, calculo_vigente_disponivel=True, quantidade_entregas=1)])
    rows('ponte_coleta_motivo', [dict(coleta_id=uid(22), motivo_id=uid(7))])
    return raw


def bronze_batch(raw):
    rows = [(name, json.dumps(row, default=str)) for name, frame in raw.items() for row in frame.to_dict('records')]
    rows.append(('__controle_bronze__', json.dumps(dict(singleton=True, snapshot_id=uid(99), refreshed_at='2026-10-30T15:00:00Z',
                 data_referencia='2026-10-31', bronze_catalog='data mart', bronze_schema='dm'))))
    return pd.DataFrame(rows, columns=['tabela','registro_json'])


class MedallionTests(TestCase):
    def test_end_to_end_pandas_and_kpis(self):
        clean = silver.silver_envelope(bronze_batch(fixture()))
        result = gold.gold_envelope(clean)
        cards = pd.DataFrame([json.loads(v) for v in result.loc[result.tabela.eq('kpis'),'registro_json']])
        cards = cards.set_index(['metrica','perfil']).valor
        self.assertEqual(cards['solicitacoes_pendentes','B2B'],1)
        self.assertEqual(cards['coletas_agendadas_hoje','B2B'],1)  # 01/11 01:00 UTC = 31/10 SP
        self.assertEqual(cards['coletas_restantes_mes','B2B'],1)
        self.assertEqual(cards['coletas_mes','B2B'],2)
        self.assertEqual(cards['entregas_pev_mes','B2C'],1)
        self.assertAlmostEqual(cards['media_litros_por_b2b','B2B'],10/3)
        self.assertEqual(set(result.tabela),set(gold.GOLD_OUTPUT_SCHEMAS)-{'ranking_anual'})

    def test_quarantine_duplicates_types_and_negatives(self):
        raw = fixture()
        raw['fato_entrega_pev']['volume_entregue_litros'] = raw['fato_entrega_pev']['volume_entregue_litros'].astype(object)
        raw['fato_entrega_pev'].loc[0,'volume_entregue_litros'] = 'abc'
        raw['fato_coleta'] = pd.concat([raw['fato_coleta'],raw['fato_coleta'].iloc[[0]]],ignore_index=True)
        clean = silver.silver_envelope(bronze_batch(raw))
        quarantine = ';'.join(json.loads(v)['motivos'] for v in clean.loc[clean.tabela.eq('quarentena'),'registro_json'])
        self.assertIn('datatype_numerico_invalido',quarantine)
        self.assertIn('duplicata_identica',quarantine)
        events = [json.loads(v) for v in clean.loc[clean.tabela.eq('fato_coleta'),'registro_json']]
        self.assertEqual(sorted(e['pontos_nominais'] for e in events),[-50,60])
        gold.gold_envelope(clean)  # gold também aceita uma fato B2C completamente vazia

    def test_missing_points_are_not_zero(self):
        raw=fixture()
        raw['fato_entrega_pev']['pontos_nominais']=None
        raw['fato_entrega_pev']['calculo_vigente_disponivel']=False
        result=gold.gold_envelope(silver.silver_envelope(bronze_batch(raw)))
        details=[json.loads(v) for v in result.loc[result.tabela.eq('detalhe_b2c'),'registro_json']]
        self.assertIsNone(details[0]['pontos_nominais'])

    def test_reference_and_schema_are_preserved(self):
        result=gold.gold_envelope(silver.silver_envelope(bronze_batch(fixture())))
        for name, payload in result.itertuples(index=False,name=None):
            self.assertEqual(set(json.loads(payload)),set(gold.GOLD_OUTPUT_SCHEMAS[name]))
        state=json.loads(result.loc[result.tabela.eq('pipeline_state'),'registro_json'].iloc[0])
        self.assertEqual(state['data_referencia'],'2026-10-31')
        self.assertEqual(state['snapshot_bronze'],uid(99))

    def test_limits_do_not_publish_samples(self):
        batch=bronze_batch(fixture())
        with self.assertRaises(MemoryError): silver.silver_envelope(batch,max_rows=2)
        with self.assertRaises(MemoryError): silver.silver_envelope(batch,max_total_rows=1)
        clean=silver.silver_envelope(batch)
        with self.assertRaises(MemoryError): gold.gold_envelope(clean,max_output_rows=1)

    def test_missing_control_and_incomplete_silver_fail(self):
        batch=bronze_batch(fixture())
        with self.assertRaises(ValueError): silver.silver_envelope(batch.loc[~batch.tabela.eq('__controle_bronze__')])
        clean=silver.silver_envelope(batch)
        with self.assertRaises(ValueError): gold.gold_envelope(clean.loc[~clean.tabela.eq('fato_coleta')])

    def test_no_imperative_pipeline_actions(self):
        for name in ['silver_note','gold_note']:
            tree=ast.parse((ROOT/(name+'.py')).read_text(encoding='utf-8'))
            forbidden=[n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                       and n.func.attr in {'toPandas','collect','saveAsTable','save','start'}]
            self.assertEqual(forbidden,[])

    def test_dataset_registration_is_lazy_and_complete(self):
        declarations=[]
        class Column:
            def alias(self,*args): return self
            def __eq__(self,other): return self
        class Frame:
            def limit(self,*args): return self
            def select(self,*args,**kwargs): return self
            def unionByName(self,*args): return self
            def withColumn(self,*args): return self
            def where(self,*args): return self
            def groupBy(self,*args): return self
            def applyInPandas(self,callback,schema):
                # O callback não pode capturar uma SparkSession no worker.
                for cell in callback.__closure__ or []:
                    self_outer.assertIsInstance(cell.cell_contents,int)
                return self
        self_outer=self
        class FakeSpark:
            conf=SimpleNamespace(get=lambda key,default:default)
            read=SimpleNamespace(table=lambda name:Frame())
        def decorator(**kwargs):
            def register(fn): declarations.append((kwargs,fn)); return fn
            return register
        dp=SimpleNamespace(materialized_view=decorator)
        F=SimpleNamespace(**{name:(lambda *args,**kwargs:Column()) for name in
            ['lit','to_json','struct','to_date','from_utc_timestamp','current_timestamp','col','from_json']})
        pyspark=ModuleType('pyspark'); pyspark.pipelines=dp
        sql=ModuleType('pyspark.sql'); sql.functions=F
        with patch.dict(sys.modules,{'pyspark':pyspark,'pyspark.sql':sql}):
            silver.register_silver_pipeline(FakeSpark())
            gold.register_gold_pipeline(FakeSpark())
            self.assertEqual(len(declarations),39)
            self.assertEqual(sum(bool(k.get('private')) for k,_ in declarations),2)
            for _,callback in declarations: self.assertIsInstance(callback(),Frame)


if __name__=='__main__': main()

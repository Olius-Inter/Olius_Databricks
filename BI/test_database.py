"""Integração somente leitora; execute contra a base da carga BI."""
from datetime import datetime
import json
import os
import subprocess
import unittest

from pipeline import calculate, read_snapshot, TIMEZONE


@unittest.skipUnless(os.environ.get("OLIUS_BI_TEST_CONTAINER"), "Banco de integração não selecionado")
class DatabaseTests(unittest.TestCase):
    def query(self, sql):
        command = ["docker", "exec", "-i", os.environ["OLIUS_BI_TEST_CONTAINER"],
                   "psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d",
                   os.environ.get("OLIUS_BI_TEST_DATABASE", "olius_bi_demo")]
        result = subprocess.run(command, input=sql, text=True, encoding="utf-8", capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        return json.loads(result.stdout)

    def test_load_invariants(self):
        counts = self.query("""
          WITH own_est AS (SELECT md5('olius-data-load-v1:est:' || i)::uuid AS id FROM generate_series(1,30) n(i)),
            own_cit AS (SELECT md5('olius-data-load-v1:citizen:' || i)::uuid AS id FROM generate_series(1,80) n(i))
          SELECT jsonb_build_object('collections',(SELECT count(*) FROM public.collection WHERE establishment_id IN (SELECT id FROM own_est)),
            'deliveries',(SELECT count(*) FROM public.delivery_pev WHERE citizen_id IN (SELECT id FROM own_cit)),
            'requests',(SELECT count(*) FROM public.collection_request WHERE establishment_id IN (SELECT id FROM own_est)),
            'dm_collections',(SELECT count(*) FROM dm.fato_coleta WHERE estabelecimento_id IN (SELECT id::text FROM own_est)),
            'dm_deliveries',(SELECT count(*) FROM dm.fato_entrega_pev WHERE cidadao_id IN (SELECT id::text FROM own_cit)),
            'months',(SELECT count(DISTINCT date_trunc('month',collection_date)) FROM public.collection),
            'pending',(SELECT count(*) FROM dm.fato_solicitacao WHERE status_id='PENDING' AND estabelecimento_id IN (SELECT id::text FROM own_est)),
            'calculations',(SELECT count(*) FROM public.point_calculation WHERE is_current
                            AND user_id IN (SELECT id FROM own_est UNION ALL SELECT id FROM own_cit)),
            'invalid_dates',(SELECT count(*) FROM public.collection c
              JOIN public.collection_request r ON r.id=c.collection_request_id
              JOIN public.subscription_cycle cy ON cy.id=r.subscription_cycle_id
              WHERE c.collection_date<r.service_accepted_at OR r.request_at<cy.starts_at OR r.request_at>=cy.ends_at),
            'gps_invented',(SELECT count(*) FROM public.addresses WHERE latitude IS NOT NULL OR longitude IS NOT NULL));
        """)
        self.assertEqual(counts, dict(collections=780,deliveries=520,requests=825,dm_collections=780,
                                      dm_deliveries=520,months=13,pending=15,calculations=1300,
                                      invalid_dates=0,gps_invented=0))

    def test_statistics_match_postgresql(self):
        snapshot = read_snapshot(os.environ["OLIUS_BI_TEST_CONTAINER"],
                                 os.environ.get("OLIUS_BI_TEST_DATABASE", "olius_bi_demo"), "postgres")
        reference = datetime.now(TIMEZONE).date()
        computed = calculate(snapshot, reference)
        sql = """
          WITH events AS (
            SELECT 'B2B' AS profile, estabelecimento_id AS owner, data_id,
                   volume_coletado_litros AS liters FROM dm.fato_coleta
            UNION ALL
            SELECT 'PEV', pev_id, data_id, volume_entregue_litros FROM dm.fato_entrega_pev
          ), entities AS (
            SELECT 'B2B' AS profile, estabelecimento_id AS owner FROM dm.dim_estabelecimento
            UNION ALL SELECT 'PEV',pev_id FROM dm.dim_pev
          ), totals AS (
            SELECT e.profile,e.owner,m.month,count(f.owner)::numeric AS count,coalesce(sum(f.liters),0) AS liters
            FROM entities e CROSS JOIN generate_series(1,12) m(month)
            LEFT JOIN events f ON f.profile=e.profile AND f.owner=e.owner
              AND extract(year FROM f.data_id)=YEAR_VALUE AND extract(month FROM f.data_id)=m.month
            GROUP BY e.profile,e.owner,m.month
          ), values AS (
            SELECT profile,extract(month FROM data_id)::integer AS month,
                   'litros_por_evento' AS metric,liters AS value
            FROM events WHERE extract(year FROM data_id)=YEAR_VALUE
            UNION ALL SELECT profile,month,'litros_por_participante',liters FROM totals
            UNION ALL SELECT profile,month,'eventos_por_participante',count FROM totals
          ), means AS (
            SELECT *,avg(value) OVER(PARTITION BY profile,month,metric) AS mean FROM values
          ), stats AS (
            SELECT profile,month,metric,count(*) AS n,avg(value) AS media,
              percentile_cont(0.5) WITHIN GROUP(ORDER BY value) AS mediana,
              stddev_pop(value) AS desvio_padrao,var_pop(value) AS variancia,
              avg(abs(value-mean)) AS desvio_medio,
              stddev_samp(value) AS desvio_padrao_amostral,var_samp(value) AS variancia_amostral
            FROM means GROUP BY profile,month,metric
          ) SELECT jsonb_agg(to_jsonb(stats)) FROM stats;
        """.replace("YEAR_VALUE", str(reference.year))
        expected = self.query(sql)
        indexed = {(r["perfil"],r["mes"],r["metrica"]):r for r in computed["estatisticas"]}
        for row in expected:
            actual = indexed[(row["profile"],row["month"],row["metric"])]
            for key in ("n","media","mediana","desvio_padrao","variancia","desvio_medio",
                        "desvio_padrao_amostral","variancia_amostral"):
                if row[key] is None:
                    self.assertIsNone(actual[key])
                else:
                    self.assertAlmostEqual(actual[key],row[key],places=7,msg=str((row["profile"],row["month"],key)))


if __name__ == "__main__":
    unittest.main()

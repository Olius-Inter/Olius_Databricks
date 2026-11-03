import copy
from datetime import date
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import calculate, describe, publish, run_once


def fixture():
    return dict(state=dict(snapshot_id="snapshot-1", refreshed_at="2026-10-28T13:00:00Z"),
                establishments=["a", "b", "c"], pevs=["p", "q"],
                collections=[dict(coleta_id="1", data_id="2026-10-01", estabelecimento_id="a", volume_coletado_litros=10),
                             dict(coleta_id="2", data_id="2026-10-02", estabelecimento_id="a", volume_coletado_litros=20),
                             dict(coleta_id="3", data_id="2026-10-03", estabelecimento_id="b", volume_coletado_litros=0)],
                deliveries=[dict(entrega_id="1", data_id="2026-10-01", pev_id="p", volume_entregue_litros=5)],
                requests=[dict(solicitacao_id=str(i), status_id=status, agendada_em=scheduled, coletada_em=collected)
                          for i, (status, scheduled, collected) in enumerate([
                              ("PENDING", None, None),
                              ("APPROVED", "2026-10-29T01:00:00Z", None),  # 28/10 em SP
                              ("APPROVED", "2026-10-31T20:00:00Z", None),
                              ("APPROVED", "2026-10-28T20:00:00Z", "2026-10-28T20:30:00Z"),
                              ("CANCELLED", "2026-10-28T20:00:00Z", None),
                              ("REJECTED", "2026-10-28T20:00:00Z", None),
                              ("APPROVED", "2026-10-01T20:00:00Z", None),
                          ])])


class PipelineTests(unittest.TestCase):
    def test_statistics_known_population(self):
        result = describe([2, 4, 4, 4, 5, 5, 7, 9])
        self.assertEqual(result["media"], 5)
        self.assertEqual(result["mediana"], 4.5)
        self.assertEqual(result["variancia"], 4)
        self.assertEqual(result["desvio_padrao"], 2)
        self.assertEqual(result["desvio_medio"], 1.5)
        self.assertAlmostEqual(result["variancia_amostral"], 32 / 7)
        self.assertIsNone(describe([])["media"])
        self.assertIsNone(describe([3])["desvio_padrao_amostral"])
        self.assertEqual(describe([3])["desvio_padrao"], 0)

    def test_cards_months_and_zero_participants(self):
        result = calculate(fixture(), date(2026, 10, 28))
        self.assertEqual(result["cards"], dict(solicitacoes_pendentes=1, coletas_agendadas_hoje=1,
            coletas_restantes_mes=2, coletas_atrasadas=1, coletas_ano=3, litros_recolhidos_ano=30,
            coletas_mes=3))
        self.assertEqual(len(result["mensal"]), 12)
        self.assertIsNone(result["mensal"][9]["variacao_litros_b2b_pct"])
        stats = {r["metrica"]: r for r in result["estatisticas"] if r["mes"] == 10 and r["perfil"] == "B2B"}
        self.assertEqual(stats["litros_por_evento"]["media"], 10)
        self.assertEqual(stats["litros_por_participante"]["media"], 10)
        self.assertEqual(stats["eventos_por_participante"]["media"], 1)
        self.assertEqual(stats["eventos_por_participante"]["n"], 3)
        self.assertTrue(result["mensal"][10]["periodo_futuro"])
        self.assertIsNone(result["mensal"][10]["variacao_litros_b2b_pct"])
        self.assertEqual(result["mensal"][9]["litros_b2b"], 30)
        self.assertEqual(result["mensal"][9]["litros_pev"], 5)

    def test_invalid_facts(self):
        for mutation in (lambda s: s["collections"].append(copy.deepcopy(s["collections"][0])),
                         lambda s: s["collections"][0].update(estabelecimento_id="missing"),
                         lambda s: s["collections"][0].update(volume_coletado_litros=float("nan")),
                         lambda s: s["collections"][0].update(data_id="2026-12-01"),
                         lambda s: s.update(state=None)):
            data = fixture()
            mutation(data)
            with self.assertRaises(ValueError):
                calculate(data, date(2026, 10, 28))

    def test_year_filter(self):
        result = calculate(fixture(), date(2026, 10, 28), 2025)
        self.assertEqual(result["cards"]["coletas_ano"], 0)
        self.assertIsNone(result["cards"]["coletas_mes"])

    def test_reprocess_snapshot_date_and_failed_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("pipeline.read_snapshot", return_value=fixture()):
                first = run_once(directory, reference=date(2026, 10, 28))
                self.assertTrue((first / "estatisticas.csv").is_file())
                self.assertIsNone(run_once(directory, reference=date(2026, 10, 28)))
                self.assertIsNotNone(run_once(directory, reference=date(2026, 10, 29)))
                (Path(directory) / json.loads((Path(directory) / "latest.json").read_text())["directory"] / "cards.csv").unlink()
                self.assertIsNotNone(run_once(directory, reference=date(2026, 10, 29)))
            pointer = (Path(directory) / "latest.json").read_bytes()
            with patch("pipeline.os.replace", side_effect=OSError("Falha simulada")):
                with self.assertRaises(OSError):
                    publish(calculate(fixture(), date(2026, 10, 28)), directory)
            self.assertEqual((Path(directory) / "latest.json").read_bytes(), pointer)


if __name__ == "__main__":
    unittest.main()

"""KPIs reproduzíveis de um snapshot consistente de dm; exporta JSON e CSV."""
import argparse
from collections import defaultdict
import csv
from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("America/Sao_Paulo")
SNAPSHOT_SQL = """
SELECT jsonb_build_object(
 'state',(SELECT to_jsonb(s) FROM dm.refresh_state s WHERE singleton),
 'collections',COALESCE((SELECT jsonb_agg(to_jsonb(f)) FROM (
   SELECT coleta_id,data_id,estabelecimento_id,volume_coletado_litros FROM dm.fato_coleta) f),'[]'::jsonb),
 'deliveries',COALESCE((SELECT jsonb_agg(to_jsonb(f)) FROM (
   SELECT entrega_id,data_id,pev_id,volume_entregue_litros FROM dm.fato_entrega_pev) f),'[]'::jsonb),
 'requests',COALESCE((SELECT jsonb_agg(to_jsonb(f)) FROM (
   SELECT solicitacao_id,status_id,agendada_em,coletada_em FROM dm.fato_solicitacao) f),'[]'::jsonb),
 'establishments',COALESCE((SELECT jsonb_agg(estabelecimento_id) FROM dm.dim_estabelecimento),'[]'::jsonb),
 'pevs',COALESCE((SELECT jsonb_agg(pev_id) FROM dm.dim_pev),'[]'::jsonb))
"""


def describe(values):
    """DP/variância populacionais; amostrais separados. Vazio != zero."""
    values = [float(v) for v in values]
    if any(not math.isfinite(v) for v in values):
        raise ValueError("Volume não finito no Data Mart")
    result = dict(n=len(values), total=math.fsum(values), media=None, mediana=None,
                  desvio_padrao=None, variancia=None, desvio_medio=None,
                  desvio_padrao_amostral=None, variancia_amostral=None, minimo=None, maximo=None)
    if not values:
        return result
    mean = statistics.mean(values)
    result.update(media=mean, mediana=statistics.median(values),
                  desvio_padrao=statistics.pstdev(values), variancia=statistics.pvariance(values),
                  desvio_medio=statistics.mean(abs(v - mean) for v in values),
                  minimo=min(values), maximo=max(values))
    if len(values) > 1:
        result.update(desvio_padrao_amostral=statistics.stdev(values),
                      variancia_amostral=statistics.variance(values))
    return result


def local_date(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Agendamento precisa de fuso horário")
        return value.astimezone(TIMEZONE).date()
    if isinstance(value, date):
        return value
    if len(value) == 10:
        return date.fromisoformat(value)
    return local_date(datetime.fromisoformat(value.replace("Z", "+00:00")))


def calculate(snapshot, reference=None, year=None):
    reference = reference or datetime.now(TIMEZONE).date()
    year = year or reference.year
    if not snapshot.get("state") or not snapshot["state"].get("snapshot_id"):
        raise ValueError("dm.refresh_state sem snapshot válido")
    entities = {"B2B": set(snapshot["establishments"]), "PEV": set(snapshot["pevs"])}
    if len(entities["B2B"]) != len(snapshot["establishments"]) or len(entities["PEV"]) != len(snapshot["pevs"]):
        raise ValueError("Dimensão com chave duplicada")
    groups = defaultdict(list)
    seen = {"B2B": set(), "PEV": set()}
    for profile, rows, id_key, owner_key, volume_key in (
        ("B2B", snapshot["collections"], "coleta_id", "estabelecimento_id", "volume_coletado_litros"),
        ("PEV", snapshot["deliveries"], "entrega_id", "pev_id", "volume_entregue_litros"),
    ):
        for row in rows:
            if row[id_key] in seen[profile]:
                raise ValueError("Fato duplicado: " + str(row[id_key]))
            seen[profile].add(row[id_key])
            if row[owner_key] not in entities[profile]:
                raise ValueError("Fato sem dimensão correspondente")
            day = local_date(row["data_id"])
            volume = float(row[volume_key])
            if not math.isfinite(volume) or volume < 0:
                raise ValueError("Volume inválido no Data Mart")
            if day > reference:
                raise ValueError("Fato físico posterior à data de referência")
            if day.year == year:
                groups[(day.month, profile)].append((row[owner_key], volume))
    requests = snapshot["requests"]
    if len({r["solicitacao_id"] for r in requests}) != len(requests):
        raise ValueError("Solicitações duplicadas")
    open_requests = [r for r in requests if r["status_id"] == "APPROVED"
                     and r["coletada_em"] is None and r["agendada_em"] is not None]
    scheduled = [local_date(r["agendada_em"]) for r in open_requests]
    cards = {
        "solicitacoes_pendentes": sum(r["status_id"] == "PENDING" for r in requests),
        "coletas_agendadas_hoje": sum(d == reference for d in scheduled),
        "coletas_restantes_mes": sum(reference <= d and (d.year, d.month) == (reference.year, reference.month)
                                     for d in scheduled),
        "coletas_atrasadas": sum(d < reference for d in scheduled),
    }
    monthly = []
    stats_rows = []
    participant_rows = []
    previous = None
    for month in range(1, 13):
        row = {"ano": year, "mes": month, "periodo": f"{year:04d}-{month:02d}",
               "periodo_futuro": (year, month) > (reference.year, reference.month)}
        for profile in ("B2B", "PEV"):
            events = groups[(month, profile)]
            totals = {entity: [0, 0.0] for entity in entities[profile]}
            for entity, volume in events:
                totals[entity][0] += 1
                totals[entity][1] += volume
            suffix = profile.lower()
            row[f"quantidade_{suffix}"] = len(events)
            row[f"litros_{suffix}"] = round(math.fsum(v for _, v in events), 2)
            for entity, (count, liters) in sorted(totals.items()):
                participant_rows.append(dict(ano=year, mes=month, perfil=profile,
                                             participante_id=entity, quantidade=count, litros=round(liters, 2)))
            for metric, values in (
                ("litros_por_evento", [v for _, v in events]),
                ("litros_por_participante", [v[1] for v in totals.values()]),
                ("eventos_por_participante", [v[0] for v in totals.values()]),
            ):
                stats_rows.append(dict(ano=year, mes=month, perfil=profile, metrica=metric,
                                       **describe(values)))
        # PEV é recebimento B2C: manter separado para evitar dupla contagem de óleo.
        row["variacao_litros_b2b"] = (None if previous is None or row["periodo_futuro"]
                                      else round(row["litros_b2b"] - previous, 2))
        row["variacao_litros_b2b_pct"] = (None if previous in (None, 0) or row["periodo_futuro"]
                                            else round((row["litros_b2b"] / previous - 1) * 100, 6))
        previous = row["litros_b2b"]
        monthly.append(row)
    cards["coletas_ano"] = sum(r["quantidade_b2b"] for r in monthly)
    cards["litros_recolhidos_ano"] = round(math.fsum(r["litros_b2b"] for r in monthly), 2)
    current = next((r for r in monthly if r["ano"] == reference.year and r["mes"] == reference.month), None)
    cards["coletas_mes"] = current["quantidade_b2b"] if current else None
    return dict(metadata=dict(snapshot_id=snapshot["state"]["snapshot_id"],
                              refreshed_at=snapshot["state"].get("refreshed_at"),
                              data_referencia=reference.isoformat(), ano=year,
                              timezone="America/Sao_Paulo", versao=1),
                cards=cards, mensal=monthly, estatisticas=stats_rows, participantes=participant_rows)


def read_snapshot(container=None, database=None, user=None):
    if container:
        command = ["docker", "exec", "-i", container, "psql", "-X", "-w", "-qAt",
                   "-U", user or "postgres", "-d", database or "olius_bi_demo", "-v", "ON_ERROR_STOP=1"]
        result = subprocess.run(command, input="BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n"
                                + SNAPSHOT_SQL + ";\nCOMMIT;", capture_output=True,
                                text=True, encoding="utf-8", timeout=120)
        if result.returncode:
            raise RuntimeError("Falha na leitura do dm pelo psql; verifique conexão e privilégios")
        return json.loads(result.stdout)
    import psycopg
    settings = {"application_name": "olius_bi_kpis", "connect_timeout": 15}
    if database:
        settings["dbname"] = database
    if user:
        settings["user"] = user
    with psycopg.connect(**settings) as connection:
        connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        connection.execute("SET LOCAL statement_timeout='120s'")
        return connection.execute(SNAPSHOT_SQL).fetchone()[0]


def publish(result, output):
    """Diretório imutável + troca atômica do ponteiro após todos os arquivos."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix="run-", dir=output))
    (folder / "kpis.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    for name in ("mensal", "estatisticas", "participantes", "cards"):
        rows = [result[name]] if name == "cards" else result[name]
        with (folder / (name + ".csv")).open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]) if rows else [])
            writer.writeheader()
            writer.writerows(rows)
    manifest = dict(result["metadata"], directory=folder.name)
    # Nome temporário exclusivo permite publicações concorrentes sem colisão.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output, delete=False) as stream:
        json.dump(manifest, stream, ensure_ascii=False)
        temporary = stream.name
    os.replace(temporary, output / "latest.json")
    return folder


def run_once(output, container=None, database=None, user=None, reference=None, year=None, force=False):
    reference = reference or datetime.now(TIMEZONE).date()
    year = year or reference.year
    snapshot = read_snapshot(container, database, user)
    state = snapshot.get("state")
    if not state or not state.get("snapshot_id"):
        raise ValueError("dm.refresh_state ausente")
    pointer = Path(output) / "latest.json"
    identity = dict(snapshot_id=state["snapshot_id"], data_referencia=reference.isoformat(), ano=year, versao=1)
    if pointer.exists() and not force:
        previous = json.loads(pointer.read_text(encoding="utf-8"))
        if all(previous.get(k) == v for k, v in identity.items()):
            folder = Path(output) / previous["directory"]
            if all((folder / name).is_file() for name in ("kpis.json", "mensal.csv", "estatisticas.csv", "participantes.csv", "cards.csv")):
                return None
    return publish(calculate(snapshot, reference, year), output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container")
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("output"))
    parser.add_argument("--date", type=date.fromisoformat, help="Data de corte, AAAA-MM-DD")
    parser.add_argument("--year", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--watch-seconds", type=int, help="Consultar novamente nesse intervalo (mínimo 30s)")
    args = parser.parse_args()
    if args.watch_seconds is not None and args.watch_seconds < 30:
        parser.error("Intervalo mínimo: 30 segundos")
    while True:
        folder = run_once(args.output, args.container, args.database, args.user, args.date, args.year, args.force)
        print("KPIs publicados em " + str(folder) if folder else "Snapshot e data sem mudanças", flush=True)
        if args.watch_seconds is None:
            break
        time.sleep(args.watch_seconds)


if __name__ == "__main__":
    main()

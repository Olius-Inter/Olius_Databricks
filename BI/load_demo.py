"""Gera SQL autocontido; modo adicional preserva cadastros de uma base existente."""
import argparse
import json
from pathlib import Path
import os
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def build_sql(additive=False):
    folder = ROOT / "PostgreSQL/data_load"
    config = json.loads((folder / "config.json").read_text(encoding="utf-8"))
    helpers = (folder / "00_helpers.sql").read_text(encoding="utf-8")
    helpers = helpers.replace(":'load_config'", literal(json.dumps(config, ensure_ascii=False)))
    seed = (folder / "01_seed.sql").read_text(encoding="utf-8")
    seed = "\n".join(line for line in seed.splitlines()
                     if not line.startswith("\\") and line not in ("BEGIN;", "COMMIT;"))
    start = seed.index("INSERT INTO addresses(")
    end = seed.index("INSERT INTO establishment(", start)
    locations = json.loads((ROOT / "BI/addresses.json").read_text(encoding="utf-8"))
    rows = []
    for kind, owner, count in (("est_address", "ESTABLISHMENT", 30), ("pev_address", "PEV", 8)):
        for i in range(1, count + 1):
            address = locations[(i - 1) % len(locations)]
            fields = [owner, "SP", "São Paulo", address["neighborhood"], address["street"],
                      address["number"], address["cep"]]
            rows.append(f"(pg_temp.dl_id('{kind}',{i})," + ",".join(map(literal, fields)) + ")")
    seed = seed[:start] + ("INSERT INTO addresses(id,owner_kind,state,city,neighborhood,street,number,cep) VALUES\n"
                            + ",\n".join(rows) + ";\n") + seed[end:]
    # Coordenadas ficam NULL: geocodificação deve ser feita pelo Maps, sem inventar GPS.
    history = (ROOT / "BI/demo_history.sql").read_text(encoding="utf-8")
    guard = """
DO $$ DECLARE t RECORD; occupied BOOLEAN; BEGIN
  IF NOT pg_try_advisory_xact_lock(71007, 2) THEN RAISE EXCEPTION 'Carga em andamento'; END IF;
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public'
    AND tablename NOT IN ('data_catalog_table','data_catalog_column','data_catalog_role') LOOP
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I)', t.tablename) INTO occupied;
    IF occupied THEN RAISE EXCEPTION 'Tabela % não vazia; carga recusada',t.tablename; END IF;
  END LOOP;
END $$;
"""
    if additive:
        return build_additive_sql(helpers, seed, history)
    return "BEGIN ISOLATION LEVEL REPEATABLE READ;\n" + guard + helpers + seed + history + "\nCALL dm.refresh_data_mart();\nCOMMIT;\n"


def build_additive_sql(helpers, seed, history):
    """Isola a massa por UUID, documentos, e-mails e nomes dos catálogos."""
    helpers = helpers.replace("olius-data-load-v1:", "olius-bi-cloud-v1:")
    helpers = helpers.replace("CREATE FUNCTION pg_temp.", "CREATE OR REPLACE FUNCTION pg_temp.")
    helpers = helpers.replace("CREATE TEMP TABLE dl_config AS", "CREATE TEMP TABLE dl_config ON COMMIT DROP AS")
    helpers = helpers.replace("810000000 + number", "820000000 + number")
    for name in ("Plano Básico", "Plano Profissional", "Plano Empresa"):
        helpers = helpers.replace(name, "[DEMO BI CLOUD] " + name)
    seed = seed.replace("@olius.example.invalid", "@bi-cloud.olius.example.invalid")
    seed = seed.replace("'[DEMO]", "'[DEMO BI CLOUD]")
    seed = seed.replace("910000000+i", "920000000+i")
    for name in ("Restaurante", "Lanchonete", "Padaria"):
        seed = seed.replace("'" + name + "'", "'[DEMO BI CLOUD] " + name + "'")
    # Nunca percorrer todos os usuários de um banco existente para inserir QR/telefone.
    owners = """ AND id IN (
      SELECT pg_temp.dl_id(kind,i)
      FROM (VALUES ('est',30),('citizen',80)) own(kind,quantity)
      CROSS JOIN LATERAL generate_series(1,quantity) n(i))"""
    seed = seed.replace("FROM users WHERE user_type<>'ADMIN'", "FROM users WHERE user_type<>'ADMIN'" + owners)
    seed = seed.replace("('COMPROMISING_OCCURRENCE','Ocorrência comprometedora');",
                        "('COMPROMISING_OCCURRENCE','Ocorrência comprometedora') ON CONFLICT (code) DO NOTHING;\n"
                        "SELECT pg_temp.dl_assert((SELECT count(*) FROM collection_failure_reason "
                        "WHERE code IN ('VOLUME_OUT_OF_TOLERANCE','OIL_UNACCEPTABLE',"
                        "'COMPROMISING_OCCURRENCE') AND status='ACTIVE')=3,"
                        "'Os três motivos oficiais de falha devem estar ativos');")
    history = history.replace("CREATE TEMP TABLE bi_clock AS", "CREATE TEMP TABLE bi_clock ON COMMIT DROP AS")
    own_est = "SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i)"
    own_citizens = "SELECT pg_temp.dl_id('citizen',i) FROM generate_series(1,80) n(i)"
    history = history.replace("FROM collection)=780", f"FROM collection WHERE establishment_id IN ({own_est}))=780")
    history = history.replace("FROM delivery_pev)=520", f"FROM delivery_pev WHERE citizen_id IN ({own_citizens}))=520")
    history = history.replace("WHERE a.reserved_collection_slots", f"WHERE c.establishment_id IN ({own_est}) AND (a.reserved_collection_slots")
    history = history.replace(">c.volume_limit_liters),", ">c.volume_limit_liters)),")
    gate = """
CREATE TEMP TABLE bi_cloud_run(loaded BOOLEAN NOT NULL) ON COMMIT DROP;
INSERT INTO bi_cloud_run VALUES(FALSE);
DO $bi_gate$
DECLARE users_count INTEGER; collections_count INTEGER; deliveries_count INTEGER; requests_count INTEGER;
BEGIN
  IF NOT pg_try_advisory_xact_lock(71007, 2) THEN RAISE EXCEPTION 'Carga em andamento'; END IF;
  SELECT count(*) INTO users_count FROM users WHERE id IN (
    SELECT pg_temp.dl_id(kind,i) FROM (VALUES ('admin',2),('citizen',80),('est',30)) own(kind,quantity)
    CROSS JOIN LATERAL generate_series(1,quantity) n(i));
  IF users_count<>0 THEN
    SELECT count(*) INTO collections_count FROM collection WHERE establishment_id IN (
      SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i));
    SELECT count(*) INTO deliveries_count FROM delivery_pev WHERE citizen_id IN (
      SELECT pg_temp.dl_id('citizen',i) FROM generate_series(1,80) n(i));
    SELECT count(*) INTO requests_count FROM collection_request WHERE establishment_id IN (
      SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i));
    IF users_count=112 AND collections_count=780 AND deliveries_count=520 AND requests_count=825 THEN
      RAISE NOTICE 'Massa BI CLOUD já carregada; nenhuma operação será duplicada.';
      RETURN;
    END IF;
    RAISE EXCEPTION 'Massa BI CLOUD parcial ou alterada; nenhuma linha foi modificada. Revise os registros de demonstração.';
  END IF;
  EXECUTE $bi_mass$
"""
    return ("-- Carga ADICIONAL: preserva os registros existentes. Executar o arquivo inteiro.\n"
            "BEGIN ISOLATION LEVEL REPEATABLE READ;\nSET LOCAL search_path=public,pg_temp;\n"
            + helpers + gate + seed + "\n" + history + """
$bi_mass$;
  UPDATE bi_cloud_run SET loaded=TRUE;
  CALL dm.refresh_data_mart();
END;
$bi_gate$;
SELECT CASE WHEN loaded THEN 'Carga adicional concluída: 780 coletas, 520 entregas, 825 solicitações; dm atualizado.'
       ELSE 'Carga BI CLOUD já existente; sem duplicações.' END AS resultado
FROM bi_cloud_run;
COMMIT;
""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql-out", type=Path, help="Somente gerar SQL, sem conexão")
    parser.add_argument("--additive", action="store_true", help="Adicionar massa isolada a um banco com dados existentes")
    parser.add_argument("--container", help="Alternativa à conexão libpq por PGHOST etc.")
    parser.add_argument("--database", default="olius_bi_demo")
    parser.add_argument("--user", default="postgres")
    args = parser.parse_args()
    sql = build_sql(additive=args.additive)
    if args.sql_out:
        args.sql_out.parent.mkdir(parents=True, exist_ok=True)
        args.sql_out.write_text(sql, encoding="utf-8")
        print("SQL gerado:", args.sql_out)
        return
    target = f"{args.container or os.environ.get('PGHOST', 'localhost')}/{args.database}"
    if os.environ.get("OLIUS_BI_DEMO_TARGET") != target:
        parser.error("Confirme o destino em OLIUS_BI_DEMO_TARGET=" + target)
    if args.container:
        result = subprocess.run(["docker", "exec", "-i", args.container, "psql", "-X", "-w",
                                 "-U", args.user, "-d", args.database, "-v", "ON_ERROR_STOP=1"],
                                input=sql, text=True, encoding="utf-8", capture_output=True, timeout=300)
        if result.returncode:
            raise RuntimeError(result.stderr[-4000:])
    else:
        import psycopg
        with psycopg.connect(dbname=args.database, user=args.user) as connection:
            connection.execute(sql, prepare=False)
    print("Carga verificada sem erros. Modo: " + ("adicional, sem duplicar uma massa já existente" if args.additive else "base vazia"))


if __name__ == "__main__":
    main()

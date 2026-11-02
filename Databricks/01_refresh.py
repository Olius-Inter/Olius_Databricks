# Databricks notebook source
"""Job de refresh no PostgreSQL; não ingere nem copia os dados do Data Mart.

Dependência do ambiente serverless: psycopg[binary]==3.2.10.
Configuração e homologação: documents/02_execution_guide.md.
"""

import json
import re
import ssl
from pathlib import Path
from tempfile import TemporaryDirectory


def normalize_ca_pem(value):
    """Reconstrói linhas de um certificado/bundle e valida sem exibir seu valor."""
    value = value.replace(r"\r\n", "\n").replace(r"\n", "\n").strip()
    pattern = r"-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\s]+)-----END CERTIFICATE-----"
    bodies = re.findall(pattern, value)
    if not bodies or re.sub(pattern, "", value).strip():
        raise ValueError("pg-ca-pem deve conter somente certificados PEM completos")
    certificates = []
    for body in bodies:
        body = "".join(body.split())
        lines = "\n".join(body[i:i + 64] for i in range(0, len(body), 64))
        certificates.append("-----BEGIN CERTIFICATE-----\n" + lines + "\n-----END CERTIFICATE-----\n")
    pem = "".join(certificates)
    try:
        ssl.create_default_context(cadata=pem)
    except (ssl.SSLError, ValueError):
        raise ValueError("pg-ca-pem não contém um certificado CA reconhecido") from None
    return pem


def refresh_data_mart(connect, settings):
    """Confirma o novo snapshot somente após COMMIT; erro causa rollback.

    `connect` segue o contrato psycopg.connect e é injetado para testes locais.
    Não há retry interno: o Job controla as tentativas.
    """
    with connect(**settings, autocommit=False) as connection:
        with connection.cursor() as cursor:
            # Primeira instrução: todas as MVs usam a mesma fotografia da origem.
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT snapshot_id FROM dm.refresh_state WHERE singleton")
            previous = cursor.fetchone()
            if previous is None:
                raise RuntimeError("dm.refresh_state sem registro de controle")
            cursor.execute("CALL dm.refresh_data_mart()")
            cursor.execute(
                "SELECT snapshot_id, refreshed_at FROM dm.refresh_state WHERE singleton"
            )
            current = cursor.fetchone()
            if current is None or current[0] == previous[0] or current[1] is None:
                raise RuntimeError("Refresh não produziu novo snapshot válido")
        # Contexto psycopg confirma a transação e fecha a conexão ao sair.
    return {"snapshot_id": str(current[0]), "refreshed_at": current[1].isoformat()}


def run_notebook(dbutils):
    """Usa secrets; nenhuma senha deve ser passada como parâmetro do Job."""
    import psycopg

    dbutils.widgets.text("secret_catalog", "workspace")
    dbutils.widgets.text("secret_schema", "olius_bi")
    secret_catalog = dbutils.widgets.get("secret_catalog").strip()
    secret_schema = dbutils.widgets.get("secret_schema").strip()
    if not secret_catalog or not secret_schema:
        raise ValueError("Informe catálogo e schema das secrets")

    def secret(key):
        return dbutils.secrets.get(catalog=secret_catalog, schema=secret_schema, key=key)

    # CA só existe no diretório temporário desta execução, nunca no Git/output.
    with TemporaryDirectory(prefix="olius-bi-ca-") as directory:
        certificate = Path(directory) / "ca.pem"
        certificate.write_text(normalize_ca_pem(secret("pg-ca-pem")), encoding="utf-8")
        settings = {
            "host": secret("pg-host"),
            "port": int(secret("pg-port")),
            "dbname": secret("pg-database"),
            "user": "olius_bi_refresh",
            "password": secret("pg-refresh-password"),
            "sslmode": "verify-full",
            "sslrootcert": str(certificate),
            "connect_timeout": 15,
            "application_name": "olius_databricks_refresh",
            "options": "-c statement_timeout=900000 -c lock_timeout=30000",
        }
        result = refresh_data_mart(psycopg.connect, settings)
    print(json.dumps(result))
    return result


if __name__ == "__main__":
    # Não mostrar strings de conexão nem detalhes potencialmente sensíveis de erros.
    try:
        run_notebook(dbutils)  # noqa: F821 - fornecido pelo notebook Databricks
    except Exception:
        raise RuntimeError(
            "Refresh não confirmado. Verifique rede, secrets, TLS, permissões, "
            "concorrência e dm.refresh_state com o administrador. "
            "O dashboard não deve ser atualizado como tarefa dependente desta falha."
        ) from None

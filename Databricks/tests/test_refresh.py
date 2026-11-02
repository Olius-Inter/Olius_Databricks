"""Testes sem rede, Spark, psycopg instalado ou credenciais."""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("refresh", ROOT / "01_refresh.py")
refresh = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(refresh)


class RefreshTests(TestCase):
    def setUp(self):
        self.connect = MagicMock()
        self.connection = self.connect.return_value.__enter__.return_value
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.now = datetime(2026, 10, 18, tzinfo=timezone.utc)
        self.cursor.fetchone.side_effect = [("old",), ("new", self.now)]

    def test_success_is_returned_after_commit_context(self):
        result = refresh.refresh_data_mart(self.connect, {"host": "example"})
        self.assertEqual(result, {"snapshot_id": "new", "refreshed_at": self.now.isoformat()})
        self.connect.assert_called_once_with(host="example", autocommit=False)
        commands = [call.args[0] for call in self.cursor.execute.call_args_list]
        self.assertEqual(commands[0], "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self.assertEqual(commands[2], "CALL dm.refresh_data_mart()")
        self.connect.return_value.__exit__.assert_called_once_with(None, None, None)

    def test_invalid_state_fails_and_leaves_rollback_to_connection(self):
        for rows in ([None], [("old",), None], [("old",), ("old", self.now)],
                     [("old",), ("new", None)]):
            with self.subTest(rows=rows):
                self.cursor.fetchone.side_effect = rows
                with self.assertRaises(RuntimeError):
                    refresh.refresh_data_mart(self.connect, {})
                self.assertIsNotNone(self.connect.return_value.__exit__.call_args.args[0])

    def test_procedure_failure_propagates_without_retry(self):
        self.cursor.execute.side_effect = [None, None, RuntimeError("busy")]
        with self.assertRaisesRegex(RuntimeError, "busy"):
            refresh.refresh_data_mart(self.connect, {})
        self.assertEqual(self.connect.call_count, 1)

    def test_commit_failure_is_not_reported_as_success(self):
        self.connect.return_value.__exit__.side_effect = RuntimeError("commit failed")
        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            refresh.refresh_data_mart(self.connect, {})

    def test_connection_failure_propagates(self):
        self.connect.side_effect = RuntimeError("unreachable")
        with self.assertRaisesRegex(RuntimeError, "unreachable"):
            refresh.refresh_data_mart(self.connect, {})

    def test_notebook_reads_secrets_and_requires_tls(self):
        utilities = MagicMock()
        utilities.widgets.get.side_effect = {"secret_catalog": "workspace",
                                             "secret_schema": "olius_bi"}.__getitem__
        values = {"pg-host": "example", "pg-port": "5432", "pg-database": "olius",
                  "pg-refresh-password": "test-only", "pg-ca-pem": "test-ca"}
        utilities.secrets.get.side_effect = lambda catalog, schema, key: values[key]
        fake_psycopg = SimpleNamespace(connect=self.connect)
        with patch.dict("sys.modules", {"psycopg": fake_psycopg}), \
                patch.object(refresh, "TemporaryDirectory") as temporary, \
                patch.object(refresh.Path, "write_text") as write, \
                patch("builtins.print") as output, \
                patch.object(refresh, "normalize_ca_pem", return_value="normalized-ca") as normalize:
            temporary.return_value.__enter__.return_value = "/temporary-ca"
            result = refresh.run_notebook(utilities)
        settings = self.connect.call_args.kwargs
        self.assertEqual(settings["sslmode"], "verify-full")
        self.assertEqual(settings["user"], "olius_bi_refresh")
        self.assertEqual(settings["password"], "test-only")
        normalize.assert_called_once_with("test-ca")
        write.assert_called_once_with("normalized-ca", encoding="utf-8")
        temporary.return_value.__exit__.assert_called_once_with(None, None, None)
        self.assertIn(result["snapshot_id"], output.call_args.args[0])
        self.assertNotIn("test-only", output.call_args.args[0])
        self.assertEqual(utilities.secrets.get.call_count, 5)
        for call in utilities.secrets.get.call_args_list:
            self.assertEqual(call.kwargs["catalog"], "workspace")
            self.assertEqual(call.kwargs["schema"], "olius_bi")

    def test_blank_secret_location_fails_before_connection(self):
        for location in ((" ", "olius_bi"), ("workspace", " ")):
            with self.subTest(location=location):
                utilities = MagicMock()
                utilities.widgets.get.side_effect = location
                with patch.dict("sys.modules", {"psycopg": SimpleNamespace()}):
                    with self.assertRaises(ValueError):
                        refresh.run_notebook(utilities)
                utilities.secrets.get.assert_not_called()

    def test_normalize_pem_formats_and_bundle(self):
        body = "A" * 80
        one_line = "-----BEGIN CERTIFICATE-----" + body + "-----END CERTIFICATE-----"
        expected = "-----BEGIN CERTIFICATE-----\n" + "A" * 64 + "\n" + "A" * 16 + "\n-----END CERTIFICATE-----\n"
        variants = (one_line, expected, expected.replace("\n", r"\n"),
                    expected.replace("\n", "\r\n"), expected.replace("\n", r"\r\n"))
        with patch.object(refresh.ssl, "create_default_context") as validate:
            for value in variants:
                self.assertEqual(refresh.normalize_ca_pem(value), expected)
                validate.assert_called_with(cadata=expected)
            self.assertEqual(refresh.normalize_ca_pem(one_line + one_line), expected * 2)

    def test_normalize_rejects_missing_markers_and_extra_content(self):
        for value in ("", "ca.pem", "-----BEGIN CERTIFICATE-----AAAA",
                      "junk-----BEGIN CERTIFICATE-----AAAA-----END CERTIFICATE-----"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                refresh.normalize_ca_pem(value)

    def test_normalize_rejects_invalid_certificate_using_real_ssl(self):
        with self.assertRaisesRegex(ValueError, "não contém um certificado"):
            refresh.normalize_ca_pem("-----BEGIN CERTIFICATE-----AAAA-----END CERTIFICATE-----")

if __name__ == "__main__":
    main()

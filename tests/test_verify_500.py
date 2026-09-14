"""Unit tests for scripts.verify_500 (Day Wed milestone)."""

from unittest.mock import MagicMock, patch

from scripts.verify_500 import check_es, check_pg, main


def make_pg_cursor(total: int, unique: int) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    cursor.fetchone.return_value = (total, unique)
    return cursor


def test_check_pg_pass():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = make_pg_cursor(523, 523)
    with patch("psycopg2.connect", return_value=mock_conn):
        assert check_pg("postgresql://test/db") == (True, True)


def test_check_pg_count_fail():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = make_pg_cursor(100, 100)
    with patch("psycopg2.connect", return_value=mock_conn):
        assert check_pg("postgresql://test/db") == (False, True)


def test_check_pg_dedup_fail():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = make_pg_cursor(523, 500)
    with patch("psycopg2.connect", return_value=mock_conn):
        assert check_pg("postgresql://test/db") == (True, False)


def test_check_pg_missing_url():
    assert check_pg(None) == (False, False)


def test_check_es_pass():
    mock_es = MagicMock()
    mock_es.ping.return_value = True
    mock_es.count.return_value = {"count": 523}
    mock_es.search.return_value = {"hits": {"total": {"value": 523}}}
    with patch("elasticsearch.Elasticsearch", return_value=mock_es):
        assert check_es("http://localhost:9200", "jobs") == (True, True)


def test_check_es_count_fail():
    mock_es = MagicMock()
    mock_es.ping.return_value = True
    mock_es.count.return_value = {"count": 10}
    mock_es.search.return_value = {"hits": {"total": {"value": 10}}}
    with patch("elasticsearch.Elasticsearch", return_value=mock_es):
        assert check_es("http://localhost:9200", "jobs") == (False, True)


def test_check_es_missing_url():
    assert check_es(None, "jobs") == (False, False)


def test_main_all_pass_returns_zero():
    with (
        patch("scripts.verify_500.check_pg", return_value=(True, True)),
        patch("scripts.verify_500.check_es", return_value=(True, True)),
    ):
        assert main() == 0


def test_main_any_fail_returns_one():
    with (
        patch("scripts.verify_500.check_pg", return_value=(True, False)),
        patch("scripts.verify_500.check_es", return_value=(True, True)),
    ):
        assert main() == 1

"""Unit tests for scripts.seed_loader (CRAWL-01-07 fallback)."""

import json
import logging
from unittest.mock import MagicMock, patch

from scripts.seed_loader import load_seed


def write_seed_file(tmp_path, records) -> str:
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return str(path)


SAMPLE_RECORDS = [
    {
        "source_url": "https://vieclam.gov.vn/seed/1",
        "title": "Ky su phan mem",
        "company": "Cong ty TNHH ABC",
        "location": "Da Nang",
        "salary_raw": "10 - 20 trieu",
        "salary_min": 10000000,
        "salary_max": 20000000,
        "salary_currency": "VND",
        "description": "Mo ta",
        "requirements": "Yeu cau",
        "category": "CNTT",
    },
    {
        "source_url": "https://vieclam.gov.vn/seed/2",
        "title": "Ke toan",
        "company": "Cong ty XYZ",
        "location": "Ha Noi",
        "salary_raw": "8 - 12 trieu",
        "salary_min": 8000000,
        "salary_max": 12000000,
        "salary_currency": "VND",
        "description": "Mo ta",
        "requirements": "Yeu cau",
        "category": "Ke toan",
    },
]


def make_pg_mock():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    return mock_conn, mock_cursor


def test_load_seed_inserts_pg_and_indexes_es(tmp_path, caplog):
    seed_path = write_seed_file(tmp_path, SAMPLE_RECORDS)
    mock_conn, mock_cursor = make_pg_mock()

    mock_es = MagicMock()
    mock_es.ping.return_value = True

    with (
        patch("psycopg2.connect", return_value=mock_conn),
        patch("elasticsearch.Elasticsearch", return_value=mock_es),
        patch("elasticsearch.helpers.bulk", return_value=(2, [])) as mock_bulk,
        caplog.at_level(logging.INFO, logger="crawler.scripts.seed_loader"),
    ):
        load_seed(
            pg_url="postgresql://test/db",
            es_url="http://localhost:9200",
            es_index="jobs",
            seed_path=seed_path,
        )

    # PG: CREATE + bulk executemany with 2 records
    assert mock_cursor.execute.called
    mock_cursor.executemany.assert_called_once()
    assert len(mock_cursor.executemany.call_args[0][1]) == 2
    # ES: bulk indexed 2 docs
    assert mock_bulk.called
    assert "Loaded 2 seed records into PostgreSQL." in caplog.text
    assert "Indexed 2 seed documents into Elasticsearch." in caplog.text


def test_load_seed_missing_file_logs_error(tmp_path, caplog):
    with caplog.at_level(logging.ERROR, logger="crawler.scripts.seed_loader"):
        load_seed(
            pg_url="postgresql://test/db",
            es_url="http://localhost:9200",
            es_index="jobs",
            seed_path=str(tmp_path / "nope.json"),
        )
    assert "Seed file not found" in caplog.text


def test_load_seed_empty_file_warns(tmp_path, caplog):
    seed_path = write_seed_file(tmp_path, [])
    with caplog.at_level(logging.WARNING, logger="crawler.scripts.seed_loader"):
        load_seed(
            pg_url="postgresql://test/db",
            es_url="http://localhost:9200",
            es_index="jobs",
            seed_path=seed_path,
        )
    assert "empty" in caplog.text.lower()


def test_load_seed_skips_pg_without_url(tmp_path):
    seed_path = write_seed_file(tmp_path, SAMPLE_RECORDS)
    mock_es = MagicMock()
    mock_es.ping.return_value = True
    with (
        patch("psycopg2.connect") as mock_connect,
        patch("elasticsearch.Elasticsearch", return_value=mock_es),
        patch("elasticsearch.helpers.bulk", return_value=(2, [])),
        patch.dict("os.environ", {}, clear=False),
    ):
        import os

        os.environ.pop("DATABASE_URL_CRAWLER", None)
        load_seed(
            pg_url=None,
            es_url="http://localhost:9200",
            es_index="jobs",
            seed_path=seed_path,
        )
    mock_connect.assert_not_called()

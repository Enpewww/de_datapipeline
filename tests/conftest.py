import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY = "MOCK_KEY1234"):
        yield Variable.get("API_KEY")

@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE = "STATSQUEST"):
        yield Variable.get("CHANNEL_HANDLE")

@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login = "mock_username",
        password = "mock_password",
        host="mock_host",
        port = 1234,
        schema = "mock_db_name"
    )
    conn_url = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_url):
        yield Connection.get_connection_from_secrets(conn_id = "POSTGRES_DB_YT_ELT")


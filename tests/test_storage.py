from libs.storage import _clickhouse_client_kwargs_from_dsn


def test_clickhouse_client_kwargs_from_dsn_disables_sessions():
    kwargs = _clickhouse_client_kwargs_from_dsn(
        "http://default:clickhousepassword@clickhouse:8123/analyze_bot"
    )

    assert kwargs == {
        "host": "clickhouse",
        "port": 8123,
        "username": "default",
        "password": "clickhousepassword",
        "database": "analyze_bot",
        "autogenerate_session_id": False,
    }



def test_clickhouse_client_kwargs_from_dsn_uses_defaults():
    kwargs = _clickhouse_client_kwargs_from_dsn("http://localhost:8123")

    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 8123
    assert kwargs["username"] == "default"
    assert kwargs["password"] == ""
    assert kwargs["database"] == "analyze_bot"
    assert kwargs["autogenerate_session_id"] is False

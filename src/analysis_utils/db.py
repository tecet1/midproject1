from __future__ import annotations

import os
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def build_mysql_engine(
    *,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    **engine_kwargs: Any,
) -> Engine:
    host = host or os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(port or os.getenv("MYSQL_PORT", "3306"))
    user = user or os.getenv("MYSQL_USER", "root")
    password = password or os.getenv("MYSQL_PASSWORD")
    database = database or os.getenv("MYSQL_DATABASE", "midproject1")

    if not password:
        raise ValueError(
            "MySQL password is required. Set MYSQL_PASSWORD or pass password=..."
        )

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True, **engine_kwargs)


def test_mysql_connection(engine: Engine) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    DATABASE() AS current_db,
                    CURRENT_USER() AS connected_user,
                    NOW() AS server_time
                """
            )
        ).mappings().one()

    return dict(row)

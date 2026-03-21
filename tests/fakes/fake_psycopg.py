"""Deterministic fake psycopg backend for hermetic tests and benchmarks."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Callable


def _optional_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


@dataclass
class FakePsycopgConfig:
    execute_base_ms: float = 2.0
    execute_per_row_ms: float = 0.01
    copy_base_ms: float = 1.0
    copy_per_row_ms: float = 0.005
    commit_ms: float = 0.25
    server_latency_ms: float = 25.0
    fail_on_executemany_call: int | None = None
    fail_on_copy_call: int | None = None
    fail_on_commit_call: int | None = None

    @classmethod
    def from_env(cls) -> "FakePsycopgConfig":
        return cls(
            execute_base_ms=float(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_EXECUTE_BASE_MS", "2.0")
            ),
            execute_per_row_ms=float(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_EXECUTE_PER_ROW_MS", "0.01")
            ),
            copy_base_ms=float(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_COPY_BASE_MS", "1.0")
            ),
            copy_per_row_ms=float(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_COPY_PER_ROW_MS", "0.005")
            ),
            commit_ms=float(os.getenv("LAKEBASE_WRITER_FAKE_DB_COMMIT_MS", "0.25")),
            server_latency_ms=float(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_SERVER_LATENCY_MS", "25.0")
            ),
            fail_on_executemany_call=_optional_int(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_FAIL_ON_EXECUTEMANY_CALL")
            ),
            fail_on_copy_call=_optional_int(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_FAIL_ON_COPY_CALL")
            ),
            fail_on_commit_call=_optional_int(
                os.getenv("LAKEBASE_WRITER_FAKE_DB_FAIL_ON_COMMIT_CALL")
            ),
        )


@dataclass
class FakeTable:
    rows: list[dict[str, Any]] = field(default_factory=list)
    primary_keys: tuple[str, ...] = ()


class FakeDatabaseState:
    def __init__(self):
        self.tables: dict[str, FakeTable] = {}

    def drop_table(self, table_name: str) -> None:
        self.tables.pop(table_name, None)

    def create_table(self, table_name: str) -> None:
        self.tables[table_name] = FakeTable()

    def ensure_table(self, table_name: str) -> FakeTable:
        return self.tables.setdefault(table_name, FakeTable())

    def add_primary_key(self, table_name: str, columns: list[str]) -> None:
        self.ensure_table(table_name).primary_keys = tuple(columns)

    def apply_rows(
        self,
        table_name: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        *,
        primary_keys: tuple[str, ...] = (),
        server_latency_ms: float,
    ) -> None:
        table = self.ensure_table(table_name)
        effective_primary_keys = primary_keys or table.primary_keys

        for index, row in enumerate(rows):
            payload = dict(zip(columns, row, strict=False))
            ts = payload.get("ts")
            if isinstance(ts, datetime):
                payload["_server_ts"] = ts + timedelta(
                    milliseconds=server_latency_ms + (index * 0.01)
                )
            else:
                payload["_server_ts"] = datetime.now(UTC)

            if effective_primary_keys:
                for existing in table.rows:
                    existing_key = tuple(
                        existing[key] for key in effective_primary_keys
                    )
                    candidate_key = tuple(
                        payload[key] for key in effective_primary_keys
                    )
                    if existing_key == candidate_key:
                        existing.update(payload)
                        break
                else:
                    table.rows.append(payload)
            else:
                table.rows.append(payload)

    def row_count(self, table_name: str) -> int:
        return len(self.ensure_table(table_name).rows)

    def latency_rows(self, table_name: str) -> list[tuple[Any, Any]]:
        rows = sorted(
            self.ensure_table(table_name).rows,
            key=lambda row: row.get("_server_ts", datetime.min.replace(tzinfo=UTC)),
        )
        return [
            (row.get("ts"), row.get("_server_ts"))
            for row in rows
            if row.get("ts") is not None and row.get("_server_ts") is not None
        ]

    def table_rows(self, table_name: str) -> list[dict[str, Any]]:
        return list(self.ensure_table(table_name).rows)


_STATE = FakeDatabaseState()
_CONFIG = FakePsycopgConfig.from_env()
_CALL_COUNTS = {"executemany": 0, "copy": 0, "commit": 0}


def reset_fake_psycopg(**overrides: Any) -> None:
    global _STATE, _CONFIG, _CALL_COUNTS
    _STATE = FakeDatabaseState()
    _CONFIG = FakePsycopgConfig.from_env()
    configure_fake_psycopg(**overrides)
    _CALL_COUNTS = {"executemany": 0, "copy": 0, "commit": 0}


def configure_fake_psycopg(**overrides: Any) -> None:
    for key, value in overrides.items():
        setattr(_CONFIG, key, value)


def ensure_fake_table(table_name: str, primary_keys: tuple[str, ...] = ()) -> None:
    table = _STATE.ensure_table(table_name)
    if primary_keys:
        table.primary_keys = primary_keys


def get_fake_row_count(table_name: str) -> int:
    return _STATE.row_count(table_name)


def get_fake_rows(table_name: str) -> list[dict[str, Any]]:
    return _STATE.table_rows(table_name)


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection
        self._result: list[tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        statement = " ".join(sql.strip().split())

        if match := re.match(r"DROP TABLE IF EXISTS ([\w_]+)", statement):
            table_name = match.group(1)
            self.connection.pending_operations.append(
                lambda: _STATE.drop_table(table_name)
            )
            return

        if match := re.match(r"CREATE TABLE ([\w_]+)", statement):
            table_name = match.group(1)
            self.connection.pending_operations.append(
                lambda: _STATE.create_table(table_name)
            )
            return

        if match := re.match(
            r"ALTER TABLE ([\w_]+) ADD PRIMARY KEY \(([^)]+)\)", statement
        ):
            table_name = match.group(1)
            primary_keys = _split_columns(match.group(2))
            self.connection.pending_operations.append(
                lambda: _STATE.add_primary_key(table_name, primary_keys)
            )
            return

        if match := re.match(r"SELECT COUNT\(\*\) FROM ([\w_]+)", statement):
            self._result = [(_STATE.row_count(match.group(1)),)]
            return

        if match := re.search(r"FROM ([\w_]+) WHERE ts IS NOT NULL", statement):
            self._result = _STATE.latency_rows(match.group(1))
            return

        raise ValueError(f"Unsupported SQL for fake psycopg execute(): {statement}")

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        _CALL_COUNTS["executemany"] += 1
        if _should_fail(_CALL_COUNTS["executemany"], _CONFIG.fail_on_executemany_call):
            raise Exception("Fake executemany failure")

        table_name, columns, primary_keys = _parse_insert_sql(sql)
        _sleep_ms(_CONFIG.execute_base_ms + (_CONFIG.execute_per_row_ms * len(rows)))
        self.connection.pending_operations.append(
            lambda: _STATE.apply_rows(
                table_name,
                columns,
                rows,
                primary_keys=primary_keys,
                server_latency_ms=_CONFIG.server_latency_ms,
            )
        )

    def copy(self, sql: str) -> "FakeCopy":
        table_name, columns = _parse_copy_sql(sql)
        return FakeCopy(self.connection, table_name, columns)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result)


class FakeCopy:
    def __init__(
        self, connection: "FakeConnection", table_name: str, columns: list[str]
    ):
        self.connection = connection
        self.table_name = table_name
        self.columns = columns
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            return None

        _CALL_COUNTS["copy"] += 1
        if _should_fail(_CALL_COUNTS["copy"], _CONFIG.fail_on_copy_call):
            raise Exception("Fake copy failure")

        _sleep_ms(_CONFIG.copy_base_ms + (_CONFIG.copy_per_row_ms * len(self.rows)))
        self.connection.pending_operations.append(
            lambda: _STATE.apply_rows(
                self.table_name,
                self.columns,
                self.rows,
                server_latency_ms=_CONFIG.server_latency_ms,
            )
        )
        return None

    def write_row(self, row: tuple[Any, ...]) -> None:
        self.rows.append(tuple(row))


class FakeConnection:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.pending_operations: list[Callable[[], None]] = []
        self.closed = False

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        _CALL_COUNTS["commit"] += 1
        if _should_fail(_CALL_COUNTS["commit"], _CONFIG.fail_on_commit_call):
            self.pending_operations.clear()
            raise Exception("Fake commit failure")

        _sleep_ms(_CONFIG.commit_ms)
        pending = list(self.pending_operations)
        self.pending_operations.clear()
        for operation in pending:
            operation()

    def rollback(self) -> None:
        self.pending_operations.clear()

    def close(self) -> None:
        self.closed = True


def connect(**kwargs: Any) -> FakeConnection:
    return FakeConnection(**kwargs)


class psycopg:
    Connection = FakeConnection
    Cursor = FakeCursor
    connect = staticmethod(connect)


def _sleep_ms(duration_ms: float) -> None:
    if duration_ms > 0:
        time.sleep(duration_ms / 1000)


def _split_columns(raw_columns: str) -> list[str]:
    return [column.strip() for column in raw_columns.split(",")]


def _parse_insert_sql(sql: str) -> tuple[str, list[str], tuple[str, ...]]:
    compact_sql = " ".join(sql.strip().split())
    insert_match = re.search(r"INSERT INTO ([\w_]+) \(([^)]+)\) VALUES", compact_sql)
    if insert_match is None:
        raise ValueError(f"Unsupported INSERT SQL for fake psycopg: {compact_sql}")

    primary_keys: tuple[str, ...] = ()
    if conflict_match := re.search(r"ON CONFLICT \(([^)]+)\)", compact_sql):
        primary_keys = tuple(_split_columns(conflict_match.group(1)))

    return (
        insert_match.group(1),
        _split_columns(insert_match.group(2)),
        primary_keys,
    )


def _parse_copy_sql(sql: str) -> tuple[str, list[str]]:
    compact_sql = " ".join(sql.strip().split())
    copy_match = re.search(r"COPY ([\w_]+) \(([^)]+)\) FROM STDIN", compact_sql)
    if copy_match is None:
        raise ValueError(f"Unsupported COPY SQL for fake psycopg: {compact_sql}")
    return copy_match.group(1), _split_columns(copy_match.group(2))


def _should_fail(call_count: int, fail_on_call: int | None) -> bool:
    return fail_on_call is not None and call_count == fail_on_call

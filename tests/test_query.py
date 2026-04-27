"""Unit tests for query builders and compile_query."""
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlmodel import Field

from adc_aiopg.query import compile_query, create, search, get_by_id, update_by_id, delete_by_id, count, delete
from adc_aiopg.types import Base


meta = MetaData()
users = sa.Table(
    "users",
    meta,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
    sa.Column("age", sa.Integer),
    sa.Column("updated", sa.DateTime),
)


# SQLModel model for _by_id queries (they access table.id as a class attribute)
query_meta = MetaData()


class UserModel(Base, table=True):
    __tablename__ = "users_model"
    metadata = query_meta
    id: int | None = Field(default=None, primary_key=True)
    name: str | None = None
    age: int | None = None
    updated: datetime | None = None


class TestCompileQuery:
    def test_returns_query_and_params(self):
        q = sa.select(users).where(users.c.id == 42)
        sql, params = compile_query(q)

        assert "$1" in sql
        assert params == [42]

    def test_multiple_params(self):
        q = sa.select(users).where(users.c.id == 1).where(users.c.name == "Alice")
        sql, params = compile_query(q)

        assert "$1" in sql
        assert "$2" in sql
        assert len(params) == 2

    def test_no_params(self):
        q = sa.select(users)
        sql, params = compile_query(q)

        assert "SELECT" in sql.upper()
        assert params == []


class TestCreateQuery:
    def test_insert_returning(self):
        q = create(users, [{"name": "Alice", "age": 30}])
        sql, params = compile_query(q)

        assert "INSERT" in sql.upper()
        assert "RETURNING" in sql.upper()


class TestSearchQuery:
    def test_basic_select(self):
        q = search(users)
        sql, _ = compile_query(q)
        assert "SELECT" in sql.upper()

    def test_with_limit(self):
        q = search(users, limit=10)
        sql, params = compile_query(q)
        assert "LIMIT" in sql.upper()

    def test_with_offset(self):
        q = search(users, offset=5)
        sql, params = compile_query(q)
        assert "OFFSET" in sql.upper()

    def test_with_order_by_asc(self):
        q = search(users, order_by="name")
        sql, _ = compile_query(q)
        assert "ORDER BY" in sql.upper()

    def test_with_order_by_desc(self):
        q = search(users, order_by="-name")
        sql, _ = compile_query(q)
        assert "DESC" in sql.upper()

    def test_with_multiple_order_by(self):
        q = search(users, order_by=["name", "-age"])
        sql, _ = compile_query(q)
        assert "ORDER BY" in sql.upper()


class TestGetByIdQuery:
    def test_where_id(self):
        q = get_by_id(UserModel, 5)
        sql, params = compile_query(q)
        assert "WHERE" in sql.upper()
        assert 5 in params


class TestUpdateByIdQuery:
    def test_update_with_returning(self):
        q = update_by_id(UserModel, 1, name="Bob")
        sql, params = compile_query(q)
        assert "UPDATE" in sql.upper()
        assert "RETURNING" in sql.upper()
        assert "Bob" in params


class TestDeleteByIdQuery:
    def test_delete_with_returning(self):
        q = delete_by_id(UserModel, 1)
        sql, params = compile_query(q)
        assert "DELETE" in sql.upper()
        assert "RETURNING" in sql.upper()
        assert 1 in params


class TestDeleteQuery:
    def test_base_delete(self):
        q = delete(users)
        sql, _ = compile_query(q)
        assert "DELETE" in sql.upper()
        assert "RETURNING" in sql.upper()


class TestCountQuery:
    def test_count(self):
        q = count(users)
        sql, _ = compile_query(q)
        assert "COUNT" in sql.upper()

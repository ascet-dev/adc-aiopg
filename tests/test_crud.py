"""Integration tests for PGDataAccessObject CRUD, filters, and pagination."""
import pytest

from adc_aiopg.errors import RowNotFoundError
from adc_aiopg.types import Paginated



# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

class TestCreate:
    async def test_create_returns_model(self, dal):
        item = await dal.items.create(name="Alice")
        assert item.name == "Alice"
        assert item.id is not None

    async def test_create_many(self, dal):
        items = await dal.items.create_many([
            {"name": "A"},
            {"name": "B"},
            {"name": "C"},
        ])
        assert len(items) == 3
        names = {i.name for i in items}
        assert names == {"A", "B", "C"}

    async def test_create_many_empty(self, dal):
        items = await dal.items.create_many([])
        assert items == []


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

class TestGetById:
    async def test_get_by_id(self, dal):
        created = await dal.items.create(name="Find me")
        found = await dal.items.get_by_id(created.id)
        assert found.name == "Find me"

    async def test_get_by_id_not_found(self, dal):
        with pytest.raises(RowNotFoundError):
            await dal.items.get_by_id(999999)


class TestSearch:
    async def test_search_all(self, dal):
        await dal.items.create(name="A")
        await dal.items.create(name="B")

        items = await dal.items.search()
        assert len(items) == 2

    async def test_search_with_filter(self, dal):
        await dal.items.create(name="Active", active=True)
        await dal.items.create(name="Inactive", active=False)

        items = await dal.items.search(active=True)
        assert len(items) == 1
        assert items[0].name == "Active"

    async def test_search_with_limit(self, dal):
        for i in range(5):
            await dal.items.create(name=f"item_{i}")

        items = await dal.items.search(limit=3)
        assert len(items) == 3

    async def test_search_with_order_by(self, dal):
        await dal.items.create(name="B")
        await dal.items.create(name="A")
        await dal.items.create(name="C")

        items = await dal.items.search(order_by="name")
        names = [i.name for i in items]
        assert names == ["A", "B", "C"]

    async def test_search_with_order_by_desc(self, dal):
        await dal.items.create(name="B")
        await dal.items.create(name="A")

        items = await dal.items.search(order_by="-name")
        assert items[0].name == "B"
        assert items[1].name == "A"


class TestGetOrCreate:
    async def test_creates_when_not_exists(self, dal):
        item = await dal.items.get_or_create(name="New")
        assert item.name == "New"

    async def test_returns_existing(self, dal):
        await dal.items.create(name="Existing")

        item = await dal.items.get_or_create(name="Existing")
        assert item.name == "Existing"

        all_items = await dal.items.search(name="Existing")
        assert len(all_items) == 1


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class TestUpdate:
    async def test_update_by_id(self, dal):
        created = await dal.items.create(name="Old")
        updated = await dal.items.update_by_id(created.id, name="New")
        assert updated.name == "New"

    async def test_update_by_id_not_found(self, dal):
        with pytest.raises(RowNotFoundError):
            await dal.items.update_by_id(999999, name="Nope")

    async def test_update_with_filters(self, dal):
        await dal.items.create(name="A", active=True)
        await dal.items.create(name="B", active=True)
        await dal.items.create(name="C", active=False)

        updated = await dal.items.update({"active": False}, active=True)
        assert len(updated) == 2


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDelete:
    async def test_delete_by_id(self, dal):
        created = await dal.items.create(name="Delete me")
        deleted = await dal.items.delete_by_id(created.id)
        assert deleted.name == "Delete me"

        items = await dal.items.search()
        assert len(items) == 0

    async def test_delete_by_id_not_found(self, dal):
        with pytest.raises(RowNotFoundError):
            await dal.items.delete_by_id(999999)

    async def test_delete_with_filters(self, dal):
        await dal.items.create(name="Keep", active=True)
        await dal.items.create(name="Remove", active=False)

        deleted = await dal.items.delete(active=False)
        assert len(deleted) == 1

        remaining = await dal.items.search()
        assert len(remaining) == 1
        assert remaining[0].name == "Keep"


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

class TestArchive:
    async def test_archive_by_id(self, dal):
        created = await dal.items.create(name="Archive me")
        archived = await dal.items.archive_by_id(created.id)
        assert archived.archived is True

    async def test_archive_with_filters(self, dal):
        await dal.items.create(name="A", active=False)
        await dal.items.create(name="B", active=False)
        await dal.items.create(name="C", active=True)

        archived = await dal.items.archive(active=False)
        assert len(archived) == 2


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

class TestFilters:
    async def test_gt(self, dal):
        await dal.items.create(name="young", age=20)
        await dal.items.create(name="old", age=50)

        items = await dal.items.search(age_gt=30)
        assert len(items) == 1
        assert items[0].name == "old"

    async def test_ge(self, dal):
        await dal.items.create(name="a", age=30)
        await dal.items.create(name="b", age=30)
        await dal.items.create(name="c", age=20)

        items = await dal.items.search(age_ge=30)
        assert len(items) == 2

    async def test_lt(self, dal):
        await dal.items.create(name="a", age=10)
        await dal.items.create(name="b", age=50)

        items = await dal.items.search(age_lt=30)
        assert len(items) == 1

    async def test_le(self, dal):
        await dal.items.create(name="a", age=30)
        await dal.items.create(name="b", age=30)
        await dal.items.create(name="c", age=50)

        items = await dal.items.search(age_le=30)
        assert len(items) == 2

    async def test_ne(self, dal):
        await dal.items.create(name="keep", active=True)
        await dal.items.create(name="skip", active=False)

        items = await dal.items.search(active_ne=False)
        assert len(items) == 1
        assert items[0].name == "keep"

    async def test_in(self, dal):
        await dal.items.create(name="A")
        await dal.items.create(name="B")
        await dal.items.create(name="C")

        items = await dal.items.search(name_in=["A", "C"])
        assert len(items) == 2

    async def test_notin(self, dal):
        await dal.items.create(name="A")
        await dal.items.create(name="B")
        await dal.items.create(name="C")

        items = await dal.items.search(name_notin=["A", "C"])
        assert len(items) == 1
        assert items[0].name == "B"

    async def test_is_none(self, dal):
        await dal.items.create(name="with_email", email="a@b.com")
        await dal.items.create(name="no_email")

        items = await dal.items.search(email_is=None)
        assert len(items) == 1
        assert items[0].name == "no_email"

    async def test_isnot_none(self, dal):
        await dal.items.create(name="with_email", email="a@b.com")
        await dal.items.create(name="no_email")

        items = await dal.items.search(email_isnot=None)
        assert len(items) == 1
        assert items[0].name == "with_email"

    async def test_like(self, dal):
        await dal.items.create(name="Alice")
        await dal.items.create(name="Bob")

        items = await dal.items.search(name_like="Ali%")
        assert len(items) == 1

    async def test_ilike(self, dal):
        await dal.items.create(name="Alice")
        await dal.items.create(name="BOB")

        items = await dal.items.search(name_ilike="%bob%")
        assert len(items) == 1
        assert items[0].name == "BOB"


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class TestPagination:
    async def test_paginated_search(self, dal):
        for i in range(10):
            await dal.items.create(name=f"item_{i}")

        result = await dal.items.paginated_search(limit=3, offset=0)
        assert isinstance(result, Paginated)
        assert len(result.items) == 3
        assert result.pagination.total == 10
        assert result.pagination.limit == 3
        assert result.pagination.offset == 0

    async def test_paginated_search_with_filters(self, dal):
        for i in range(5):
            await dal.items.create(name=f"active_{i}", active=True)
        for i in range(3):
            await dal.items.create(name=f"inactive_{i}", active=False)

        result = await dal.items.paginated_search(limit=2, offset=0, active=True)
        assert len(result.items) == 2
        assert result.pagination.total == 5


# ---------------------------------------------------------------------------
# Custom DAO
# ---------------------------------------------------------------------------

class TestCustomDAO:
    async def test_custom_method(self, custom_dal):
        await custom_dal.items.create(name="Alice", email="alice@test.com")
        await custom_dal.items.create(name="Bob", email="bob@test.com")

        found = await custom_dal.items.find_by_name("Alice")
        assert found is not None
        assert found.email == "alice@test.com"

    async def test_custom_method_not_found(self, custom_dal):
        found = await custom_dal.items.find_by_name("Nobody")
        assert found is None

    async def test_standard_crud_still_works(self, custom_dal):
        created = await custom_dal.items.create(name="Standard")
        found = await custom_dal.items.get_by_id(created.id)
        assert found.name == "Standard"


# ---------------------------------------------------------------------------
# Multiple tables
# ---------------------------------------------------------------------------

class TestMultipleTables:
    async def test_different_tables(self, dal):
        item = await dal.items.create(name="Item")
        product = await dal.products.create(title="Product", price=100)

        assert item.name == "Item"
        assert product.title == "Product"
        assert product.price == 100

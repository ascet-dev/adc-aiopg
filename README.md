# adc-aiopg

Async-библиотека для PostgreSQL на базе `asyncpg`. Предоставляет пул соединений с бинарными кодеками, query builder поверх SQLAlchemy, generic DAO с фильтрами, систему версионирования таблиц и интеграцию с Alembic.

## Установка

```bash
pip install git+https://github.com/ascet-dev/adc-aiopg.git@main
```

## Быстрый старт

```python
import asyncio
from adc_aiopg import create_db_pool, PGPoolManager

async def main():
    pool = await create_db_pool("postgresql://user:pass@localhost/mydb")
    pm = PGPoolManager(pool)

    async with pm.transaction() as conn:
        rows = await pm.fetch(
            select(users).where(users.c.active == True)
        )

    await pool.close()

asyncio.run(main())
```

## API

### create_db_pool

Создает `asyncpg.Pool` с предустановленными бинарными кодеками для `jsonb`, `timestamp`, `timestamptz` (через `ujson`).

```python
from adc_aiopg import create_db_pool

pool = await create_db_pool(
    dsn="postgresql://user:pass@localhost/mydb",
    min_size=5,
    max_size=20,
)
```

### compile_query

Компилирует SQLAlchemy-выражение в строку с позиционными параметрами `$1, $2, ...` для asyncpg.

```python
from adc_aiopg import compile_query
from sqlalchemy import select

sql, params = compile_query(select(users).where(users.c.id == 42))
# sql: "SELECT ... WHERE users.id = $1"
# params: [42]
```

### PGPoolManager

Обертка над пулом. Принимает SQLAlchemy-выражения, компилирует и выполняет. Результаты возвращаются как `dict`.

```python
from adc_aiopg import PGPoolManager

pm = PGPoolManager(pool)

# Получить все строки
rows = await pm.fetch(select(users))

# Одна строка
row = await pm.fetchrow(select(users).where(users.c.id == 1))

# Скалярное значение
count = await pm.fetchval(select(func.count()).select_from(users))

# Контекстные менеджеры
async with pm.connection() as conn:
    ...

async with pm.transaction() as conn:
    ...
```

### PGDataAccessObject[T]

Generic DAO поверх SQLModel. Полный CRUD с типизированными результатами. Хранит конфигурацию таблицы: `model`, `metadata`, `table_name`, `table_model`.

```python
from sqlmodel import Field
from adc_aiopg import PGDataAccessObject
from adc_aiopg.types import Base

class User(Base):
    id: int = Field(primary_key=True)
    name: str
    email: str
    active: bool = True

# Быстрое создание DAO из модели
UserDAO = PGDataAccessObject.from_model(User, "users", metadata=meta)
user_dao = UserDAO(db_pool=pool)

# CRUD
user = await user_dao.create(name="Alice", email="alice@example.com")
user = await user_dao.get_by_id(1)
users = await user_dao.search(active=True)
user = await user_dao.update_by_id(1, name="Alice Smith")
await user_dao.delete_by_id(1)

# Soft delete (ставит archived=True)
await user_dao.archive_by_id(1)
```

#### Кастомный DAO

Наследуйтесь от `PGDataAccessObject` и добавляйте свои методы:

```python
class UsersDAO(PGDataAccessObject[User], table_name="users"):
    model = User

    async def find_by_email(self, email: str) -> User | None:
        results = await self.search(email=email)
        return results[0] if results else None
```

Если модель уже является таблицей (`table=True`), `bind()` использует её как есть:

```python
class User(Base, table=True):
    __tablename__ = "users"
    id: int = Field(primary_key=True)
    name: str

class UsersDAO(PGDataAccessObject[User]):
    model = User  # уже selectable — bind() не создаёт обёртку
```

#### Фильтры

DAO поддерживает суффиксные фильтры в `search()` и других методах:

```python
# Сравнение
users = await dao.search(age_gt=18)          # age > 18
users = await dao.search(age_ge=18)          # age >= 18
users = await dao.search(age_lt=65)          # age < 65
users = await dao.search(age_le=65)          # age <= 65
users = await dao.search(status_ne="banned") # status != 'banned'

# Списки
users = await dao.search(role_in=["admin", "moderator"])
users = await dao.search(role_notin=["banned"])

# NULL
users = await dao.search(deleted_at_is=None)
users = await dao.search(deleted_at_isnot=None)

# LIKE / ILIKE
users = await dao.search(name_ilike="%alice%")
```

#### Пагинация

```python
from adc_aiopg.types import Paginated

result: Paginated[User] = await dao.paginated_search(
    limit=20, offset=0, active=True
)
# result.items: list[User]
# result.pagination: Pagination(total=150, limit=20, offset=0)
```

### PostgresAccessLayer + TableDescriptor

Декларативный слой доступа к БД. Группирует несколько DAO в одном объекте. Передаёт `metadata` всем DAO и вызывает `bind()` при определении класса (для миграций).

```python
from adc_aiopg import PostgresAccessLayer, PGDataAccessObject, TableDescriptor
from sqlalchemy import MetaData

meta = MetaData()

class User(Base):
    id: int = Field(primary_key=True)
    name: str

class Post(Base):
    id: int = Field(primary_key=True)
    title: str
    author_id: int

# Простые DAO из моделей
class DB(PostgresAccessLayer, metadata=meta):
    users = TableDescriptor(PGDataAccessObject.from_model(User, "users"))
    posts = TableDescriptor(PGDataAccessObject.from_model(Post, "posts"))

# Кастомный DAO с дополнительными методами
class UsersDAO(PGDataAccessObject[User], table_name="users"):
    model = User

    async def find_by_email(self, email: str) -> User | None:
        results = await self.search(email=email)
        return results[0] if results else None

class DB(PostgresAccessLayer, metadata=meta):
    users: UsersDAO = TableDescriptor(UsersDAO)
    posts = TableDescriptor(PGDataAccessObject.from_model(Post, "posts"))

# Использование
db = DB(pool)
user = await db.users.find_by_email("alice@example.com")
posts = await db.posts.search(author_id=1)

# Произвольные запросы через PGPoolManager
count = await db.pm.fetchval(select(func.count()).select_from(users))
```

`TableDescriptor` — минимальный дескриптор, принимает DAO-класс. Если `table_name` не задан на DAO, используется имя атрибута как fallback.

### Версионирование таблиц

Создает shadow-таблицу `{table}_log` и PostgreSQL-триггер для автоматического логирования изменений.

```python
from adc_aiopg import declare_version_table

VersionedUser = declare_version_table(User)
# Создается таблица users_log с теми же колонками
# Триггер автоматически пишет в _log при INSERT/UPDATE/DELETE
```

### Alembic-интеграция

В `env.py` вашего Alembic-проекта:

```python
from adc_aiopg.alembic_env import run_alembic
from myapp.models import Base

run_alembic(
    sqlalchemy_url="postgresql://user:pass@localhost/mydb",
    target_metadata=Base.metadata,
)
```

Автоматически:
- Нумерует миграции инкрементально (0001, 0002, ...)
- Добавляет CREATE/DROP TRIGGER для версионированных таблиц

## Типы

### Base

Базовый класс для моделей (наследует SQLModel):

```python
from adc_aiopg.types import Base

class User(Base, table=True):
    __tablename__ = "users"
    id: int = Field(primary_key=True)
    name: str

# Проекции
UserPartial = User.partial()       # все поля Optional
UserNames = User.only("id", "name")  # только указанные поля
UserNoEmail = User.exclude("email")  # все кроме указанных
```

### sqla_enum

Хелпер для enum-полей с поддержкой SQLAlchemy:

```python
from adc_aiopg.enum import sqla_enum

class Role(str, Enum):
    admin = "admin"
    user = "user"

class User(Base, table=True):
    role: Role = sqla_enum(Role)
```

## Требования

- Python >= 3.8
- asyncpg >= 0.27.0
- sqlalchemy >= 2.0.0
- sqlmodel >= 0.0.8
- pydantic >= 2.0.0
- alembic >= 1.11.0
- ujson >= 5.10.0

## Лицензия

MIT

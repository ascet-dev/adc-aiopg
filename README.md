# aiopg

Асинхронная библиотека для работы с PostgreSQL, предоставляющая удобный интерфейс для управления подключениями, выполнения запросов и работы с репозиториями.

## Возможности

- **Пул соединений**: Эффективное управление подключениями к PostgreSQL
- **Асинхронные операции**: Полная поддержка async/await для всех операций с базой данных
- **JSON поддержка**: Встроенная поддержка JSONB с оптимизированными кодерами
- **Временные метки**: Автоматическая обработка timestamp и timestamptz
- **Репозитории**: Готовые абстракции для работы с данными
- **Система версионирования**: Поддержка версионирования таблиц
- **Обработка ошибок**: Специализированные исключения для различных сценариев

## Установка

```bash
pip install aiopg
```

## Быстрый старт

### Создание пула соединений

```python
from aiopg import create_db_pool

# Создание пула соединений
pool = create_db_pool(
    dsn="postgresql://user:password@localhost/dbname",
    min_size=5,
    max_size=20
)
```

### Выполнение запросов

```python
from aiopg import compile_query

# Компиляция и выполнение запроса
query = compile_query("SELECT * FROM users WHERE id = $1")
async with pool.acquire() as conn:
    result = await conn.fetch(query, user_id)
```

### Работа с репозиториями

```python
from aiopg.repository import PGDataAccessObject, TableDescriptor

# Определение таблицы
users_table = TableDescriptor("users", ["id", "name", "email"])

# Создание DAO
users_dao = PGDataAccessObject(pool, users_table)

# Операции с данными
user = await users_dao.get_by_id(user_id)
users = await users_dao.get_all()
await users_dao.insert({"name": "John", "email": "john@example.com"})
```

## Основные компоненты

### Connection Pool
- `create_db_pool()` - создание пула соединений с автоматической инициализацией кодеков

### Query Compilation
- `compile_query()` - компиляция SQL запросов для оптимизации

### Repository Pattern
- `PGDataAccessObject` - базовый DAO для работы с таблицами
- `PostgresAccessLayer` - слой доступа к PostgreSQL
- `PGPoolManager` - менеджер пулов соединений

### Version Management
- `declare_version_table()` - создание таблиц для версионирования

## Требования

- Python >= 3.8
- PostgreSQL
- asyncpg >= 0.27.0
- psycopg2-binary >= 2.9.0

## Лицензия

MIT License - см. файл [LICENSE](LICENSE) для подробностей.

## Разработка

```bash
# Клонирование репозитория
git clone https://github.com/ascet-dev/adc-aiopg.git
cd adc-aiopg

# Установка зависимостей для разработки
pip install -e .
``` 
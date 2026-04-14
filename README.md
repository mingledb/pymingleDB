# pymingledb

Lightweight file-based NoSQL engine — Python port of [mingleDB](https://github.com/mingledb/mingleDB). Same format as the JavaScript and [gomingleDB](https://github.com/mingledb/gomingleDB) implementations: BSON serialization, zlib compression, optional schema validation, query operators, and basic authentication.

## Install (uv)

```bash
cd pymingledb
uv sync
```

Or add to your project:

```bash
uv add pymingledb
```

## Usage

```python
from pymingledb import MingleDB, ValidationError, UsernameExistsError, AuthFailedError

db = MingleDB("./mydb")  # directory -> ./mydb/database.mgdb
# db = MingleDB("./mydb/app.mgdb")  # explicit single-file path

# Schema (optional)
db.define_schema("users", {
    "name": {"type": "string", "required": True},
    "email": {"type": "string", "required": True, "unique": True},
    "age": {"type": "number"},
})

# CRUD
db.insert_one("users", {"name": "Alice", "email": "alice@example.com", "age": 30})
db.insert_one("users", {"name": "Bob", "email": "bob@example.com", "age": 17})

db.find_all("users")
db.find_one("users", {"email": "alice@example.com"})
db.find("users", {"age": {"$gte": 18, "$lt": 60}})
db.find("users", {"name": {"$regex": "ali", "$options": "i"}})
db.find("users", {"email": {"$in": ["alice@example.com", "bob@example.com"]}})

db.update_one("users", {"name": "Alice"}, {"age": 31})
db.delete_one("users", {"email": "bob@example.com"})

# Auth (uses internal _auth collection)
db.register_user("admin", "secure123")
db.login("admin", "secure123")
db.is_authenticated("admin")  # True
db.logout("admin")

# Reset (wipe database file and schemas)
db.reset()
```

## Query operators

- `$gt`, `$gte`, `$lt`, `$lte` — numeric comparison
- `$eq`, `$ne` — equality
- `$in`, `$nin` — in list / not in list
- `$regex`, `$options` — regex (e.g. `"i"` for case-insensitive)

You can also pass a compiled `re.Pattern` as a filter value for regex match.

## Exceptions

- `MingleDBError` — base
- `UsernameExistsError` — register with existing username
- `AuthFailedError` — login failed
- `ValidationError` — schema validation (required, type, unique)

## File format

All collections are stored in one `.mgdb` database file, compatible with mingleDB (JS) and gomingleDB (Go). Internal file layout details are intentionally abstracted from user-facing docs.

## Tests

```bash
uv sync --extra dev
uv run pytest tests/ -v
```

## License

Use under the same terms as mingleDB / gomingleDB.

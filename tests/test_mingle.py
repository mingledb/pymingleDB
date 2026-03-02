import re
import tempfile
from pathlib import Path

import pytest

from pymingledb import (
    MingleDB,
    MingleDBError,
    AuthFailedError,
    UsernameExistsError,
    ValidationError,
)


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield MingleDB(d)


def test_register_login_logout(tmp_db):
    tmp_db.register_user("admin", "secure123")
    tmp_db.login("admin", "secure123")
    assert tmp_db.is_authenticated("admin") is True
    tmp_db.logout("admin")
    assert tmp_db.is_authenticated("admin") is False


def test_define_schema_and_insert(tmp_db):
    tmp_db.define_schema("users", {
        "name": {"type": "string", "required": True},
        "email": {"type": "string", "required": True, "unique": True},
        "age": {"type": "number"},
    })
    tmp_db.insert_one("users", {"name": "Cloud", "email": "cloud@seed.com", "age": 25})
    tmp_db.insert_one("users", {"name": "Alice", "email": "alice@example.com", "age": 30})
    tmp_db.insert_one("users", {"name": "Bob", "email": "bob@example.com", "age": 17})
    all_docs = tmp_db.find_all("users")
    assert len(all_docs) == 3


def test_reject_duplicate_or_missing_required(tmp_db):
    tmp_db.define_schema("users", {
        "name": {"type": "string", "required": True},
        "email": {"type": "string", "required": True, "unique": True},
    })
    tmp_db.insert_one("users", {"name": "A", "email": "a@a.com"})
    with pytest.raises(ValidationError, match="unique"):
        tmp_db.insert_one("users", {"name": "B", "email": "a@a.com"})
    with pytest.raises(ValidationError, match="required"):
        tmp_db.insert_one("users", {"email": "missingname@x.com"})


def test_find_all_find_one_regex_range_in(tmp_db):
    tmp_db.define_schema("users", {
        "name": {"type": "string", "required": True},
        "email": {"type": "string", "required": True, "unique": True},
        "age": {"type": "number"},
    })
    tmp_db.insert_one("users", {"name": "Cloud", "email": "cloud@seed.com", "age": 25})
    tmp_db.insert_one("users", {"name": "Alice", "email": "alice@example.com", "age": 30})
    tmp_db.insert_one("users", {"name": "Bob", "email": "bob@example.com", "age": 17})

    assert len(tmp_db.find_all("users")) == 3

    alice = tmp_db.find_one("users", {"email": "alice@example.com"})
    assert alice is not None and alice["name"] == "Alice"

    regex_match = tmp_db.find("users", {"name": re.compile(r"clo", re.I)})
    assert len(regex_match) == 1 and regex_match[0]["name"] == "Cloud"

    age_range = tmp_db.find("users", {"age": {"$gte": 18, "$lt": 60}})
    names = {u["name"] for u in age_range}
    assert names == {"Cloud", "Alice"}

    email_in = tmp_db.find("users", {"email": {"$in": ["cloud@seed.com", "a@b.com"]}})
    assert len(email_in) == 1 and email_in[0]["email"] == "cloud@seed.com"


def test_update_one(tmp_db):
    tmp_db.define_schema("users", {
        "name": {"type": "string", "required": True},
        "email": {"type": "string", "required": True, "unique": True},
        "age": {"type": "number"},
    })
    tmp_db.insert_one("users", {"name": "Alice", "email": "alice@example.com", "age": 30})
    updated = tmp_db.update_one("users", {"name": "Alice"}, {"age": 31})
    assert updated is True
    check = tmp_db.find_one("users", {"name": "Alice"})
    assert check is not None and check["age"] == 31


def test_delete_one(tmp_db):
    tmp_db.define_schema("users", {
        "name": {"type": "string", "required": True},
        "email": {"type": "string", "required": True, "unique": True},
    })
    tmp_db.insert_one("users", {"name": "Alice", "email": "alice@example.com"})
    deleted = tmp_db.delete_one("users", {"email": "alice@example.com"})
    assert deleted is True
    assert len(tmp_db.find_all("users")) == 0


def test_reset(tmp_db):
    tmp_db.insert_one("users", {"name": "X", "email": "x@x.com"})
    tmp_db.reset()
    assert len(tmp_db.find_all("users")) == 0


def test_login_fails_wrong_password(tmp_db):
    tmp_db.register_user("admin", "secure123")
    with pytest.raises(AuthFailedError):
        tmp_db.login("admin", "wrong")


def test_register_duplicate_username(tmp_db):
    tmp_db.register_user("admin", "secure123")
    with pytest.raises(UsernameExistsError):
        tmp_db.register_user("admin", "other")

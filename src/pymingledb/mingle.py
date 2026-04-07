# pymingleDB - Lightweight file-based NoSQL engine (Python port of mingleDB).
# BSON serialization, zlib compression, schema validation, query operators, basic auth.

from __future__ import annotations

import hashlib
import json
import re
import struct
import threading
import zlib
from pathlib import Path
from typing import Any, Pattern

from bson import BSON


HEADER = b"MINGLEDBv1"
EXTENSION = ".mgdb"
AUTH_COLLECTION = "_auth"


class MingleDBError(Exception):
    """Base exception for pymingleDB."""


class UsernameExistsError(MingleDBError):
    """Username already exists when registering."""


class AuthFailedError(MingleDBError):
    """Authentication failed (wrong username or password)."""


class ValidationError(MingleDBError):
    """Schema validation error (required, type, unique)."""


def _float_cmp(a: Any, b: Any) -> tuple[float, float] | None:
    """Return (a, b) as floats if both are numeric, else None."""
    try:
        fa = float(a) if isinstance(a, (int, float)) else None
        fb = float(b) if isinstance(b, (int, float)) else None
        if fa is not None and fb is not None:
            return (fa, fb)
    except (TypeError, ValueError):
        pass
    return None


def _value_equal(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    pair = _float_cmp(a, b)
    if pair is not None:
        return pair[0] == pair[1]
    return a == b


def _match_operators(doc_val: Any, op_map: dict[str, Any]) -> bool:
    for op, op_val in op_map.items():
        if op == "$options":
            continue
        if op == "$gt":
            pair = _float_cmp(doc_val, op_val)
            if pair is None or not (pair[0] > pair[1]):
                return False
        elif op == "$gte":
            pair = _float_cmp(doc_val, op_val)
            if pair is None or not (pair[0] >= pair[1]):
                return False
        elif op == "$lt":
            pair = _float_cmp(doc_val, op_val)
            if pair is None or not (pair[0] < pair[1]):
                return False
        elif op == "$lte":
            pair = _float_cmp(doc_val, op_val)
            if pair is None or not (pair[0] <= pair[1]):
                return False
        elif op == "$eq":
            if not _value_equal(doc_val, op_val):
                return False
        elif op == "$ne":
            if _value_equal(doc_val, op_val):
                return False
        elif op == "$in":
            if not isinstance(op_val, (list, tuple)):
                return False
            if not any(_value_equal(doc_val, v) for v in op_val):
                return False
        elif op == "$nin":
            if isinstance(op_val, (list, tuple)) and any(_value_equal(doc_val, v) for v in op_val):
                return False
        elif op == "$regex":
            pattern = op_val if isinstance(op_val, str) else ""
            flags = 0
            if isinstance(op_map.get("$options"), str) and "i" in op_map["$options"]:
                flags = re.IGNORECASE
            try:
                rx = re.compile(pattern, flags)
            except re.error:
                return False
            if not isinstance(doc_val, str) or not rx.search(doc_val):
                return False
    return True


def _match_query(doc: dict[str, Any], filter: dict[str, Any]) -> bool:
    for key, filter_val in filter.items():
        doc_val = doc.get(key)
        if filter_val is None:
            if doc_val is not None:
                return False
            continue
        if isinstance(filter_val, dict) and any(
            k.startswith("$") for k in filter_val
        ):
            if not _match_operators(doc_val, filter_val):
                return False
            continue
        if isinstance(filter_val, Pattern):
            if not isinstance(doc_val, str) or not filter_val.search(doc_val):
                return False
            continue
        if not _value_equal(doc_val, filter_val):
            return False
    return True


class MingleDB:
    """
    Lightweight file-based NoSQL database.
    Uses BSON + zlib, optional schema validation, query operators ($gt, $gte, $in, $regex, etc.),
    and basic authentication (register_user / login / logout).
    """

    def __init__(self, db_dir: str | Path = ".mgdb") -> None:
        self._db_dir = Path(db_dir)
        self._db_dir.mkdir(parents=True, exist_ok=True)
        self._schemas: dict[str, dict[str, Any]] = {}
        self._sessions: set[str] = set()
        self._lock = threading.RLock()

    def reset(self) -> None:
        """Remove all .mgdb collection files and clear schemas and auth state."""
        with self._lock:
            if self._db_dir.exists():
                for f in self._db_dir.iterdir():
                    if f.is_file() and f.suffix == EXTENSION:
                        f.unlink()
            self._schemas.clear()
            self._sessions.clear()

    def _get_file_path(self, collection: str) -> Path:
        return self._db_dir / f"{collection}{EXTENSION}"

    def _init_collection_file(self, collection: str) -> None:
        path = self._get_file_path(collection)
        if path.exists():
            return
        meta = json.dumps({"collection": collection}).encode("utf-8")
        meta_len = struct.pack("<I", len(meta))
        path.write_bytes(HEADER + meta_len + meta)

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def define_schema(self, collection: str, schema_definition: dict[str, Any]) -> None:
        """Define schema for a collection. Rules: type ('string'|'number'), required, unique."""
        with self._lock:
            self._schemas[collection] = schema_definition

    def _validate_schema(self, collection: str, doc: dict[str, Any]) -> None:
        schema = self._schemas.get(collection)
        if not schema:
            return
        all_docs = self._find_all_locked(collection)
        for key, rule in schema.items():
            if not isinstance(rule, dict):
                continue
            typ = rule.get("type")
            required = rule.get("required", False)
            unique = rule.get("unique", False)
            value = doc.get(key)
            if required and (value is None and key not in doc):
                raise ValidationError(f'Field "{key}" is required.')
            if value is not None and key in doc:
                if typ == "string" and not isinstance(value, str):
                    raise ValidationError(f'Field "{key}" must be of type string.')
                if typ == "number" and not isinstance(value, (int, float)):
                    raise ValidationError(f'Field "{key}" must be of type number.')
                if unique:
                    for d in all_docs:
                        if d.get(key) == value:
                            raise ValidationError(
                                f'Duplicate value for unique field "{key}".'
                            )

    def register_user(self, username: str, password: str) -> None:
        """Register a user in _auth. Raises UsernameExistsError if username exists."""
        with self._lock:
            self._init_collection_file(AUTH_COLLECTION)
            users = self._find_all_locked(AUTH_COLLECTION)
            for u in users:
                if u.get("username") == username:
                    raise UsernameExistsError("Username already exists.")
            hashed = self._hash_password(password)
            self._insert_one_locked(
                AUTH_COLLECTION, {"username": username, "password": hashed}
            )

    def login(self, username: str, password: str) -> None:
        """Authenticate user; adds to session. Raises AuthFailedError on failure."""
        with self._lock:
            users = self._find_all_locked(AUTH_COLLECTION)
            user = None
            for u in users:
                if u.get("username") == username:
                    user = u
                    break
            if user is None or user.get("password") != self._hash_password(password):
                raise AuthFailedError("Authentication failed.")
            self._sessions.add(username)

    def is_authenticated(self, username: str) -> bool:
        return username in self._sessions

    def logout(self, username: str) -> None:
        self._sessions.discard(username)

    def _insert_one_locked(self, collection: str, doc: dict[str, Any]) -> None:
        raw = BSON.encode(doc)
        compressed = zlib.compress(raw)
        length = struct.pack("<I", len(compressed))
        path = self._get_file_path(collection)
        with open(path, "ab") as f:
            f.write(length + compressed)

    def insert_one(self, collection: str, doc: dict[str, Any]) -> None:
        """Append one document. Validates schema if defined."""
        with self._lock:
            self._init_collection_file(collection)
            self._validate_schema(collection, doc)
            self._insert_one_locked(collection, doc)

    def _find_all_locked(self, collection: str) -> list[dict[str, Any]]:
        path = self._get_file_path(collection)
        if not path.exists():
            return []
        data = path.read_bytes()
        if len(data) < len(HEADER) + 4:
            return []
        if data[: len(HEADER)] != HEADER:
            raise MingleDBError("Invalid mingleDB file header.")
        offset = len(HEADER)
        (meta_len,) = struct.unpack("<I", data[offset : offset + 4])
        offset += 4 + meta_len
        docs = []
        while offset + 4 <= len(data):
            (doc_len,) = struct.unpack("<I", data[offset : offset + 4])
            offset += 4
            if offset + doc_len > len(data):
                break
            compressed = data[offset : offset + doc_len]
            offset += doc_len
            raw = zlib.decompress(compressed)
            doc = BSON.decode(raw)
            docs.append(doc)
        return docs

    def find_all(self, collection: str) -> list[dict[str, Any]]:
        """Return all documents in the collection."""
        with self._lock:
            return self._find_all_locked(collection)

    def find(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return documents matching the filter. Empty filter matches all."""
        filter = filter or {}
        with self._lock:
            docs = self._find_all_locked(collection)
            return [d for d in docs if _match_query(d, filter)]

    def find_one(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Return the first document matching the filter, or None."""
        found = self.find(collection, filter or {})
        return found[0] if found else None

    def _rewrite_collection_locked(
        self, collection: str, docs: list[dict[str, Any]]
    ) -> None:
        meta = json.dumps({"collection": collection}).encode("utf-8")
        meta_len = struct.pack("<I", len(meta))
        body = bytearray()
        for doc in docs:
            raw = BSON.encode(doc)
            compressed = zlib.compress(raw)
            body.extend(struct.pack("<I", len(compressed)))
            body.extend(compressed)
        path = self._get_file_path(collection)
        path.write_bytes(HEADER + meta_len + meta + bytes(body))

    def update_one(
        self,
        collection: str,
        query: dict[str, Any],
        update: dict[str, Any],
    ) -> bool:
        """Update the first document matching query with update. Returns True if one was updated."""
        with self._lock:
            docs = self._find_all_locked(collection)
            updated = False
            for i, doc in enumerate(docs):
                if not updated and _match_query(doc, query):
                    updated = True
                    docs[i] = {**doc, **update}
            if updated:
                self._rewrite_collection_locked(collection, docs)
            return updated

    def delete_one(self, collection: str, query: dict[str, Any]) -> bool:
        """Remove the first document matching query. Returns True if one was deleted."""
        with self._lock:
            docs = self._find_all_locked(collection)
            out = []
            deleted = False
            for doc in docs:
                if not deleted and _match_query(doc, query):
                    deleted = True
                    continue
                out.append(doc)
            if deleted:
                self._rewrite_collection_locked(collection, out)
            return deleted

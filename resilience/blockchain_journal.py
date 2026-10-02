"""Crash-recoverable append-only storage for the blockchain reconciliation lab."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from resilience.blockchain import Block, ChainReconciler, Output, Reconciliation, Transaction

JOURNAL_SCHEMA = 1
ZERO_EVENT_HASH = "0" * 64


class JournalCorruption(ValueError):
    """Raised when durable blockchain evidence cannot be trusted."""


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _event_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _strict_json(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise JournalCorruption(f"duplicate JSON field: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicates)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise JournalCorruption("journal record is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise JournalCorruption("journal record must be an object")
    return value


def _block_dict(block: Block) -> dict[str, Any]:
    return {
        "block_hash": block.block_hash,
        "parent_hash": block.parent_hash,
        "height": block.height,
        "work": block.work,
        "transactions": [transaction.as_dict() for transaction in block.transactions],
    }


def _parse_block(value: Any) -> Block:
    if not isinstance(value, dict) or set(value) != {
        "block_hash",
        "parent_hash",
        "height",
        "work",
        "transactions",
    }:
        raise JournalCorruption("journal block has an invalid schema")
    transactions = value["transactions"]
    if not isinstance(transactions, list):
        raise JournalCorruption("journal transactions must be a list")
    parsed_transactions: list[Transaction] = []
    for transaction in transactions:
        if not isinstance(transaction, dict) or set(transaction) != {"txid", "inputs", "outputs"}:
            raise JournalCorruption("journal transaction has an invalid schema")
        inputs = transaction["inputs"]
        outputs = transaction["outputs"]
        if not isinstance(inputs, list) or not isinstance(outputs, list):
            raise JournalCorruption("journal transaction I/O must be lists")
        parsed_outputs: list[Output] = []
        for output in outputs:
            if not isinstance(output, dict) or set(output) != {"owner", "amount"}:
                raise JournalCorruption("journal output has an invalid schema")
            parsed_outputs.append(Output(owner=output["owner"], amount=output["amount"]))
        parsed_transactions.append(
            Transaction(
                txid=transaction["txid"],
                inputs=tuple(inputs),
                outputs=tuple(parsed_outputs),
            )
        )
    return Block(
        block_hash=value["block_hash"],
        parent_hash=value["parent_hash"],
        height=value["height"],
        work=value["work"],
        transactions=tuple(parsed_transactions),
    )


class JournaledChain:
    """Persist admitted blocks before exposing their in-memory effects.

    A complete newline-terminated event is the commit boundary. Recovery ignores
    one torn trailing fragment, but rejects corruption in every committed record.
    """

    def __init__(
        self,
        journal_path: str | Path,
        genesis_utxos: Mapping[str, Output],
        *,
        confirmation_depth: int = 6,
        max_known_blocks: int = 10_000,
        max_transactions_per_block: int = 1_000,
        max_io_per_transaction: int = 100,
        max_record_bytes: int = 1_048_576,
    ) -> None:
        if type(max_record_bytes) is not int or max_record_bytes < 256:
            raise ValueError("max_record_bytes must be an integer of at least 256")
        self._path = Path(journal_path)
        if self._path.is_symlink():
            raise ValueError("journal path must not be a symbolic link")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._genesis = dict(genesis_utxos)
        self._chain_options = {
            "confirmation_depth": confirmation_depth,
            "max_known_blocks": max_known_blocks,
            "max_transactions_per_block": max_transactions_per_block,
            "max_io_per_transaction": max_io_per_transaction,
        }
        self._max_record_bytes = max_record_bytes
        self._records = self._read_records()
        self._chain = self._replay(self._records)

    @property
    def chain(self) -> ChainReconciler:
        return self._chain

    @property
    def event_count(self) -> int:
        return len(self._records)

    def add_block(self, block: Block) -> Reconciliation:
        candidate = self._replay(self._records)
        preview = candidate.add_block(block)
        if preview.decision == "duplicate":
            return self._chain.add_block(block)

        previous_hash = self._records[-1]["event_hash"] if self._records else ZERO_EVENT_HASH
        payload = {
            "schema_version": JOURNAL_SCHEMA,
            "sequence": len(self._records) + 1,
            "previous_hash": previous_hash,
            "block": _block_dict(block),
        }
        record = {**payload, "event_hash": _event_hash(payload)}
        encoded = _canonical(record) + b"\n"
        if len(encoded) > self._max_record_bytes:
            raise ValueError("journal record exceeds max_record_bytes")
        self._append_durable(encoded)
        self._records.append(record)
        return self._chain.add_block(block)

    def snapshot(self) -> dict[str, Any]:
        return {
            **self._chain.snapshot(),
            "journal_events": self.event_count,
            "journal_head": self._records[-1]["event_hash"] if self._records else ZERO_EVENT_HASH,
        }

    def _new_chain(self) -> ChainReconciler:
        return ChainReconciler(self._genesis, **self._chain_options)

    def _replay(self, records: list[dict[str, Any]]) -> ChainReconciler:
        chain = self._new_chain()
        for record in records:
            chain.add_block(_parse_block(record["block"]))
        return chain

    def _read_records(self) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        raw = self._path.read_bytes()
        parts = raw.split(b"\n")
        torn_tail = bool(parts[-1])
        if parts[-1]:
            parts.pop()  # A crash may leave one uncommitted trailing fragment.
        else:
            parts.pop()
        records: list[dict[str, Any]] = []
        previous_hash = ZERO_EVENT_HASH
        for index, line in enumerate(parts, start=1):
            if not line or len(line) + 1 > self._max_record_bytes:
                raise JournalCorruption(f"journal record {index} is blank or oversized")
            record = _strict_json(line)
            if set(record) != {
                "schema_version",
                "sequence",
                "previous_hash",
                "block",
                "event_hash",
            }:
                raise JournalCorruption(f"journal record {index} has an invalid schema")
            payload = {key: value for key, value in record.items() if key != "event_hash"}
            if record["schema_version"] != JOURNAL_SCHEMA or record["sequence"] != index:
                raise JournalCorruption(f"journal record {index} has invalid version or sequence")
            if record["previous_hash"] != previous_hash:
                raise JournalCorruption(f"journal record {index} breaks the hash chain")
            if record["event_hash"] != _event_hash(payload):
                raise JournalCorruption(f"journal record {index} digest mismatch")
            _parse_block(record["block"])
            records.append(record)
            previous_hash = record["event_hash"]
        if torn_tail:
            self._truncate_to_last_commit(raw.rfind(b"\n") + 1)
        return records

    def _truncate_to_last_commit(self, size: int) -> None:
        flags = os.O_WRONLY
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self._path, flags)
        try:
            os.ftruncate(descriptor, size)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _append_durable(self, encoded: bytes) -> None:
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self._path, flags, 0o600)
        try:
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                raise OSError("short append left an uncommitted journal tail")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

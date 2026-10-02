from __future__ import annotations

import json
from pathlib import Path

import pytest

from resilience.blockchain import GENESIS_HASH, Block, Output, Transaction
from resilience.blockchain_journal import JournalCorruption, JournaledChain


def journal(path: Path) -> JournaledChain:
    return JournaledChain(path, {"funding:0": Output("alice", 100)}, confirmation_depth=3)


def spend(outpoint: str, owner: str, amount: int = 100) -> Transaction:
    return Transaction.build([outpoint], [Output(owner, amount)])


def block(parent: Block | None, *transactions: Transaction, work: int = 1) -> Block:
    return Block.build(
        parent_hash=GENESIS_HASH if parent is None else parent.block_hash,
        height=1 if parent is None else parent.height + 1,
        work=work,
        transactions=transactions,
    )


def test_restart_replays_canonical_state(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    first = block(None, spend("funding:0", "bob"))
    live.add_block(first)
    second = block(first)
    live.add_block(second)

    recovered = journal(path)

    assert recovered.chain.tip_hash == second.block_hash
    assert recovered.chain.balance("bob") == 100
    assert recovered.snapshot() == live.snapshot()


def test_restart_replays_heavier_branch_reorganization(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    a1 = block(None, spend("funding:0", "bob"))
    live.add_block(a1)
    b1 = block(None, spend("funding:0", "carol"))
    live.add_block(b1)
    b2 = block(b1, work=2)
    assert live.add_block(b2).decision == "reorg"

    recovered = journal(path)

    assert recovered.chain.tip_hash == b2.block_hash
    assert recovered.chain.balance("bob") == 0
    assert recovered.chain.balance("carol") == 100


def test_torn_trailing_record_is_ignored(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    first = block(None, spend("funding:0", "bob"))
    live.add_block(first)
    with path.open("ab") as stream:
        stream.write(b'{"schema_version":1,"sequence":2')

    recovered = journal(path)

    assert recovered.event_count == 1
    assert recovered.chain.tip_hash == first.block_hash


def test_recovery_truncates_torn_tail_before_next_append(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    first = block(None, spend("funding:0", "bob"))
    live.add_block(first)
    with path.open("ab") as stream:
        stream.write(b'{"schema_version":1')

    recovered = journal(path)
    second = block(first)
    recovered.add_block(second)
    restarted = journal(path)

    assert restarted.event_count == 2
    assert restarted.chain.tip_hash == second.block_hash


def test_tampered_committed_record_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    live.add_block(block(None, spend("funding:0", "bob")))
    path.write_text(path.read_text().replace('"bob"', '"eve"'), encoding="utf-8")

    with pytest.raises(JournalCorruption, match="digest mismatch"):
        journal(path)


def test_deleted_middle_record_breaks_sequence(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    first = block(None, spend("funding:0", "bob"))
    live.add_block(first)
    live.add_block(block(first))
    lines = path.read_text().splitlines()
    path.write_text(lines[1] + "\n", encoding="utf-8")

    with pytest.raises(JournalCorruption, match="version or sequence"):
        journal(path)


def test_duplicate_block_does_not_append_an_event(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    first = block(None, spend("funding:0", "bob"))
    live.add_block(first)

    assert live.add_block(first).decision == "duplicate"
    assert live.event_count == 1
    assert len(path.read_text().splitlines()) == 1


def test_invalid_block_is_not_persisted(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    invalid = block(None, spend("missing:0", "bob"))

    with pytest.raises(ValueError, match="missing or already-spent"):
        live.add_block(invalid)

    assert live.event_count == 0
    assert not path.exists()


def test_duplicate_json_field_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    path.write_text('{"schema_version":1,"schema_version":1}\n', encoding="utf-8")

    with pytest.raises(JournalCorruption, match="duplicate JSON field"):
        journal(path)


def test_record_size_is_bounded_before_append(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = JournaledChain(
        path,
        {"funding:0": Output("alice", 100)},
        max_record_bytes=256,
    )
    candidate = block(None, spend("funding:0", "x" * 128))

    with pytest.raises(ValueError, match="exceeds max_record_bytes"):
        live.add_block(candidate)

    assert not path.exists()


def test_snapshot_head_matches_last_event_digest(tmp_path: Path) -> None:
    path = tmp_path / "chain.jsonl"
    live = journal(path)
    live.add_block(block(None, spend("funding:0", "bob")))
    record = json.loads(path.read_text().splitlines()[0])

    assert live.snapshot()["journal_head"] == record["event_hash"]

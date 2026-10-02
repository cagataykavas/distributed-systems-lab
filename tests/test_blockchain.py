from __future__ import annotations

from dataclasses import replace

import pytest

from resilience.blockchain import (
    GENESIS_HASH,
    Block,
    CapacityExceeded,
    ChainReconciler,
    FinalityViolation,
    InvalidBlock,
    Output,
    Transaction,
    TransactionStatus,
    UnknownParent,
    _demo,
)


def chain(*, depth: int = 3, capacity: int = 100) -> ChainReconciler:
    return ChainReconciler(
        {"funding:0": Output("alice", 100)},
        confirmation_depth=depth,
        max_known_blocks=capacity,
    )


def spend(outpoint: str, *outputs: tuple[str, int]) -> Transaction:
    return Transaction.build([outpoint], [Output(owner, amount) for owner, amount in outputs])


def block(parent: Block | None, *transactions: Transaction, work: int = 1) -> Block:
    return Block.build(
        parent_hash=GENESIS_HASH if parent is None else parent.block_hash,
        height=1 if parent is None else parent.height + 1,
        work=work,
        transactions=transactions,
    )


def test_heavier_fork_reorganizes_utxo_state() -> None:
    ledger = chain()
    tx_a = spend("funding:0", ("alice", 40), ("bob", 60))
    a1 = block(None, tx_a)
    ledger.add_block(a1)
    a2 = block(a1, spend(f"{tx_a.txid}:0", ("carol", 40)))
    ledger.add_block(a2)

    tx_b = spend("funding:0", ("alice", 25), ("dave", 75))
    b1 = block(None, tx_b)
    assert ledger.add_block(b1).decision == "side_branch"
    b2 = block(b1, spend(f"{tx_b.txid}:0", ("erin", 25)), work=2)
    result = ledger.add_block(b2)

    assert result.decision == "reorg"
    assert result.removed == (a2.block_hash, a1.block_hash)
    assert result.added == (b1.block_hash, b2.block_hash)
    assert ledger.balance("bob") == 0
    assert ledger.balance("dave") == 75
    assert ledger.balance("erin") == 25
    assert ledger.transaction_status(tx_a.txid) is TransactionStatus.ORPHANED
    assert ledger.transaction_status(tx_b.txid) is TransactionStatus.PENDING


def test_equal_work_preserves_first_seen_tip() -> None:
    ledger = chain()
    a1 = block(None, spend("funding:0", ("bob", 100)))
    b1 = block(None, spend("funding:0", ("carol", 100)))

    ledger.add_block(a1)
    result = ledger.add_block(b1)

    assert result.decision == "side_branch"
    assert ledger.tip_hash == a1.block_hash


def test_direct_extension_reports_append_and_finalizes_transaction() -> None:
    ledger = chain(depth=2)
    tx = spend("funding:0", ("bob", 100))
    first = block(None, tx)
    ledger.add_block(first)
    assert ledger.transaction_status(tx.txid) is TransactionStatus.PENDING

    second = block(first)
    result = ledger.add_block(second)

    assert result.decision == "appended"
    assert result.newly_finalized == (tx.txid,)
    assert ledger.transaction_status(tx.txid) is TransactionStatus.FINALIZED


def test_reorg_cannot_remove_finalized_block() -> None:
    ledger = chain(depth=2)
    a1 = block(None, spend("funding:0", ("bob", 100)))
    ledger.add_block(a1)
    a2 = block(a1)
    ledger.add_block(a2)
    b1 = block(None, spend("funding:0", ("carol", 100)), work=3)

    with pytest.raises(FinalityViolation, match="precedes finalized height"):
        ledger.add_block(b1)

    assert ledger.tip_hash == a2.block_hash
    assert ledger.snapshot()["known_blocks"] == 2


def test_side_branch_is_validated_against_its_own_parent_state() -> None:
    ledger = chain()
    a1 = block(None, spend("funding:0", ("bob", 100)))
    ledger.add_block(a1)
    b_tx = spend("funding:0", ("carol", 100))
    b1 = block(None, b_tx)
    ledger.add_block(b1)

    b2 = block(b1, spend(f"{b_tx.txid}:0", ("dave", 100)), work=2)
    ledger.add_block(b2)

    assert ledger.balance("dave") == 100
    assert ledger.balance("bob") == 0


def test_unknown_parent_fails_closed() -> None:
    candidate = Block.build(parent_hash="f" * 64, height=1, work=1, transactions=[])

    with pytest.raises(UnknownParent):
        chain().add_block(candidate)


def test_tampered_block_hash_is_rejected() -> None:
    candidate = block(None, spend("funding:0", ("bob", 100)))

    with pytest.raises(InvalidBlock, match="block hash"):
        chain().add_block(replace(candidate, work=2))


def test_tampered_transaction_id_is_rejected() -> None:
    transaction = spend("funding:0", ("bob", 100))
    tampered = replace(transaction, outputs=(Output("mallory", 100),))

    with pytest.raises(InvalidBlock, match="txid"):
        chain().add_block(block(None, tampered))


def test_double_spend_inside_block_is_rejected() -> None:
    first = spend("funding:0", ("bob", 100))
    second = spend("funding:0", ("carol", 100))

    with pytest.raises(InvalidBlock, match="already-spent"):
        chain().add_block(block(None, first, second))


def test_duplicate_input_inside_transaction_is_rejected() -> None:
    transaction = Transaction.build(
        ["funding:0", "funding:0"],
        [Output("bob", 200)],
    )

    with pytest.raises(InvalidBlock, match="duplicate inputs"):
        chain().add_block(block(None, transaction))


def test_malformed_outpoint_is_rejected() -> None:
    transaction = Transaction.build([""], [Output("bob", 100)])

    with pytest.raises(InvalidBlock, match="inputs must contain"):
        chain().add_block(block(None, transaction))


def test_value_creation_is_rejected() -> None:
    inflation = spend("funding:0", ("bob", 101))

    with pytest.raises(InvalidBlock, match="conserve value"):
        chain().add_block(block(None, inflation))


def test_duplicate_block_is_idempotent() -> None:
    ledger = chain()
    candidate = block(None, spend("funding:0", ("bob", 100)))
    ledger.add_block(candidate)

    result = ledger.add_block(candidate)

    assert result.decision == "duplicate"
    assert not result.canonical_changed
    assert ledger.snapshot()["known_blocks"] == 1


def test_block_capacity_is_bounded() -> None:
    ledger = chain(capacity=1)
    first = block(None, spend("funding:0", ("bob", 100)))
    ledger.add_block(first)

    with pytest.raises(CapacityExceeded):
        ledger.add_block(block(first))


def test_unknown_transaction_status() -> None:
    assert chain().transaction_status("missing") is TransactionStatus.UNKNOWN


def test_demo_emits_reorg_evidence_without_false_balances() -> None:
    report = _demo()

    assert report["schema_version"] == 1
    assert report["reconciliation"]["decision"] == "reorg"
    assert report["balances"] == {
        "alice": 0,
        "bob": 0,
        "carol": 0,
        "dave": 75,
        "erin": 25,
    }
    assert set(report["transaction_status"].values()) == {"pending", "orphaned"}

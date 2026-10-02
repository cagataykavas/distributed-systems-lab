"""Deterministic UTXO-chain reconciliation with bounded reorganization safety."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

GENESIS_HASH = "0" * 64
MAX_AMOUNT = 2**63 - 1
MAX_WORK = 10**9


class ChainError(ValueError):
    """Base class for rejected chain evidence."""


class UnknownParent(ChainError):
    """Raised when a block's parent has not been admitted."""


class InvalidBlock(ChainError):
    """Raised when block or transaction invariants fail."""


class FinalityViolation(ChainError):
    """Raised when a candidate chain would roll back finalized state."""


class CapacityExceeded(ChainError):
    """Raised when configured evidence bounds are exhausted."""


class TransactionStatus(StrEnum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    FINALIZED = "finalized"
    ORPHANED = "orphaned"


@dataclass(frozen=True, slots=True)
class Output:
    owner: str
    amount: int

    def as_dict(self) -> dict[str, str | int]:
        return {"owner": self.owner, "amount": self.amount}


@dataclass(frozen=True, slots=True)
class Transaction:
    txid: str
    inputs: tuple[str, ...]
    outputs: tuple[Output, ...]

    @classmethod
    def build(cls, inputs: Iterable[str], outputs: Iterable[Output]) -> Transaction:
        input_tuple = tuple(inputs)
        output_tuple = tuple(outputs)
        txid = _digest(
            {
                "inputs": list(input_tuple),
                "outputs": [output.as_dict() for output in output_tuple],
            }
        )
        return cls(txid=txid, inputs=input_tuple, outputs=output_tuple)

    def expected_txid(self) -> str:
        return _digest(
            {
                "inputs": list(self.inputs),
                "outputs": [output.as_dict() for output in self.outputs],
            }
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "txid": self.txid,
            "inputs": list(self.inputs),
            "outputs": [output.as_dict() for output in self.outputs],
        }


@dataclass(frozen=True, slots=True)
class Block:
    block_hash: str
    parent_hash: str
    height: int
    work: int
    transactions: tuple[Transaction, ...]

    @classmethod
    def build(
        cls,
        *,
        parent_hash: str,
        height: int,
        work: int,
        transactions: Iterable[Transaction],
    ) -> Block:
        txs = tuple(transactions)
        payload = _block_payload(parent_hash, height, work, txs)
        return cls(
            block_hash=_digest(payload),
            parent_hash=parent_hash,
            height=height,
            work=work,
            transactions=txs,
        )

    def expected_hash(self) -> str:
        return _digest(_block_payload(self.parent_hash, self.height, self.work, self.transactions))


@dataclass(frozen=True, slots=True)
class Reconciliation:
    decision: str
    old_tip: str
    new_tip: str
    fork_height: int
    removed: tuple[str, ...]
    added: tuple[str, ...]
    newly_finalized: tuple[str, ...]

    @property
    def canonical_changed(self) -> bool:
        return self.old_tip != self.new_tip

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "canonical_changed": self.canonical_changed,
            "old_tip": self.old_tip,
            "new_tip": self.new_tip,
            "fork_height": self.fork_height,
            "removed": list(self.removed),
            "added": list(self.added),
            "newly_finalized": list(self.newly_finalized),
        }


@dataclass(frozen=True, slots=True)
class _Node:
    block: Block | None
    cumulative_work: int


def _digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _block_payload(
    parent_hash: str,
    height: int,
    work: int,
    transactions: tuple[Transaction, ...],
) -> dict[str, Any]:
    return {
        "parent_hash": parent_hash,
        "height": height,
        "work": work,
        "transactions": [transaction.as_dict() for transaction in transactions],
    }


def _validate_output(output: Output, context: str) -> None:
    if not isinstance(output.owner, str) or not 1 <= len(output.owner) <= 128:
        raise InvalidBlock(f"{context} owner must contain 1..128 characters")
    if type(output.amount) is not int or not 1 <= output.amount <= MAX_AMOUNT:
        raise InvalidBlock(f"{context} amount must be a positive bounded integer")


class ChainReconciler:
    """Validate competing UTXO branches and select the heaviest admissible chain.

    Equal-work branches never displace the first-seen canonical tip. A heavier
    branch may replace it only when the fork point does not precede finalized
    canonical state.
    """

    def __init__(
        self,
        genesis_utxos: Mapping[str, Output],
        *,
        confirmation_depth: int = 6,
        max_known_blocks: int = 10_000,
        max_transactions_per_block: int = 1_000,
        max_io_per_transaction: int = 100,
    ) -> None:
        if type(confirmation_depth) is not int or confirmation_depth < 1:
            raise ValueError("confirmation_depth must be a positive integer")
        if type(max_known_blocks) is not int or max_known_blocks < 1:
            raise ValueError("max_known_blocks must be a positive integer")
        if type(max_transactions_per_block) is not int or max_transactions_per_block < 1:
            raise ValueError("max_transactions_per_block must be a positive integer")
        if type(max_io_per_transaction) is not int or max_io_per_transaction < 1:
            raise ValueError("max_io_per_transaction must be a positive integer")

        initial: dict[str, Output] = {}
        for outpoint, output in genesis_utxos.items():
            if not isinstance(outpoint, str) or not 1 <= len(outpoint) <= 256:
                raise ValueError("genesis outpoints must contain 1..256 characters")
            _validate_output(output, f"genesis[{outpoint}]")
            initial[outpoint] = output

        self._confirmation_depth = confirmation_depth
        self._max_known_blocks = max_known_blocks
        self._max_transactions_per_block = max_transactions_per_block
        self._max_io_per_transaction = max_io_per_transaction
        self._nodes = {GENESIS_HASH: _Node(block=None, cumulative_work=0)}
        self._states = {GENESIS_HASH: initial}
        self._transaction_blocks: dict[str, set[str]] = {}
        self._tip = GENESIS_HASH

    @property
    def tip_hash(self) -> str:
        return self._tip

    @property
    def tip_height(self) -> int:
        return self._height(self._tip)

    @property
    def finalized_height(self) -> int:
        return max(0, self.tip_height - self._confirmation_depth + 1)

    def snapshot(self) -> dict[str, Any]:
        return {
            "tip_hash": self.tip_hash,
            "tip_height": self.tip_height,
            "cumulative_work": self._nodes[self._tip].cumulative_work,
            "finalized_height": self.finalized_height,
            "confirmation_depth": self._confirmation_depth,
            "known_blocks": len(self._nodes) - 1,
            "utxo_count": len(self._states[self._tip]),
        }

    def balance(self, owner: str) -> int:
        return sum(
            output.amount for output in self._states[self._tip].values() if output.owner == owner
        )

    def transaction_status(self, txid: str) -> TransactionStatus:
        locations = self._transaction_blocks.get(txid)
        if not locations:
            return TransactionStatus.UNKNOWN
        canonical = self._canonical_hashes(self._tip)
        canonical_location = next(
            (block_hash for block_hash in locations if block_hash in canonical),
            None,
        )
        if canonical_location is None:
            return TransactionStatus.ORPHANED
        if self._height(canonical_location) <= self.finalized_height:
            return TransactionStatus.FINALIZED
        return TransactionStatus.PENDING

    def add_block(self, block: Block) -> Reconciliation:
        if block.block_hash in self._nodes:
            return Reconciliation(
                decision="duplicate",
                old_tip=self._tip,
                new_tip=self._tip,
                fork_height=self.tip_height,
                removed=(),
                added=(),
                newly_finalized=(),
            )
        if len(self._nodes) - 1 >= self._max_known_blocks:
            raise CapacityExceeded("known block capacity exhausted")
        parent = self._nodes.get(block.parent_hash)
        if parent is None:
            raise UnknownParent(f"unknown parent: {block.parent_hash}")
        self._validate_header(block, parent)
        state = self._apply_transactions(self._states[block.parent_hash], block)
        candidate = _Node(block=block, cumulative_work=parent.cumulative_work + block.work)

        old_tip = self._tip
        old_finalized = self._finalized_transactions(old_tip)
        should_switch = candidate.cumulative_work > self._nodes[old_tip].cumulative_work
        fork_hash = self._fork_point(old_tip, block.block_hash, pending=block)
        fork_height = self._height(fork_hash)
        if should_switch and fork_height < self.finalized_height:
            raise FinalityViolation(
                f"fork at height {fork_height} precedes finalized height {self.finalized_height}"
            )

        self._nodes[block.block_hash] = candidate
        self._states[block.block_hash] = state
        for transaction in block.transactions:
            self._transaction_blocks.setdefault(transaction.txid, set()).add(block.block_hash)

        if not should_switch:
            return Reconciliation(
                decision="side_branch",
                old_tip=old_tip,
                new_tip=old_tip,
                fork_height=fork_height,
                removed=(),
                added=(),
                newly_finalized=(),
            )

        self._tip = block.block_hash
        removed = tuple(reversed(self._path_after(fork_hash, old_tip)))
        added = tuple(self._path_after(fork_hash, self._tip))
        newly_finalized = tuple(sorted(self._finalized_transactions(self._tip) - old_finalized))
        decision = "appended" if block.parent_hash == old_tip else "reorg"
        return Reconciliation(
            decision=decision,
            old_tip=old_tip,
            new_tip=self._tip,
            fork_height=fork_height,
            removed=removed,
            added=added,
            newly_finalized=newly_finalized,
        )

    def _validate_header(self, block: Block, parent: _Node) -> None:
        parent_height = 0 if parent.block is None else parent.block.height
        if type(block.height) is not int or block.height != parent_height + 1:
            raise InvalidBlock(f"height must be parent height + 1 ({parent_height + 1})")
        if type(block.work) is not int or not 1 <= block.work <= MAX_WORK:
            raise InvalidBlock("work must be a positive bounded integer")
        if len(block.transactions) > self._max_transactions_per_block:
            raise InvalidBlock("transaction count exceeds block bound")
        if block.block_hash != block.expected_hash():
            raise InvalidBlock("block hash does not match canonical contents")

    def _apply_transactions(
        self, parent_state: Mapping[str, Output], block: Block
    ) -> dict[str, Output]:
        state = dict(parent_state)
        block_txids: set[str] = set()
        for tx_index, transaction in enumerate(block.transactions):
            context = f"transactions[{tx_index}]"
            if transaction.txid != transaction.expected_txid():
                raise InvalidBlock(f"{context} txid does not match canonical contents")
            if transaction.txid in block_txids:
                raise InvalidBlock(f"{context} duplicates a transaction in the block")
            block_txids.add(transaction.txid)
            if not transaction.inputs or not transaction.outputs:
                raise InvalidBlock(f"{context} must contain inputs and outputs")
            if (
                len(transaction.inputs) > self._max_io_per_transaction
                or len(transaction.outputs) > self._max_io_per_transaction
            ):
                raise InvalidBlock(f"{context} exceeds the transaction I/O bound")
            if len(transaction.inputs) != len(set(transaction.inputs)):
                raise InvalidBlock(f"{context} contains duplicate inputs")
            if any(
                not isinstance(outpoint, str) or not 1 <= len(outpoint) <= 256
                for outpoint in transaction.inputs
            ):
                raise InvalidBlock(f"{context} inputs must contain 1..256 characters")

            input_total = 0
            for outpoint in transaction.inputs:
                spent = state.get(outpoint)
                if spent is None:
                    raise InvalidBlock(
                        f"{context} spends missing or already-spent output {outpoint}"
                    )
                input_total += spent.amount
            output_total = 0
            for output_index, output in enumerate(transaction.outputs):
                _validate_output(output, f"{context}.outputs[{output_index}]")
                output_total += output.amount
            if input_total != output_total:
                raise InvalidBlock(
                    f"{context} does not conserve value: {input_total} != {output_total}"
                )

            for outpoint in transaction.inputs:
                del state[outpoint]
            for output_index, output in enumerate(transaction.outputs):
                outpoint = f"{transaction.txid}:{output_index}"
                if outpoint in state:
                    raise InvalidBlock(f"{context} creates an existing outpoint")
                state[outpoint] = output
        return state

    def _height(self, block_hash: str) -> int:
        block = self._nodes[block_hash].block
        return 0 if block is None else block.height

    def _parent_hash(self, block_hash: str, *, pending: Block | None = None) -> str | None:
        if pending is not None and block_hash == pending.block_hash:
            return pending.parent_hash
        block = self._nodes[block_hash].block
        return None if block is None else block.parent_hash

    def _fork_point(self, left: str, right: str, *, pending: Block | None = None) -> str:
        left_ancestors = self._canonical_hashes(left)
        cursor = right
        while cursor not in left_ancestors:
            parent = self._parent_hash(cursor, pending=pending)
            if parent is None:
                raise AssertionError("branches must share genesis")
            cursor = parent
        return cursor

    def _canonical_hashes(self, tip: str) -> set[str]:
        hashes: set[str] = set()
        cursor: str | None = tip
        while cursor is not None:
            hashes.add(cursor)
            cursor = self._parent_hash(cursor)
        return hashes

    def _path_after(self, ancestor: str, tip: str) -> list[str]:
        path: list[str] = []
        cursor = tip
        while cursor != ancestor:
            path.append(cursor)
            parent = self._parent_hash(cursor)
            if parent is None:
                raise AssertionError("ancestor is not on path")
            cursor = parent
        path.reverse()
        return path

    def _finalized_transactions(self, tip: str) -> set[str]:
        finalized_height = max(0, self._height(tip) - self._confirmation_depth + 1)
        finalized: set[str] = set()
        for block_hash in self._canonical_hashes(tip):
            node = self._nodes[block_hash]
            if node.block is not None and node.block.height <= finalized_height:
                finalized.update(transaction.txid for transaction in node.block.transactions)
        return finalized


def _demo() -> dict[str, Any]:
    chain = ChainReconciler(
        {"funding:0": Output("alice", 100)},
        confirmation_depth=3,
    )
    tx_a = Transaction.build(
        ["funding:0"],
        [Output("alice", 40), Output("bob", 60)],
    )
    a1 = Block.build(parent_hash=GENESIS_HASH, height=1, work=1, transactions=[tx_a])
    chain.add_block(a1)
    tx_a2 = Transaction.build([f"{tx_a.txid}:0"], [Output("carol", 40)])
    a2 = Block.build(parent_hash=a1.block_hash, height=2, work=1, transactions=[tx_a2])
    chain.add_block(a2)

    tx_b = Transaction.build(
        ["funding:0"],
        [Output("alice", 25), Output("dave", 75)],
    )
    b1 = Block.build(parent_hash=GENESIS_HASH, height=1, work=1, transactions=[tx_b])
    chain.add_block(b1)
    tx_b2 = Transaction.build([f"{tx_b.txid}:0"], [Output("erin", 25)])
    b2 = Block.build(parent_hash=b1.block_hash, height=2, work=2, transactions=[tx_b2])
    reconciliation = chain.add_block(b2)
    return {
        "schema_version": 1,
        "scenario": "heavier-fork-reorganization",
        "reconciliation": reconciliation.as_dict(),
        "snapshot": chain.snapshot(),
        "balances": {
            owner: chain.balance(owner) for owner in ("alice", "bob", "carol", "dave", "erin")
        },
        "transaction_status": {
            tx_a.txid: chain.transaction_status(tx_a.txid),
            tx_b.txid: chain.transaction_status(tx_b.txid),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the deterministic blockchain reorg scenario")
    parser.add_argument("--output", type=Path, help="write JSON evidence to this path")
    args = parser.parse_args(argv)
    rendered = json.dumps(_demo(), indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Review and Fix Report

## Changes
- Added shard helpers to remove entries while also cleaning expiration indexes.
- Treated expired-but-unswept keys as missing for `DEL`, `APPEND`, `EXPIRE`, and `PERSIST`.
- Made `MSET` remove stale expiration index entries when overwriting keys with TTL.
- Added TTL regression tests for lazy expiration semantics and MSET index cleanup.

## Verification
- `cargo test` passed.
- `git diff --check` passed.

## Remaining
- Historical note resolved in later commits: AOF now writes absolute TTLs where
  Redis accepts them, keeps extremely large Redis-compatible relative TTLs
  replayable, and has regression coverage for restart behavior.

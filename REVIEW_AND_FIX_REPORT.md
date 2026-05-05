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
- AOF still records relative TTL commands and can resurrect expired keys after replay. I left that larger persistence design for a separate pass.

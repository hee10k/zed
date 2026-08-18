# 05 — git fetch with prune

**What to build:** The existing Fetch and Fetch-From actions prune deleted remote-tracking branches (`--prune`) on every fetch. The prune behaviour is default-on, routed through a `FetchOptions` extension so the fetch command builder appends `--prune`.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `git fetch` and `git fetch --from` run with `--prune`.
- [x] Stale deleted-remote-tracking refs are removed on fetch.
- [x] The prune behaviour is on by default.
- [x] FetchOptions carries the prune dimension through to the command builder.

**Verification:** `cargo test -p git test_fetch_prunes_deleted_remote_tracking_branch` passes — fetch with a server-deleted feature branch prunes its local `origin/feature` ref while keeping `origin/main`. Clippy finds no new issues in this crate (3 pre-existing findings on this branch, all outside the touched regions).
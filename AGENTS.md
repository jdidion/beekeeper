# AGENTS.md — beekeeper

Worker-pool library for Rust. Edition 2024, MSRV 1.85. Trunk branch: `main`.

## Build / test / lint

```sh
# build with the canonical feature set
cargo build -F affinity,local-batch,retry

# unit + doc tests (the CI `check` job runs clippy, fmt, doc, and doc-tests)
cargo test  -F affinity,local-batch,retry
cargo test  -F affinity,local-batch,retry --doc

# lint exactly as CI does (warnings are denied)
cargo clippy --all-targets -F affinity,local-batch,retry -- -D warnings
cargo fmt -- --check
RUSTDOCFLAGS="-D warnings" cargo doc -F affinity,local-batch,retry
```

## Features

`default = ["local-batch"]`. Optional: `affinity`, `local-batch`, `retry`.

The channel-backend features `crossbeam`, `flume`, and `loole` are **mutually
exclusive** — at most one may be enabled, and with none enabled the library uses
`std::sync::mpsc`. `cargo …  --all-features` therefore does **not** compile;
build/test each backend separately (CI runs a matrix over `default`, `crossbeam`,
`flume`, `loole`). The canonical feature set used for lint/doc/coverage is
`affinity,local-batch,retry`.

Tests are inline (`#[cfg(test)] mod tests`) per source file; there is no `tests/`
directory. Test modules carry `#[cfg_attr(coverage_nightly, coverage(off))]` so
the test code itself is excluded from coverage.

## Coverage

```sh
cargo llvm-cov -F affinity,local-batch,retry --summary-only          # totals
cargo llvm-cov -F affinity,local-batch,retry --show-missing-lines    # uncovered lines
cargo llvm-cov -F affinity,local-batch,retry --lcov --output-path lcov.info
```

Coverage needs `llvm-tools` (for `llvm-profdata`/`llvm-cov`). If the active
toolchain lacks them (e.g. a Homebrew-installed `rustc`), run through a rustup
toolchain that has the `llvm-tools` component, e.g.
`rustup run stable cargo llvm-cov …`.

## Release

Releases are automated via `release-plz` (`release-plz.toml`) and changelog
generation via `git-cliff` (`cliff.toml`). Publishing to crates.io is a
maintainer action — do not publish from an agent session.

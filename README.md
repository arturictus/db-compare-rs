# Rust DB migration checker

Command line tool to verify that two postgres databases have the same data.

Usefull to verify when moving databases to different cloud provider.

## Installation

- get github token to access to repo releases
- install (eget)[https://github.com/zyedidia/eget]

**run:**

```
export EGET_GITHUB_TOKEN=ghp_1234567890

eget --sha256 selma-finance/rust_db_migrate_checker
=> sha256output

eget --verify-sha256=sha256output selma-finance/rust_db_migrate_checker
```

## RUN

**Local:**

```
cargo run -- --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

**Installed bin:**

```
db-compare --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

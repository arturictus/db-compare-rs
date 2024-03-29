# DB-Compare

Command line tool to compare two Postgres databases.

![Screenshot](/media/screenshot.png?raw=true)

## Installation

```shell
cargo install --git "https://github.com/arturictus/db-compare-rs"
```

## Usage

```sh
db-compare run --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

Run `--help` for more information

```sh
db-compare --help
db-compare run --help
db-compare summarize --help
```

### Run command

#### Jobs

- **by_id_excluding_replica_updated_ats** [default if no job list is supplied]

  1. Gets replica `updated_at` `id` after cutoff.
  2. Gets max number `id` from the master.
  3. Compares rows by id excluding ids in the step 1 list.
  4. Stops if `by-id-sample-size` arg is reached.

- **by_id**

  1. Gets max number `id` from the master.
  2. Compares rows grouped by id.
  3. Stops if `by-id-sample-size` arg is reached.

- **counters**

  Compares the `count` of each table between databases.

- **sequences**

  Compares the `sequences` of each table between databases.

- **last_updated_ats:**

  _It tries to compare grouping by id if the table has the id column_

  Compares last updated ats rows until the `limit` arg is reached

- **last_created_ats:**

  _It tries to compare grouping by id if the table has the id column._

  Compares last created ats rows until the `limit` arg is reached

#### Output

Markers:

- `+`: Not in DB1.
- `-`: Not in DB2.
- `>`: Different.
- `@@ ... @@`: Comments and markers.

#### Config File

You can pass all the arguments in a `yaml` file for convenience.

**IMPORTANT:** Command params take precedence over this configuration.

```sh
db-compare run --config ./config.yml
```

```yaml
# ./config.yml
db1: "postgresql://postgres:postgres@127.0.0.1/db1"
db2: "postgresql://postgres:postgres@127.0.0.1/db2"
tables:
  - users
jobs:
  - by_id_excluding_replica_updated_ats # default: no need to pass jobs list if only running this job
  - counters
  - last_updated_ats
  - last_created_ats
  - by_id
  - sequences
  - updated_ats_until
limit: 100
output-folder: ./my_diffs
by-id-sample-size: 10000 # If wanting to test all rows, remove this config
```

All configs can be overridden by command parameters.

```sh
db-compare run --db2 postgresql://postgres:postgres@127.0.0.1/another_replica --limit 1000 --config ./config.yml
```

### Summarize Command

Parses the `run` command's output file and prints a summary.

```shell
db-compare summarize -f tests/fixtures/examples/outputs/users.diff
=>
Summary:
  Table: `users`
  Updated: 14
  Deleted: 13
  Created: 0
  Updated ids:
    - [40, 37, 34, 31, 28, 25, 22, 19, 16, 13, 10, 7, 4, 1]
  Updated columns:
    name: 14
    updated_at: 7
```

## Development

```sh
cargo run -- run --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

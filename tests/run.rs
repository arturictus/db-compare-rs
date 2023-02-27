mod common;
use common::{around, User, DB};
use db_compare::*;

fn default_args() -> Args {
    Args {
        db1: Some(DB::A.url().to_string()),
        db2: Some(DB::B.url().to_string()),
        limit: 1,
        no_tls: false,
        all_columns_sample_size: None,
        diff_file: None,
        tables_file: None,
        config: Some("./tests/fixtures/run-test-config.yml".to_string()),
    }
}
#[test]
fn integration_test() {
    around(|| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        // let second = first.next(None).insert(DB1_URL).unwrap();
        // let third = second.next(None).insert(DB1_URL).unwrap();

        db_compare::run(default_args())
    });
}

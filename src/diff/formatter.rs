use crate::{DBResultTypes, DBsResults, JsonMap};
use similar::{ChangeTag, TextDiff};
use std::collections::BTreeMap;

pub fn call(result: DBsResults) -> Vec<(String, String)> {
    let (header, a, b) = result;
    let (rows, missing) = generate_diff(&a, &b);
    let mut out = vec![(header.clone(), rows)];

    if !missing.is_empty() {
        out.push(("|- missing rows".to_string(), missing));
    }
    out
}

fn generate_diff(a: &DBResultTypes, b: &DBResultTypes) -> (String, String) {
    let (rows, missing) = match (a, b) {
        (DBResultTypes::Map(_a), DBResultTypes::Map(_b)) => normalize_map_type(a, b),
        _ => (
            print_diff(&produce_diff(
                &normalize_input(&a).unwrap(),
                &normalize_input(&b).unwrap(),
            )),
            "".to_string(),
        ),
    };
    (rows, missing)
}

fn normalize_map_type(a: &DBResultTypes, b: &DBResultTypes) -> (String, String) {
    let RowSelector {
        matches: (result_a, result_b),
        missing,
    } = only_matching_ids(a, b);
    let rows = result_a
        .to_h()
        .into_iter()
        .zip(result_b.to_h())
        .map(|(a, b)| {
            print_diff(&produce_diff(
                &serde_json::to_string(&a).unwrap(),
                &serde_json::to_string(&b).unwrap(),
            ))
        })
        .collect();
    let missing = print_diff(&produce_diff(
        &normalize_input(&missing).unwrap(),
        &normalize_input(&DBResultTypes::Empty).unwrap(),
    ));
    (rows, missing)
}

#[derive(Debug)]
struct RowSelector {
    matches: (DBResultTypes, DBResultTypes),
    missing: DBResultTypes,
}
fn only_matching_ids(a: &DBResultTypes, b: &DBResultTypes) -> RowSelector {
    if let (DBResultTypes::Map(tmp_a), DBResultTypes::Map(_tmp_b)) = (a, b) {
        if let Some(first) = tmp_a.clone().first() {
            if first.contains_key("id") && first.get("id").unwrap().as_u64().is_some() {
                return group_by_id(a, b);
            }
        }
    };
    RowSelector {
        matches: (a.clone(), b.clone()),
        missing: DBResultTypes::Empty,
    }
}

fn group_by_id(a: &DBResultTypes, b: &DBResultTypes) -> RowSelector {
    let btree: BTreeMap<u64, JsonMap> =
        b.to_h().into_iter().fold(BTreeMap::new(), |mut acc, data| {
            acc.insert(data.get("id").unwrap().as_u64().unwrap(), data);
            acc
        });
    let mut missing: Vec<JsonMap> = Vec::new();
    let acc: Vec<(JsonMap, JsonMap)> = a.to_h().into_iter().fold(Vec::new(), |mut acc, data| {
        let id = data.get("id").unwrap().as_u64().unwrap();
        if let Some(value) = btree.get(&id) {
            acc.push((data, value.clone()));
        } else {
            missing.push(data);
        }
        acc
    });
    let mut a_result: Vec<JsonMap> = vec![];
    let mut b_result: Vec<JsonMap> = vec![];
    for e in acc {
        a_result.push(e.0);
        b_result.push(e.1);
    }

    RowSelector {
        matches: (DBResultTypes::Map(a_result), DBResultTypes::Map(b_result)),
        missing: if missing.is_empty() {
            DBResultTypes::Empty
        } else {
            DBResultTypes::Map(missing)
        },
    }
}
fn normalize_input(list: &DBResultTypes) -> Result<std::string::String, serde_json::Error> {
    let list: Vec<String> = match list {
        DBResultTypes::String(l) => l.clone(),
        DBResultTypes::Map(e) => e
            .into_iter()
            .map(|e| serde_json::to_string(&e).unwrap())
            .collect(),
        DBResultTypes::Empty => vec![],
    };
    serde_json::to_string_pretty(&list)
}

fn produce_diff(json1: &str, json2: &str) -> String {
    let diff = TextDiff::from_lines(json1, json2);
    let mut output = Vec::new();

    for change in diff.iter_all_changes() {
        if change.tag() == ChangeTag::Equal {
            continue;
        }
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        output.push(format!("{sign}{change}"));
    }
    output.join("")
}

fn print_diff(result: &str) -> String {
    match result {
        "" => "✓".to_string(),
        diff => diff.to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_str, Map, Value};

    #[test]
    fn test_diff_dates() {
        assert_eq!(
            produce_diff(
                "2 : 2023-02-01 11:28:44.453989",
                "2 : 2023-02-01 11:28:45.453989",
            ),
            "-2 : 2023-02-01 11:28:44.453989\n+2 : 2023-02-01 11:28:45.453989\n"
        );
        assert_eq!(
            produce_diff(
                "2 : 2023-02-01 11:28:45.453989",
                "2 : 2023-02-01 11:28:45.453989",
            ),
            ""
        )
    }

    #[test]
    fn test_only_matching_ids() {
        let a = gen_data(1);
        let b = gen_data(2);
        let c = gen_data(3);
        let d = gen_data(4);

        let RowSelector {
            matches: (result_a, result_b),
            missing,
        } = only_matching_ids(
            &DBResultTypes::Map(vec![a.clone(), b.clone(), c.clone()]),
            &DBResultTypes::Map(vec![a.clone(), b.clone(), d.clone()]),
        );
        assert_eq!(result_a.to_h(), vec![a.clone(), b.clone()]);
        assert_eq!(result_b.to_h(), vec![a.clone(), b.clone()]);
        assert_eq!(missing.to_h(), vec![c.clone()]);
    }

    fn gen_data(id: u64) -> Map<String, Value> {
        let data = format!(r#"{{"id": {id},"name": "John_{id}"}}"#);
        let mut v: Value = from_str(&data).unwrap();
        let val = v.as_object_mut().unwrap();
        let mut m = Map::new();
        m.append(val);
        m
    }
}

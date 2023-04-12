use crate::Config;

use crate::database::{DBResultType, DBsResults, JsonMap};
use similar::{ChangeTag, TextDiff};
use std::collections::BTreeMap;

type FmtHeader = Option<String>;
type FmtRows = Vec<String>;
type FmtMissing = Option<Vec<String>>;
type FmtExtra = Option<Vec<String>>;
pub type FmtOutput = (FmtHeader, FmtRows, FmtMissing, FmtExtra);
pub fn call(config: &Config, result: DBsResults) -> Vec<FmtOutput> {
    let (header, a, b) = result;
    let (rows, missing, extra) = generate_diff(config, &a, &b);

    vec![(Some(header), rows, missing, extra)]
}

fn generate_diff(
    config: &Config,
    a: &DBResultType,
    b: &DBResultType,
) -> (FmtRows, FmtMissing, FmtExtra) {
    let (rows, missing, extra) = match (a, b) {
        (DBResultType::JsonMaps(_a), DBResultType::JsonMaps(_b)) => compare_json_maps(config, a, b),

        _ => {
            let st = print_diff(produce_simple_diff(
                &normalize_input(a).unwrap(),
                &normalize_input(b).unwrap(),
            ));
            (vec![st], None, None)
        }
    };
    (rows, missing, extra)
}

fn compare_json_maps(
    config: &Config,
    a: &DBResultType,
    b: &DBResultType,
) -> (FmtRows, FmtMissing, FmtExtra) {
    let RowSelector {
        matches: (result_a, result_b),
        missing,
        extra,
    } = only_matching_ids(a, b);

    fn filter_map(
        config: &Config,
        iter: impl IntoIterator<Item = (JsonMap, JsonMap)>,
    ) -> Vec<String> {
        iter.into_iter()
            .filter_map(|(a, b)| {
                produce_diff(
                    config,
                    &serde_json::to_string(&a).unwrap(),
                    &serde_json::to_string(&b).unwrap(),
                )
            })
            .collect()
    }

    let rows = match result_a {
        DBResultType::JsonMaps(a) => filter_map(config, a.into_iter().zip(result_b.to_h())),
        DBResultType::GroupedRows(a) => filter_map(config, a),
        _ => panic!("unexpected type: {:?}", result_a),
    };

    let missing = do_unmached_rows_format(&missing, "-");
    let extra = do_unmached_rows_format(&extra, "+");
    (rows, missing, extra)
}

fn do_unmached_rows_format(missing: &DBResultType, operator: &str) -> Option<Vec<String>> {
    match missing {
        DBResultType::JsonMaps(s) => {
            let coll: Vec<String> = s
                .iter()
                .map(|e| format!("{operator} {}", serde_json::to_string(&e).unwrap()))
                .collect();

            if coll.is_empty() {
                None
            } else {
                Some(coll)
            }
        }
        DBResultType::Empty => None,
        DBResultType::Strings(s) => {
            panic!("unexpected string: {:?}", s);
        }
        _ => panic!("unexpected type: {:?}", missing),
    }
}

#[derive(Debug)]
struct RowSelector {
    matches: (DBResultType, DBResultType),
    missing: DBResultType,
    extra: DBResultType,
}
fn only_matching_ids(a: &DBResultType, b: &DBResultType) -> RowSelector {
    if let (DBResultType::JsonMaps(tmp_a), DBResultType::JsonMaps(_tmp_b)) = (a, b) {
        if let Some(first) = tmp_a.clone().first() {
            if first.contains_key("id") && first.get("id").unwrap().as_u64().is_some() {
                return group_by_id(a, b);
            }
        }
    };
    RowSelector {
        matches: (a.clone(), b.clone()),
        missing: DBResultType::Empty,
        extra: DBResultType::Empty,
    }
}

fn group_by_id(a: &DBResultType, b: &DBResultType) -> RowSelector {
    let mut btree: BTreeMap<u64, JsonMap> =
        b.to_h().into_iter().fold(BTreeMap::new(), |mut acc, data| {
            acc.insert(data.get("id").unwrap().as_u64().unwrap(), data);
            acc
        });
    let mut missing: Vec<JsonMap> = Vec::new();
    let acc: Vec<(JsonMap, JsonMap)> = a.to_h().into_iter().fold(Vec::new(), |mut acc, data| {
        let id = data.get("id").unwrap().as_u64().unwrap();

        if let Some(value) = btree.remove(&id) {
            acc.push((data, value));
        } else {
            missing.push(data);
        }
        acc
    });

    let extra = btree.into_values().collect::<Vec<JsonMap>>();

    RowSelector {
        matches: (DBResultType::GroupedRows(acc), DBResultType::Empty),
        missing: if missing.is_empty() {
            DBResultType::Empty
        } else {
            DBResultType::JsonMaps(missing)
        },
        extra: if extra.is_empty() {
            DBResultType::Empty
        } else {
            DBResultType::JsonMaps(extra)
        },
    }
}
fn normalize_input(list: &DBResultType) -> Result<String, serde_json::Error> {
    let list: String = match list {
        DBResultType::Strings(l) => {
            if l.len() == 1 {
                l[0].clone()
            } else {
                panic!("unexpected string: {:?}", l);
            }
        }
        _ => panic!("normalize_input({:?})", list),
    };
    Ok(list)
}

fn produce_diff(_config: &Config, json1: &str, json2: &str) -> Option<String> {
    if json1 == json2 {
        return None;
    }
    produce_char_diff(json1, json2)
}

fn produce_char_diff(old: &str, new: &str) -> Option<String> {
    use ansi_term::{Colour, Style};
    use prettydiff::{basic::DiffOp, diff_words};

    let style = Style::new().bold().on(Colour::Black).fg(Colour::Fixed(118));
    let diff = diff_words(old, new)
        .set_insert_style(style)
        .set_insert_whitespace_style(style);
    if diff.diff().len() == 1 {
        if let DiffOp::Equal(_) = diff.diff()[0] {
            return None;
        }
    }
    Some(format!("> {}", diff))
}
fn produce_simple_diff(json1: &str, json2: &str) -> Option<String> {
    if json1 == json2 {
        return None;
    }
    let diff = TextDiff::from_lines(json1, json2);
    let mut output = Vec::new();

    for change in diff.iter_all_changes() {
        if change.tag() == ChangeTag::Equal {
            continue;
        }
        let sign = match change.tag() {
            ChangeTag::Delete => "- ",
            ChangeTag::Insert => "+ ",
            ChangeTag::Equal => " ",
        };
        output.push(format!("{sign}{change}"));
    }
    Some(output.join(""))
}

// TODO: return option
// fn print_diff(result: &str) -> Option<String> {
//     match result {
//         "" => None,
//         diff => Some(diff.to_string()),
//     }
// }

fn print_diff(result: Option<String>) -> String {
    match result {
        None => "".to_string(),
        Some(diff) => diff,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_str, Map, Value};

    #[test]
    fn test_map() {
        let mut v = vec![Some(1), None, Some(2)];
        v.retain(|e| e.is_some());
        assert_eq!(v, vec![Some(1), Some(2)]);
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
            extra: _,
        } = only_matching_ids(
            &DBResultType::JsonMaps(vec![a.clone(), b.clone(), c.clone()]),
            &DBResultType::JsonMaps(vec![a.clone(), b, d]),
        );
        assert_eq!(result_a.to_gr()[0], (a.clone(), a));
        assert_eq!(result_b.to_h(), vec![]);
        assert_eq!(missing.to_h(), vec![c]);
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

use crate::{Config, DiffFormat};

use crate::database::{DBResultTypes, DBsResults, JsonMap};
use similar::{ChangeTag, TextDiff};
use std::collections::BTreeMap;
pub type FmtOutput = (
    Option<String>,
    Vec<String>,
    Option<Vec<String>>,
    Option<Vec<String>>,
);
pub fn call(config: &Config, result: DBsResults) -> Vec<FmtOutput> {
    let (header, a, b) = result;
    let (rows, missing, extra) = generate_diff(config, &a, &b);

    vec![(Some(header), rows, missing, extra)]
}

fn generate_diff(
    config: &Config,
    a: &DBResultTypes,
    b: &DBResultTypes,
) -> (Vec<String>, Option<Vec<String>>, Option<Vec<String>>) {
    let (rows, missing, extra) = match (a, b) {
        (DBResultTypes::Map(_a), DBResultTypes::Map(_b)) => normalize_map_type(config, a, b),

        _ => {
            let st = print_diff(&produce_simple_diff(
                &normalize_input(a).unwrap(),
                &normalize_input(b).unwrap(),
            ));
            (vec![st], None, None)
        }
    };
    (rows, missing, extra)
}

fn normalize_map_type(
    config: &Config,
    a: &DBResultTypes,
    b: &DBResultTypes,
) -> (Vec<String>, Option<Vec<String>>, Option<Vec<String>>) {
    let RowSelector {
        matches: (result_a, result_b),
        missing,
        extra,
    } = only_matching_ids(a, b);

    let rows = match result_a {
        DBResultTypes::Map(a) => a
            .into_iter()
            .zip(result_b.to_h())
            .map(|(a, b)| {
                print_diff(&produce_diff(
                    config,
                    &serde_json::to_string(&a).unwrap(),
                    &serde_json::to_string(&b).unwrap(),
                ))
            })
            .collect(),
        DBResultTypes::GroupedRows(a) => a
            .into_iter()
            // TODO: remove duplication
            .map(|(a, b)| {
                print_diff(&produce_diff(
                    config,
                    &serde_json::to_string(&a).unwrap(),
                    &serde_json::to_string(&b).unwrap(),
                ))
            })
            .collect(),
        _ => panic!("unexpected type: {:?}", result_a),
    };

    let missing = do_unmached_rows_format(&missing, "-");
    let extra = do_unmached_rows_format(&extra, "+");
    (rows, missing, extra)
}

fn do_unmached_rows_format(missing: &DBResultTypes, operator: &str) -> Option<Vec<String>> {
    match missing {
        DBResultTypes::Map(s) => {
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
        DBResultTypes::Empty => None,
        DBResultTypes::String(s) => {
            panic!("unexpected string: {:?}", s);
        }
        _ => panic!("unexpected type: {:?}", missing),
    }
}

#[derive(Debug)]
struct RowSelector {
    matches: (DBResultTypes, DBResultTypes),
    missing: DBResultTypes,
    extra: DBResultTypes,
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
        extra: DBResultTypes::Empty,
    }
}

fn group_by_id(a: &DBResultTypes, b: &DBResultTypes) -> RowSelector {
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
        matches: (DBResultTypes::GroupedRows(acc), DBResultTypes::Empty),
        missing: if missing.is_empty() {
            DBResultTypes::Empty
        } else {
            DBResultTypes::Map(missing)
        },
        extra: if extra.is_empty() {
            DBResultTypes::Empty
        } else {
            DBResultTypes::Map(extra)
        },
    }
}
fn normalize_input(list: &DBResultTypes) -> Result<String, serde_json::Error> {
    let list: String = match list {
        DBResultTypes::String(l) => {
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

fn produce_diff(config: &Config, json1: &str, json2: &str) -> String {
    match config.diff_format {
        DiffFormat::Char => produce_char_diff(json1, json2),
        DiffFormat::Simple => produce_simple_diff(json1, json2),
    }
}
fn produce_char_diff(old: &str, new: &str) -> String {
    use ansi_term::{Colour, Style};
    use prettydiff::{basic::DiffOp, diff_words};

    let style = Style::new().bold().on(Colour::Black).fg(Colour::Fixed(118));
    let diff = diff_words(old, new)
        .set_insert_style(style)
        .set_insert_whitespace_style(style);
    if diff.diff().len() == 1 {
        if let DiffOp::Equal(_) = diff.diff()[0] {
            return "".to_string();
        }
    }
    format!("> {}", diff)
}
fn produce_simple_diff(json1: &str, json2: &str) -> String {
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
    output.join("")
}

// TODO: return option
// fn print_diff(result: &str) -> Option<String> {
//     match result {
//         "" => None,
//         diff => Some(diff.to_string()),
//     }
// }

fn print_diff(result: &str) -> String {
    match result {
        "" => "".to_string(),
        diff => diff.to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_str, Map, Value};

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
            &DBResultTypes::Map(vec![a.clone(), b.clone(), c.clone()]),
            &DBResultTypes::Map(vec![a.clone(), b, d]),
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

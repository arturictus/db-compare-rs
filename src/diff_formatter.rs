use crate::{DBsResult, Diff};
use similar::{ChangeTag, TextDiff};

pub fn call(result: DBsResult) -> Diff {
    let (header, a, b) = result;
    let diff = print_diff(&produce_diff(&to_json(&a).unwrap(), &to_json(&b).unwrap()));
    (header, diff)
}

fn to_json(list: &Vec<String>) -> Result<std::string::String, serde_json::Error> {
    serde_json::to_string_pretty(list)
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
}

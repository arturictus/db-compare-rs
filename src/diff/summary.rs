use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

#[derive(Debug)]
pub struct Summary {
    pub table: String,
    pub updated: usize,
    pub deleted: usize,
    pub created: usize,
    pub updated_rows: Vec<u32>,
    pub updated_columns: HashMap<String, usize>,
}

const ANSI_CHARS: &str = r##"[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]+)*|[a-zA-Z\\d]+(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TZcf-nq-uy=><~]))"##;

impl Summary {
    fn new() -> Self {
        Self {
            table: "".to_string(),
            updated: 0,
            deleted: 0,
            created: 0,
            updated_rows: Vec::new(),
            updated_columns: HashMap::new(),
        }
    }

    fn map_line(&mut self, line: &str) {
        if line.contains("#start#") {
            let table = capture_table(line);
            self.table = table.unwrap();
        } else if line.starts_with("> ") {
            self.updated += 1;
            if let Some(id) = capture_id(line) {
                self.updated_rows.push(id);
            }
            if let Some(column_names) = capture_column_names(line) {
                for column_name in column_names {
                    let count = self.updated_columns.entry(column_name).or_insert(0);
                    *count += 1;
                }
            }
        } else if line.starts_with("- ") {
            self.deleted += 1;
        } else if line.starts_with("+ ") {
            self.created += 1;
        }
    }

    pub fn from_file(file_path: &str) -> Vec<Self> {
        let mut summaries = Vec::new();
        let mut summary = Summary::new();
        for line in read_lines(file_path).unwrap() {
            let line = line.unwrap();
            if line.contains("#start#") {
                summary.map_line(&line);
            } else if line.contains("#end#") {
                summaries.push(summary);
                summary = Summary::new();
            } else {
                summary.map_line(&line);
            }
        }
        summaries
    }

    pub fn print(&self) {
        println!("Summary:");
        println!("  Table: {}", self.table);
        println!("  Updated: {}", self.updated);
        println!("  Deleted: {}", self.deleted);
        println!("  Created: {}", self.created);
        println!("  Updated rows:");
        for id in &self.updated_rows {
            println!("    {id}");
        }
        println!("  Updated columns:");
        for (column, count) in &self.updated_columns {
            println!("    {column}: {count}");
        }
    }
}

fn capture_table(line: &str) -> Option<String> {
    let re = Regex::new(r"Table:\s`(?P<table>.+)`").unwrap();
    let caps = re.captures(line)?;
    caps.name("table").map(|m| m.as_str().to_string())
}

fn capture_column_names(line: &str) -> Option<Vec<String>> {
    let json_regex = Regex::new(r#""([^"]+)":([^,]+)"#).unwrap();
    let mut acc = Vec::new();
    for captures in json_regex.captures_iter(line) {
        match (captures.get(1), captures.get(2)) {
            (Some(k), Some(v)) => {
                let pair = (k.as_str(), v.as_str());
                let ansi_re = Regex::new(ANSI_CHARS).unwrap();
                if ansi_re.is_match(pair.1) {
                    acc.push(pair.0.to_string());
                }
            }
            _ => (),
        }
    }

    if acc.len() > 0 {
        Some(acc)
    } else {
        None
    }
}
fn capture_id(line: &str) -> Option<u32> {
    let re = Regex::new(r###"id":(?P<id>\d+)"###).unwrap();
    let caps = re.captures(line)?;
    if let Some(data) = caps.name("id") {
        data.as_str().parse::<u32>().ok()
    } else {
        None
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_summary() {
        let summaries = Summary::from_file("tests/fixtures/examples/outputs/users.diff");
        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        summary.print();
        assert_eq!(summary.table, "users");
        assert_eq!(summary.updated, 14);
        assert_eq!(summary.deleted, 13);
        assert_eq!(summary.created, 0);
        assert_eq!(summary.updated_rows.len(), 14);
        assert_eq!(summary.updated_columns.get("name").unwrap().clone(), 14);
        assert_eq!(
            summary.updated_columns.get("updated_at").unwrap().clone(),
            7
        );
    }
    #[test]
    fn test_extract_column_name() {
        let line = r##"> {"created_at":"2020-05-07T20:52:24","id":40,"name":"John-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I[1;40;38;5;118m [0m[1;40;38;5;118mchanged[0m","updated_at":"2020-[9;31m05[0m[1;40;38;5;118m06[0m-[9;31m07T20[0m[1;40;38;5;118m06T20[0m:52:24"}"##;
        let columns = capture_column_names(line).unwrap();
        assert_eq!(columns[0], "name".to_string());
        assert_eq!(columns[1], "updated_at".to_string());
    }
}

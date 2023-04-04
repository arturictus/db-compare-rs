use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
pub struct Summary {
    pub table: String,
    pub updated: usize,
    pub deleted: usize,
    pub created: usize,
    pub updated_rows: Vec<u32>,
}

impl Summary {
    pub fn new() -> Self {
        Self {
            table: "".to_string(),
            updated: 0,
            deleted: 0,
            created: 0,
            updated_rows: Vec::new(),
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
        } else if line.starts_with("- ") {
            self.deleted += 1;
        } else if line.starts_with("+ ") {
            self.created += 1;
        }
    }

    fn from_file(file_path: &str) -> Vec<Self> {
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

    // s

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
    }
}

fn capture_table(line: &str) -> Option<String> {
    let re = Regex::new(r"Table:\s`(?P<table>.+)`").unwrap();
    let caps = re.captures(line)?;
    caps.name("table").map(|m| m.as_str().to_string())
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
    }
}

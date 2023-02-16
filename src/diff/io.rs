use crate::diff::formatter;
use crate::{Args, DBsResults};
use std::fs::File;
use std::io::prelude::*;
use std::io::LineWriter;

#[derive(Debug)]
pub enum IOType {
    Stdout,
    File(LineWriter<File>),
}

pub trait IO {
    fn write(&mut self, result: DBsResults);
    fn close(&mut self);
    fn new(config: &Args) -> Self;
    fn new_from_path(file_path: Option<String>) -> Self;
    fn is_stdout(&self) -> bool;
}

impl IO for IOType {
    fn new(config: &Args) -> Self {
        match &config.diff_file {
            Some(file_path) => Self::File(new_file(file_path)),
            _ => Self::Stdout,
        }
    }
    fn new_from_path(file_path: Option<String>) -> Self {
        match file_path {
            Some(f) => Self::File(new_file(&f)),
            _ => Self::Stdout,
        }
    }
    fn write(&mut self, result: DBsResults) {
        let (header, diff) = formatter::call(result);
        match self {
            Self::File(file) => {
                write_to_file(file, &header);
                write_to_file(file, &diff);
            }
            _ => {
                println!("{header}");
                println!("{diff}");
            }
        }
    }

    fn close(&mut self) {
        if let Self::File(file) = self {
            flush_file(file);
        }
    }
    fn is_stdout(&self) -> bool {
        matches!(self, Self::Stdout)
    }
}

fn write_to_file(file: &mut LineWriter<File>, msg: &str) {
    file.write_all(msg.as_bytes()).unwrap();
    file.write_all(b"\n").unwrap();
}

fn flush_file(file: &mut LineWriter<File>) {
    file.flush().unwrap()
}

fn new_file(file_path: &String) -> LineWriter<File> {
    let file = File::create(file_path)
        .unwrap_or_else(|_| panic!("unable to create diff file at {file_path}"));
    LineWriter::new(file)
}

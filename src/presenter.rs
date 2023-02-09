use crate::diff_formatter;
use crate::{Args, DBsResult};
use std::fs::File;
use std::io::prelude::*;
use std::io::LineWriter;

pub struct Presenter {
    file: Option<LineWriter<File>>,
}

pub trait PresenterAbstract {
    fn call(&mut self, result: DBsResult);
    fn close(&mut self);
    fn new(config: &Args) -> Self;
}

impl PresenterAbstract for Presenter {
    fn new(config: &Args) -> Self {
        match &config.diff_file {
            Some(f) => {
                let file_path = f;
                let writer = Some(new_file(file_path));
                Self { file: writer }
            }
            _ => Self { file: None },
        }
    }
    fn call(&mut self, result: DBsResult) {
        let (header, diff) = diff_formatter::call(result);
        match &mut self.file {
            Some(file) => {
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
        if let Some(file) = &mut self.file {
            flush_file(file);
        }
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
    let file = File::create(file_path).unwrap();
    LineWriter::new(file)
}
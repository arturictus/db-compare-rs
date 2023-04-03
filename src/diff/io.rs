use crate::database::DBsResults;
use crate::diff::formatter::{self, FmtOutput};
use crate::Config;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::LineWriter;
use std::path::Path;

#[derive(Debug, Default)]
pub enum IOType<'a> {
    #[default]
    Stdout,
    File(LineWriter<File>),
    Test(Vec<String>),
    Phantom(&'a Config<'a>),
}

pub trait IO {
    fn write(&mut self, config: &Config, result: DBsResults);
    fn echo(&mut self, msg: &str);
    fn close(&mut self);
    fn new_from_path(file_path: String) -> Self;
    fn is_stdout(&self) -> bool;
    fn start_block(&mut self, msg: &str);
    fn end_block(&mut self, msg: &str);
    fn comment(&mut self, msg: &str);
    fn read(&self) -> String;
}

impl<'a> IO for IOType<'a> {
    fn new_from_path(file_path: String) -> Self {
        Self::File(new_file(&file_path))
    }
    fn write(&mut self, config: &Config, result: DBsResults) {
        if matches!(self, Self::Phantom(_)) {
            config.diff_io.borrow_mut().write(config, result);
            return;
        }
        let list = formatter::call(config, result);

        for fmt in list {
            let lines = generate_output(fmt);
            match self {
                Self::File(file) => {
                    for line in lines {
                        write_to_file(file, &line);
                    }
                }
                Self::Test(_test) => {
                    for line in lines {
                        if !line.is_empty() {
                            self.echo(&line);
                        }
                    }
                }
                _ => {
                    for line in lines {
                        println!("{line}");
                    }
                }
            }
        }
    }

    fn start_block(&mut self, msg: &str) {
        if let Self::Phantom(config) = self {
            config.diff_io.borrow_mut().start_block(msg);
            return;
        }
        if msg.is_empty() {
            return;
        }
        let msg = &format!("@@ #start# {msg} @@");
        self.echo(msg);
    }
    fn end_block(&mut self, msg: &str) {
        if let Self::Phantom(config) = self {
            config.diff_io.borrow_mut().end_block(msg);
            return;
        }
        if msg.is_empty() {
            return;
        }
        let msg = &format!("@@ {msg} #end# @@");
        self.echo(msg);
    }
    fn comment(&mut self, msg: &str) {
        if let Self::Phantom(config) = self {
            config.diff_io.borrow_mut().comment(msg);
            return;
        }
        if msg.is_empty() {
            return;
        }
        let msg = &format!("@@ {msg} @@");
        self.echo(msg);
    }

    fn echo(&mut self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        match self {
            Self::File(file) => write_to_file(file, msg),
            Self::Test(test) => test.push(msg.to_string()),
            _ => println!("{}", msg),
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

    fn read(&self) -> String {
        if let Self::Test(test) = self {
            test.join("\n")
        } else {
            todo!()
        }
    }
}

fn generate_output(fomatter: FmtOutput) -> Vec<String> {
    let (header, diff, missing, extra) = fomatter;
    let mut acc = Vec::new();
    if let Some(header) = header {
        acc.push(format!("@@ {header} @@"));
    }
    if diff.is_empty() && missing.is_none() && extra.is_none() {
        acc.push("@@ No diff @@".to_string());
    } else {
        for line in diff {
            acc.push(line);
        }
    }
    if let Some(missing) = missing {
        for line in missing {
            acc.push(line);
        }
    }
    if let Some(extra) = extra {
        for line in extra {
            acc.push(line);
        }
    }
    acc
}
fn write_to_file(file: &mut LineWriter<File>, msg: &str) {
    if !msg.is_empty() {
        file.write_all(msg.as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }
}

fn flush_file(file: &mut LineWriter<File>) {
    file.flush().unwrap()
}

// TODO: this should return a Result
fn new_file(file_path: &String) -> LineWriter<File> {
    let folder = Path::new(file_path).parent().unwrap();
    fs::create_dir_all(folder).unwrap_or_else(|_| {
        panic!("unable to create folder {folder:?}");
    });
    let file = File::create(file_path)
        .unwrap_or_else(|_| panic!("unable to create diff file at {file_path}"));
    LineWriter::new(file)
}

use crate::{diff_formatter, file_presenter};
use crate::{Args, DBsResult, Diff};
use std::fs::File;
use std::io::prelude::*;
use std::io::LineWriter;

pub struct Presenter {
    pub file: Option<LineWriter<File>>,
    use_file: bool,
}

impl Presenter {
    pub fn new(args: &Args) -> Self {
        if args.file.is_some() {
            let default_file_path = &"tmp/default.diff".to_string();
            let file_path = args.file.as_ref().unwrap_or(default_file_path);
            let writer = Some(file_presenter::new(&file_path));
            Self {
                file: writer,
                use_file: true,
            }
        } else {
            Self {
                file: None,
                use_file: false,
            }
        }
    }
    pub fn call(&mut self, result: DBsResult) {
        let (header, diff) = diff_formatter::call(result);
        if self.use_file {
            let mut file = self.file.as_mut().unwrap();
            file_presenter::call(&mut file, &header);
            file_presenter::call(&mut file, &diff);
        } else {
            println!("{}", header);
            println!("{}", diff);
        }
    }

    pub fn end(&mut self) {
        let file = self.file.as_mut().unwrap();
        file_presenter::flush(file);
    }
}

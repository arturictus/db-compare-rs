use std::fs::File;
use std::io::prelude::*;
use std::io::LineWriter;

pub fn call(file: &mut LineWriter<File>, msg: &str) {
    file.write_all(msg.as_bytes()).unwrap();
    file.write_all(b"\n").unwrap();
}

pub fn flush(file: &mut LineWriter<File>) {
    file.flush().unwrap()
}

pub fn new(file_path: &String) -> LineWriter<File> {
    let file = File::create(file_path).unwrap();
    LineWriter::new(file)
}

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use similar::{ChangeTag, TextDiff};

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

fn simple_compare(a: &str, b: &str) -> bool {
    a == b
}

fn criterion_benchmark(c: &mut Criterion) {
    let old = r#"{"created_at":"2020-05-07T22:52:24","id":41,"name":"John-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I","updated_at":"2020-05-07T22:52:24"}"#;
    let new = r#"{"created_at":"2020-05-07T22:52:24","id":41,"name":"John-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I-I","updated_at":"2020-05-07T22:52:24"}"#;
    c.bench_function("char_diff", |b| {
        b.iter(|| produce_char_diff(black_box(old), black_box(new)))
    });
    c.bench_function("simple_diff", |b| {
        b.iter(|| produce_simple_diff(black_box(old), black_box(new)))
    });
    c.bench_function("comparison", |b| {
        b.iter(|| simple_compare(black_box(old), black_box(new)))
    });
    c.bench_function("char_diff eq", |b| {
        b.iter(|| produce_char_diff(black_box(old), black_box(old)))
    });
    c.bench_function("simple_diff eq", |b| {
        b.iter(|| produce_simple_diff(black_box(old), black_box(old)))
    });
    c.bench_function("comparison eq", |b| {
        b.iter(|| simple_compare(black_box(old), black_box(old)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

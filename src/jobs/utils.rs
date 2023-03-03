use crate::database::{Request, RequestBuilder};

pub fn par_run<T: Send>(
    r: RequestBuilder,
    f: fn(Request) -> Result<T, postgres::Error>,
) -> Result<(T, T), postgres::Error> {
    let (result1, result2) = rayon::join(|| f(r.build_master()), || f(r.build_replica()));

    Ok((result1?, result2?))
}

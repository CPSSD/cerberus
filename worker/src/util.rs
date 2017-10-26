use errors::*;

pub fn output_error(err: &Error) {
    error!("{}", err);

    for e in err.iter().skip(1) {
        error!("caused by: {}", e);
    }

    if let Some(backtrace) = err.backtrace() {
        error!("backtrace: {:?}", backtrace);
    }
}

use std::fmt::Display;

pub trait WrapError: Sized {
    fn wrap<C: Display + Send + Sync + 'static>(self, c: C) -> Self {
        self.with_wrap(|| c)
    }
    fn with_wrap<C: Display + Send + Sync + 'static, F: FnOnce() -> C>(self, f: F) -> Self;
}

impl<T, E: WrapError> WrapError for Result<T, E> {
    fn with_wrap<C: Display + Send + Sync + 'static, F: FnOnce() -> C>(self, f: F) -> Self {
        self.map_err(|err| err.with_wrap(f))
    }
}

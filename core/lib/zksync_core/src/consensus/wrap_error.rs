use std::fmt::Display;

pub trait WrapError: Sized {
    fn wrap<C: Display + Send + Sync + 'static>(self, c: C) -> Self {
        self.with_wrap(|| c)
    }
    fn with_wrap<C: Display + Send + Sync + 'static, F: FnOnce() -> C>(self, f: F) -> Self;
}

impl WrapError for anyhow::Error {
    fn with_wrap<C: Display + Send + Sync + 'static, F: FnOnce() -> C>(self, f: F) -> Self {
        anyhow::Context::with_context(Err::<(), _>(anyhow::Error::from(self)), f)
            .err()
            .unwrap()
    }
}

impl<T, E: WrapError> WrapError for Result<T, E> {
    fn with_wrap<C: Display + Send + Sync + 'static, F: FnOnce() -> C>(self, f: F) -> Self {
        self.map_err(|err| err.with_wrap(f))
    }
}

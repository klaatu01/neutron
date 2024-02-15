use async_trait::async_trait;
use std::future::Future;

#[async_trait]
pub(crate) trait AsyncMap<T, U, F, FU>
where
    F: FnOnce(T) -> FU + Send,
    FU: Future<Output = U> + Send,
{
    type Output;
    async fn async_map(self, map: F) -> Self::Output;
}

#[async_trait]
impl<T, U, F, FU> AsyncMap<T, U, F, FU> for Option<T>
where
    T: Send,
    U: Send,
    F: 'static + FnOnce(T) -> FU + Send,
    FU: Future<Output = U> + Send,
{
    type Output = Option<U>;
    async fn async_map(self, map: F) -> Self::Output {
        match self {
            Some(t) => {
                let u = map(t).await;
                Some(u)
            }
            None => None,
        }
    }
}

#[async_trait]
impl<T, U, F, FU> AsyncMap<T, U, F, FU> for Result<T, U>
where
    T: Send,
    U: Send,
    F: 'static + FnOnce(T) -> FU + Send,
    FU: Future<Output = U> + Send,
{
    type Output = Result<U, U>;
    async fn async_map(self, map: F) -> Self::Output {
        match self {
            Ok(t) => {
                let u = map(t).await;
                Ok(u)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
pub trait AsyncAndThen<T, U, F, FU>
where
    F: FnOnce(T) -> FU + Send,
    FU: Future<Output = U> + Send,
{
    type Output;
    async fn async_and_then(self, f: F) -> Self::Output;
}

#[async_trait]
impl<T, U, F, FU> AsyncAndThen<T, Option<U>, F, FU> for Option<T>
where
    T: Send,
    U: Send,
    F: 'static + FnOnce(T) -> FU + Send,
    FU: Future<Output = Option<U>> + Send,
{
    type Output = Option<U>;

    async fn async_and_then(self, f: F) -> Self::Output {
        match self {
            Some(t) => f(t).await,
            None => None,
        }
    }
}

#[async_trait]
impl<T, E, U, F, FU> AsyncAndThen<T, Result<U, E>, F, FU> for Result<T, E>
where
    T: Send,
    E: Send + Sync, // Added Sync to allow error to be sent across threads if needed
    U: Send,
    F: 'static + FnOnce(T) -> FU + Send,
    FU: Future<Output = Result<U, E>> + Send,
{
    type Output = Result<U, E>;

    async fn async_and_then(self, f: F) -> Self::Output {
        match self {
            Ok(t) => f(t).await,
            Err(e) => Err(e),
        }
    }
}

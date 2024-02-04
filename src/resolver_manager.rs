use std::collections::HashMap;

use futures::lock::Mutex;
use tokio::time::Instant;

type Comparison<T> = Box<dyn Fn(&T) -> bool + Send + 'static>;

#[allow(dead_code)]
enum Resolver<T>
where
    T: Resolvable + Clone + Send + 'static,
{
    Comparison(Comparison<T>, async_channel::Sender<T>),
    Basic(async_channel::Sender<T>),
}

pub type ResolverKey = String;

pub trait Resolvable {
    fn resolver_id(&self) -> Option<ResolverKey>;
}

pub struct ResolverManager<T: Resolvable + Clone + Send + 'static> {
    map: Mutex<HashMap<String, (Resolver<T>, Instant)>>,
}

impl<T: Resolvable + Clone + Send + 'static> ResolverManager<T> {
    pub fn new() -> Self {
        ResolverManager {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_resolver<K>(&self, resolve: &K) -> Option<async_channel::Receiver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = async_channel::bounded(1);
                self.map.lock().await.insert(
                    resolver_id.to_string(),
                    (Resolver::Basic(tx), Instant::now()),
                );
                Some(rx)
            }
            None => None,
        }
    }

    #[allow(dead_code)]
    pub async fn put_comparison_resolver<K>(
        &self,
        resolve: &K,
        comparison: Comparison<T>,
    ) -> Option<async_channel::Receiver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = async_channel::bounded(1);
                self.map.lock().await.insert(
                    resolver_id.to_string(),
                    (
                        Resolver::Comparison(Box::new(comparison), tx),
                        Instant::now(),
                    ),
                );
                Some(rx)
            }
            None => None,
        }
    }

    pub async fn try_resolve(&self, resolvable: &T) -> bool {
        if let Some(resolver_id) = resolvable.resolver_id() {
            let mut map = self.map.lock().await;
            let resolver = map.remove(&resolver_id);
            match resolver {
                Some((Resolver::Comparison(comparison, tx), instant)) => {
                    if comparison(resolvable) {
                        let _ = tx.send(resolvable.clone()).await;
                    };
                    log::debug!("{} in {:?}", resolver_id, instant.elapsed());
                    true
                }
                Some((Resolver::Basic(tx), instant)) => {
                    let _ = tx.send(resolvable.clone()).await;
                    log::debug!("{} in {:?}", resolver_id, instant.elapsed());
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }
}

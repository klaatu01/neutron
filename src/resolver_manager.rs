use std::collections::HashMap;

use futures::lock::Mutex;

type Comparison<T> = Box<dyn Fn(&T) -> bool + Send + 'static>;

enum Resolver<T>
where
    T: Resolvable + Clone + Send + 'static,
{
    Comparison(Comparison<T>, futures::channel::oneshot::Sender<T>),
    Basic(futures::channel::oneshot::Sender<T>),
}

pub type ResolverKey = String;

pub trait Resolvable {
    fn resolver_id(&self) -> Option<ResolverKey>;
}

pub struct ResolverManager<T: Resolvable + Clone + Send + 'static> {
    map: Mutex<HashMap<String, Resolver<T>>>,
}

impl<T: Resolvable + Clone + Send + 'static> ResolverManager<T> {
    pub fn new() -> Self {
        ResolverManager {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_resolver<K>(
        &self,
        resolve: &K,
    ) -> Option<futures::channel::oneshot::Receiver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = futures::channel::oneshot::channel();
                self.map
                    .lock()
                    .await
                    .insert(resolver_id.to_string(), Resolver::Basic(tx));
                Some(rx)
            }
            None => None,
        }
    }

    pub async fn put_comparison_resolver<K>(
        &self,
        resolve: &K,
        comparison: Comparison<T>,
    ) -> Option<futures::channel::oneshot::Receiver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = futures::channel::oneshot::channel();
                self.map.lock().await.insert(
                    resolver_id.to_string(),
                    Resolver::Comparison(Box::new(comparison), tx),
                );
                Some(rx)
            }
            None => None,
        }
    }

    pub async fn get_receipt(&self, resolvable: &T) {
        if let Some(resolver_id) = resolvable.resolver_id() {
            let mut map = self.map.lock().await;
            let resolver = map.remove(&resolver_id);
            match resolver {
                Some(Resolver::Comparison(comparison, tx)) => {
                    if comparison(resolvable) {
                        let _ = tx.send(resolvable.clone());
                    };
                }
                Some(Resolver::Basic(tx)) => {
                    let _ = tx.send(resolvable.clone());
                }
                _ => (),
            }
        }
    }
}

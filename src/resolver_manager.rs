use std::collections::HashMap;

use futures::lock::Mutex;
use tokio::time::Instant;

type Comparison<T> = Box<dyn Fn(&T) -> bool + Send + 'static>;

#[allow(dead_code)]
enum ResolverSender<T>
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
    map: Mutex<HashMap<String, (ResolverSender<T>, Instant)>>,
}

pub type Resolver<T> = async_channel::Receiver<T>;

impl<T: Resolvable + Clone + Send + 'static> ResolverManager<T> {
    pub fn new() -> Self {
        ResolverManager {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_resolver<K>(&self, resolve: &K) -> Option<Resolver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = async_channel::bounded(1);
                self.map.lock().await.insert(
                    resolver_id.to_string(),
                    (ResolverSender::Basic(tx), Instant::now()),
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
    ) -> Option<Resolver<T>>
    where
        K: Resolvable,
    {
        match resolve.resolver_id() {
            Some(resolver_id) => {
                let (tx, rx) = async_channel::bounded(1);
                self.map.lock().await.insert(
                    resolver_id.to_string(),
                    (
                        ResolverSender::Comparison(Box::new(comparison), tx),
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
                Some((ResolverSender::Comparison(comparison, tx), instant)) => {
                    if comparison(resolvable) {
                        let _ = tx.send(resolvable.clone()).await;
                    };
                    log::debug!("{} in {:?}", resolver_id, instant.elapsed());
                    true
                }
                Some((ResolverSender::Basic(tx), instant)) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestResolver {
        id: String,
    }

    impl Resolvable for TestResolver {
        fn resolver_id(&self) -> Option<ResolverKey> {
            Some(self.id.clone())
        }
    }

    #[tokio::test]
    async fn test_resolver_manager() {
        let manager = ResolverManager::new();
        let resolver = TestResolver {
            id: "test".to_string(),
        };
        let rx = manager.put_resolver(&resolver).await.unwrap();
        let resolver = TestResolver {
            id: "test".to_string(),
        };
        assert!(manager.try_resolve(&resolver).await);
        assert_eq!(rx.recv().await.unwrap().id, "test");
    }

    #[tokio::test]
    async fn test_multiple_resolver_managers() {
        let manager: ResolverManager<TestResolver> = ResolverManager::new();
        let mut resolvers = Vec::new();
        for i in 0..10000 {
            let resolver = TestResolver {
                id: format!("test{}", i),
            };
            let rx = manager.put_resolver(&resolver).await.unwrap();
            resolvers.push(rx);
        }

        let resolvers = futures::future::join_all(
            resolvers
                .into_iter()
                .map(|rx| async move { rx.recv().await }),
        );

        tokio::join!(resolvers, async {
            for i in 0..10000 {
                let resolver = TestResolver {
                    id: format!("test{}", i),
                };
                assert!(manager.try_resolve(&resolver).await);
            }
        });

        assert!(true);
    }
}

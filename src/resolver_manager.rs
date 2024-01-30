use std::collections::HashMap;

use futures::lock::Mutex;

use crate::message::{EngineInbound, Inbound};

enum Resolver {
    Comparison(
        Box<dyn Fn(&EngineInbound) -> bool>,
        futures::channel::oneshot::Sender<()>,
    ),
    Basic(futures::channel::oneshot::Sender<()>),
}

pub type ResolverKey = String;

pub trait Resolvable {
    fn resolve_id(&self) -> Option<ResolverKey>;
}

pub struct ResolverManager {
    map: Mutex<HashMap<String, Resolver>>,
}

impl ResolverManager {
    pub fn new() -> Self {
        ResolverManager {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn put_resolver<K>(
        &self,
        resolve: K,
    ) -> Option<futures::channel::oneshot::Receiver<()>>
    where
        K: Resolvable,
    {
        let resolver_id = resolve.resolver_id();
        let (tx, rx) = futures::channel::oneshot::channel();
        self.map
            .lock()
            .await
            .insert(resolver_id.to_string(), Resolver::Basic(tx));
        rx
    }

    pub async fn put_comparison_resolver<K, T>(
        &self,
        resolve: K,
        comparison: T,
    ) -> Option<futures::channel::oneshot::Receiver<()>>
    where
        K: Resolvable,
        T: Fn(&Inbound) -> bool + 'static,
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        let resolver_id = resolve.resolver_id();
        self.map.lock().await.insert(
            resolver_id.to_string(),
            Resolver::Comparison(Box::new(comparison), tx),
        );
        rx
    }

    pub async fn get_receipt<K>(&self, resolve: K, engine_inbound: &EngineInbound)
    where
        K: Resolvable,
    {
        if let Some(resolver_id) = resolve.resolver_id() {
            let mut map = self.map.lock().await;
            let resolver = map.remove(resolver_id);
            match resolver {
                Some(Resolver::Comparison(comparison, tx)) => {
                    if comparison(engine_inbound) {
                        let _ = tx.send(());
                    };
                }
                Some(Resolver::Basic(tx)) => {
                    let _ = tx.send(());
                }
                _ => (),
            }
        }
    }
}

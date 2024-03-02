use futures::lock::Mutex;

use crate::NeutronError;
pub trait ResolverKey {
    fn try_key(&self) -> Option<String>;
}

pub struct CommandResolver<Outbound, Inbound> {
    hash_map: Mutex<
        std::collections::HashMap<
            String,
            futures::channel::oneshot::Sender<Result<Inbound, NeutronError>>,
        >,
    >,
    _phantom_data: std::marker::PhantomData<Outbound>,
}

impl<Outbound, Inbound> CommandResolver<Outbound, Inbound>
where
    Outbound: ResolverKey,
    Inbound: ResolverKey,
{
    pub fn new() -> Self {
        Self {
            hash_map: std::collections::HashMap::new(),
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub async fn put(
        &self,
        outbound: Outbound,
        value: futures::channel::oneshot::Sender<Result<Inbound, NeutronError>>,
    ) {
        if let Some(key) = outbound.try_key() {
            self.hash_map.lock().await.insert(key, value);
        }
    }

    pub async fn try_resolve(&self, inbound: Inbound) -> bool {
        if let Some(key) = inbound.try_key() {
            if let Some(tx) = self.hash_map.lock().await.remove(&key) {
                let _ = tx.send(Ok(inbound));
            }
            return true;
        }
        false
    }
}

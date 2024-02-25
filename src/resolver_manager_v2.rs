use async_channel::{unbounded, Receiver, Sender};
use futures::lock::Mutex;
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

pub trait Resolvable: Any + Send + Sync {
    fn resolve_id(&self) -> String;
}

pub trait ResolvesInto<Output>: Resolvable {}

pub struct ResolverManagerV2 {
    resolvers: Mutex<HashMap<String, Sender<Box<dyn Any + Send + Sync>>>>,
}

pub struct Resolver<Output: Any + Send + Sync> {
    rx: Receiver<Box<dyn Any + Send + Sync>>,
    _phantom: PhantomData<Output>,
}

impl<Output: Any + Send + Sync + Clone> Resolver<Output> {
    pub async fn resolve(&self) -> Option<Output> {
        if let Ok(resolvable) = self.rx.recv().await {
            if let Ok(output) = resolvable.downcast::<Output>() {
                return Some(*output);
            }
        }
        None
    }
}

impl ResolverManagerV2 {
    pub fn new() -> Self {
        Self {
            resolvers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_resolver<Output: Resolvable, Input: Resolvable + ResolvesInto<Output>>(
        &self,
        input: Input,
    ) -> Resolver<Output> {
        let (tx, rx) = unbounded();
        let mut resolvers = self.resolvers.lock().await;
        resolvers.insert(input.resolve_id(), tx as Sender<Box<dyn Any + Send + Sync>>);
        Resolver {
            rx,
            _phantom: PhantomData,
        }
    }

    pub async fn resolve<Output: Resolvable + Any + Send + Sync>(&self, output: Output) {
        let mut resolvers = self.resolvers.lock().await;
        if let Some(tx) = resolvers.remove(&output.resolve_id()) {
            tx.send(Box::new(output) as Box<dyn Any + Send + Sync>)
                .await
                .expect("Failed to send resolved output");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Resolvable, ResolverManagerV2, ResolvesInto};

    #[derive(Clone, Debug)]
    struct A {
        a: i32,
    }

    impl Resolvable for A {
        fn resolve_id(&self) -> String {
            self.a.to_string()
        }
    }

    #[derive(Clone, Debug)]
    struct B {
        b: i32,
    }

    impl Resolvable for B {
        fn resolve_id(&self) -> String {
            self.b.to_string()
        }
    }

    #[derive(Clone, Debug)]
    struct C {
        c: i32,
    }

    impl Resolvable for C {
        fn resolve_id(&self) -> String {
            self.c.to_string()
        }
    }

    impl ResolvesInto<B> for A {}
    impl ResolvesInto<C> for B {}

    #[tokio::test]
    async fn test_resolver_manager_v2() {
        env_logger::init();
        let manager = ResolverManagerV2::new();

        let a = A { a: 1 };
        let b_1 = B { b: 1 };
        let b_2 = B { b: 2 };
        let c = C { c: 2 };

        let resolver_b = manager.get_resolver(a).await;
        let resolver_c = manager.get_resolver(b_2).await;

        manager.resolve::<B>(b_1).await;
        manager.resolve::<C>(c).await;

        assert_eq!(resolver_b.resolve().await.unwrap().b, 1);
        assert_eq!(resolver_c.resolve().await.unwrap().c, 2);
    }
}

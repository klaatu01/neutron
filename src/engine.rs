use async_trait::async_trait;

use crate::error::NeutronError;

#[async_trait]
pub trait Engine<Input, Output> {
    async fn run(mut self) -> EngineConnection<Output, Input>;
}

pub struct EngineConnection<Input, Output> {
    tx: async_channel::Sender<Result<Input, NeutronError>>,
    rx: async_channel::Receiver<Result<Output, NeutronError>>,
}

impl<Input, Output> EngineConnection<Input, Output> {
    pub fn new(
        tx: async_channel::Sender<Result<Input, NeutronError>>,
        rx: async_channel::Receiver<Result<Output, NeutronError>>,
    ) -> Self {
        Self { tx, rx }
    }

    pub fn pair() -> (
        EngineConnection<Input, Output>,
        EngineConnection<Output, Input>,
    ) {
        let (tx1, rx1) = async_channel::unbounded();
        let (tx2, rx2) = async_channel::unbounded();
        (
            EngineConnection::new(tx1, rx2),
            EngineConnection::new(tx2, rx1),
        )
    }
}

impl<Input, Output> EngineConnection<Input, Output> {
    pub async fn send(&self, event: Result<Input, NeutronError>) -> Result<(), NeutronError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| NeutronError::ChannelTerminated)
    }

    pub async fn recv(&self) -> Result<Output, NeutronError> {
        self.rx
            .recv()
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?
    }
}

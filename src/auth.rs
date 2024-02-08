use crate::NeutronError;
use async_trait::async_trait;
use oauth2::{basic::BasicClient, AuthUrl, ClientId, ClientSecret, TokenResponse};

#[async_trait]
pub trait AuthenticationPlugin {
    async fn auth_data(&self) -> Result<Vec<u8>, NeutronError>;
}

pub struct OAuth2 {
    pub client: BasicClient,
}

impl OAuth2 {
    pub fn new(client_id: String, client_secret: String, token_url: String) -> Self {
        let token_url = AuthUrl::new(token_url).expect("Invalid authorization URL");
        let client_id = ClientId::new(client_id);
        let client_secret = ClientSecret::new(client_secret);
        Self {
            client: BasicClient::new(client_id, Some(client_secret), token_url, None),
        }
    }
}

#[async_trait]
impl AuthenticationPlugin for OAuth2 {
    // perform OAuth2 flow and return the token as bytes
    async fn auth_data(&self) -> Result<Vec<u8>, NeutronError> {
        let token = self
            .client
            .exchange_client_credentials()
            .request_async(oauth2::reqwest::async_http_client)
            .await
            .map_err(|e| NeutronError::AuthenticationFailed(e.to_string()))?;
        let token = token.access_token().secret().as_bytes().to_vec();
        Ok(token)
    }
}

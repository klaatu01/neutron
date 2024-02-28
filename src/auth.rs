use crate::NeutronError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::lock::Mutex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[async_trait]
pub trait AuthenticationPlugin {
    fn auth_method_name(&self) -> String;
    async fn auth_data(&self) -> Result<Vec<u8>, NeutronError>;
}

#[derive(Serialize)]
struct OAuth2TokenRequest {
    client_id: String,
    scope: String,
    client_secret: String,
    grant_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    audience: Option<String>,
}

#[derive(Deserialize)]
struct OAuth2TokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_on: DateTime<Utc>,
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        Utc::now() > self.expires_on
    }
}

pub struct ClientCredentialsOAuth2 {
    client_id: String,
    client_secret: String,
    authority: String,
    scope: String,
    audience: Option<String>,
    cached_token: Mutex<Option<CachedToken>>,
}

impl ClientCredentialsOAuth2 {
    pub fn new(
        client_id: String,
        client_secret: String,
        authority: String,
        scopes: Vec<String>,
        audience: Option<String>,
    ) -> Self {
        ClientCredentialsOAuth2 {
            client_id,
            client_secret,
            authority,
            scope: scopes.join(" "),
            audience,
            cached_token: Mutex::new(None),
        }
    }

    pub async fn get_auth_data(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut cached_token = self.cached_token.lock().await;

        if let Some(cached_token) = cached_token.as_ref() {
            if !cached_token.is_expired() {
                return Ok(cached_token.access_token.clone().into_bytes());
            }
        }

        let token_response = self.request_new_token().await?;
        let token = CachedToken {
            access_token: token_response.access_token.clone(),
            expires_on: Utc::now() + chrono::Duration::seconds(token_response.expires_in),
        };
        *cached_token = Some(token.clone());
        Ok(token.access_token.clone().into_bytes())
    }

    async fn request_new_token(&self) -> Result<OAuth2TokenResponse, Box<dyn Error>> {
        let client = Client::new();
        let res = client
            .post(format!("{}/oauth2/v2.0/token", &self.authority))
            .form(&OAuth2TokenRequest {
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
                grant_type: "client_credentials".to_string(),
                scope: self.scope.clone(),
                audience: self.audience.clone(),
            })
            .send()
            .await?
            .json::<OAuth2TokenResponse>()
            .await?;

        log::info!("Received token: {:?}", res.access_token);

        Ok(res)
    }
}

#[async_trait]
impl AuthenticationPlugin for ClientCredentialsOAuth2 {
    async fn auth_data(&self) -> Result<Vec<u8>, NeutronError> {
        self.get_auth_data()
            .await
            .map_err(|e| NeutronError::AuthenticationFailed(e.to_string()))
    }

    fn auth_method_name(&self) -> String {
        "token".to_string()
    }
}

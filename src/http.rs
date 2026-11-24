use crate::{
    amq::Amq,
    server::{MailWatch, Server},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    path::{Path, PathBuf},
    sync::Arc,
};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

type McpService = StreamableHttpService<Server, LocalSessionManager>;

struct Identity {
    token: Vec<u8>,
    service: McpService,
}

pub struct HttpServer {
    pub url: String,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl HttpServer {
    pub async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub async fn start_http(port: u16, tokens: &Path, root: Option<PathBuf>) -> Result<HttpServer> {
    let configured = serde_json::from_slice(
        &tokio::fs::read(tokens)
            .await
            .context("Could not read HTTP tokens file")?,
    )
    .context("Tokens must be a JSON object of handle to token.")?;
    start_http_with_credentials(port, configured, root).await
}

pub async fn start_http_with_credentials(
    port: u16,
    configured: Value,
    root: Option<PathBuf>,
) -> Result<HttpServer> {
    let credentials = validate_credentials(&configured)?;
    let cancellation = CancellationToken::new();
    let mut identities = Vec::new();
    let mut watchers: HashMap<PathBuf, Arc<MailWatch>> = HashMap::new();
    for (me, token) in credentials {
        let amq = Amq::connect(root.clone(), Some(me)).await?;
        let mail = match watchers.entry(amq.root().to_owned()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry.insert(Arc::new(MailWatch::new(amq.root())?)).clone(),
        };
        let server = Server::with_mail_watch(amq, mail);
        let service = StreamableHttpService::new(
            move || Ok(server.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_allowed_origins([
                    "http://localhost:*",
                    "http://127.0.0.1:*",
                    "http://[::1]:*",
                    "https://localhost:*",
                    "https://127.0.0.1:*",
                    "https://[::1]:*",
                ])
                .with_cancellation_token(cancellation.clone()),
        );
        identities.push(Identity {
            token: token.into_bytes(),
            service,
        });
    }
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let url = format!("http://127.0.0.1:{}/mcp", listener.local_addr()?.port());
    let app = Router::new()
        .fallback(dispatch)
        .with_state(Arc::new(identities));
    let shutdown = cancellation.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    eprintln!("amq-mcp: {url}");
    Ok(HttpServer {
        url,
        cancellation,
        task: Some(task),
    })
}

async fn dispatch(State(identities): State<Arc<Vec<Identity>>>, request: Request) -> Response {
    if !valid_host(request.headers()) || !valid_origin(request.headers()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if request
        .uri()
        .path_and_query()
        .is_none_or(|path| path.as_str() != "/mcp")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "));
    let mut caller = None;
    if let Some(token) = authorization {
        for identity in identities.iter() {
            if token.len() == identity.token.len()
                && bool::from(token.as_bytes().ct_eq(&identity.token))
            {
                caller = Some(identity);
            }
        }
    }
    let Some(identity) = caller else {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
        )
            .into_response();
    };
    match identity.service.clone().oneshot(request).await {
        Ok(response) => response.map(Body::new),
        Err(error) => match error {},
    }
}

fn validate_credentials(configured: &Value) -> Result<Vec<(String, String)>> {
    let object = configured
        .as_object()
        .context("Tokens must be a JSON object of handle to token.")?;
    ensure!(
        !object.is_empty(),
        "Each token entry needs a valid AMQ handle and a token of at least 32 characters."
    );
    let mut seen = HashSet::new();
    let mut credentials = Vec::new();
    for (me, token) in object {
        let mut bytes = me.bytes();
        let valid_handle = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte));
        let token = token
            .as_str()
            .filter(|token| token.encode_utf16().count() >= 32);
        ensure!(
            valid_handle && token.is_some(),
            "Each token entry needs a valid AMQ handle and a token of at least 32 characters."
        );
        let token = token.expect("validated token");
        ensure!(
            seen.insert(token),
            "Each identity must have a distinct token."
        );
        credentials.push((me.clone(), token.to_owned()));
    }
    Ok(credentials)
}

fn valid_host(headers: &HeaderMap) -> bool {
    if headers.get_all(header::HOST).iter().count() != 1 {
        return false;
    }
    headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .and_then(|host| host.parse::<axum::http::uri::Authority>().ok())
        .is_some_and(|host| {
            matches!(
                host.host().to_ascii_lowercase().as_str(),
                "localhost" | "127.0.0.1" | "[::1]"
            )
        })
}

fn valid_origin(headers: &HeaderMap) -> bool {
    if !headers.contains_key(header::ORIGIN) {
        return true;
    }
    if headers.get_all(header::ORIGIN).iter().count() != 1 {
        return false;
    }
    headers
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok())
        .and_then(|origin| reqwest::Url::parse(origin).ok())
        .is_some_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_configuration_rejects_ambiguous_identities() {
        let token = "x".repeat(40);
        assert!(validate_credentials(&json!({"alice":token})).is_ok());
        for value in [
            json!([]),
            json!({}),
            json!({"../bob":token}),
            json!({"bob":"short"}),
            json!({"alice":token,"bob":token}),
        ] {
            assert!(validate_credentials(&value).is_err());
        }
    }

    #[test]
    fn host_and_origin_are_loopback_only() {
        let mut headers = HeaderMap::new();
        assert!(!valid_host(&headers));
        headers.insert(header::HOST, "127.0.0.1:8765".parse().unwrap());
        assert!(valid_host(&headers));
        assert!(valid_origin(&headers));
        for origin in [
            "https://untrusted.example",
            "null",
            "http://localhost.attacker.example",
            "http://localhost@attacker.example",
        ] {
            headers.insert(header::ORIGIN, origin.parse().unwrap());
            assert!(!valid_origin(&headers));
        }
        headers.insert(header::ORIGIN, "http://localhost:3000".parse().unwrap());
        assert!(valid_origin(&headers));
        headers.insert(header::HOST, "untrusted.example".parse().unwrap());
        assert!(!valid_host(&headers));
    }
}

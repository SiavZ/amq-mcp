use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use rmcp::{
    ClientLifecycleMode, ClientServiceExt, RoleClient,
    model::{
        CallToolRequestParams, ProtocolVersion, ReadResourceRequestParams, SubscriptionFilter,
    },
    service::{Peer, RunningService},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(15);
const VERSION: &str = "2026-07-28";

fn token(handle: &str) -> String {
    handle.repeat(40)
}

struct Fixture {
    directory: TempDir,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let fixture = Self {
            directory: tempfile::tempdir()?,
        };
        let output = fixture
            .command("amq")
            .args(["init", "--root"])
            .arg(fixture.root())
            .args(["--agents", "alice,bob,charlie"])
            .output()
            .await?;
        assert!(
            output.status.success(),
            "amq init: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::fs::write(
            fixture.root().join("tokens.json"),
            serde_json::to_vec(
                &json!({"alice": token("a"), "bob": token("b"), "charlie": token("c")}),
            )?,
        )?;
        Ok(fixture)
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn command(&self, executable: &str) -> Command {
        let mut command = Command::new(executable);
        command
            .current_dir(self.root())
            .env("AM_ROOT", self.root())
            .env("AM_BASE_ROOT", self.root())
            .env("AMQ_BIN", "amq")
            .env("AMQ_NO_UPDATE_CHECK", "1")
            .env("AMQ_WAKE_NO_SELF_UPGRADE", "1");
        for key in [
            "AM_ROOT_ID",
            "AM_BASE_ROOT_ID",
            "AM_SESSION",
            "AM_ME",
            "AMQ_GLOBAL_ROOT",
            "YARAN_SESSION_ID",
            "AMQ_MCP_TOKEN",
            "AMQ_MCP_TOKENS",
            "AMQ_MCP_URL",
        ] {
            command.env_remove(key);
        }
        command.kill_on_drop(true);
        command
    }

    fn binary(&self) -> Command {
        self.command(env!("CARGO_BIN_EXE_amq-mcp"))
    }

    async fn http(&self, port: u16) -> Result<Http> {
        let mut child = self
            .binary()
            .args(["--http", &port.to_string(), "--tokens"])
            .arg(self.root().join("tokens.json"))
            .arg("--root")
            .arg(self.root())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut lines = BufReader::new(child.stderr.take().context("HTTP stderr")?).lines();
        let url = timeout(WAIT, async {
            let mut diagnostics = String::new();
            while let Some(line) = lines.next_line().await? {
                if let Some(url) = line.strip_prefix("amq-mcp: http://") {
                    return Ok::<_, anyhow::Error>(format!("http://{url}"));
                }
                diagnostics.push_str(&line);
                diagnostics.push('\n');
            }
            bail!("HTTP process exited before startup: {diagnostics}")
        })
        .await??;
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Ok(Http { child, url })
    }
}

struct Http {
    child: Child,
    url: String,
}

impl Http {
    async fn stop(mut self) -> Result<()> {
        self.child.kill().await?;
        self.child.wait().await?;
        Ok(())
    }
}

fn modern() -> ClientLifecycleMode {
    ClientLifecycleMode::Discover {
        preferred_versions: vec![ProtocolVersion::V_2026_07_28],
    }
}

async fn client(url: &str, handle: &str) -> Result<RunningService<RoleClient, ()>> {
    let key = match handle {
        "alice" => "a",
        "bob" => "b",
        "charlie" => "c",
        _ => unreachable!(),
    };
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url.to_owned()).auth_header(token(key)),
    );
    Ok(timeout(WAIT, ().serve_with_lifecycle(transport, modern())).await??)
}

async fn call(peer: &Peer<RoleClient>, name: &str, arguments: Value) -> Result<Value> {
    let result = timeout(
        WAIT,
        peer.call_tool(
            CallToolRequestParams::new(name.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        ),
    )
    .await??;
    assert_ne!(result.is_error, Some(true), "{name}: {result:?}");
    Ok(serde_json::from_str(
        &result.content[0].as_text().context("tool text")?.text,
    )?)
}

fn raw_request(method: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":1, "method":method, "params":{"_meta":{
        "io.modelcontextprotocol/protocolVersion":VERSION,
        "io.modelcontextprotocol/clientCapabilities":{}
    }}})
}

#[tokio::test]
async fn http_authenticates_every_request_without_sessions_or_initialize() -> Result<()> {
    let fixture = Fixture::new().await?;
    let app = fixture.http(0).await?;
    let http = reqwest::Client::builder().timeout(WAIT).build()?;
    let request = || {
        http.post(&app.url)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", VERSION)
            .header("Mcp-Method", "tools/list")
            .json(&raw_request("tools/list"))
    };
    assert_eq!(request().send().await?.status(), 401);
    assert_eq!(
        request().bearer_auth("wrong-token").send().await?.status(),
        401
    );
    assert_eq!(
        request()
            .bearer_auth(token("a"))
            .header("Origin", "https://untrusted.example")
            .send()
            .await?
            .status(),
        403
    );
    assert_eq!(
        request()
            .bearer_auth(token("a"))
            .header("Host", "untrusted.example")
            .send()
            .await?
            .status(),
        403
    );
    let response = request().bearer_auth(token("a")).send().await?;
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("mcp-session-id").is_none());
    let response: Value = response.json().await?;
    assert_eq!(response["result"]["resultType"], "complete", "{response}");
    assert!(
        response["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "send_message")
    );
    assert_eq!(request().send().await?.status(), 401);
    assert_eq!(
        http.get(&app.url)
            .bearer_auth(token("a"))
            .send()
            .await?
            .status(),
        405
    );
    let alice = client(&app.url, "alice").await?;
    let invalid = alice
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                json!({"to":["bob"],"body":" ","me":"bob"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    assert_eq!(invalid.is_error, Some(true));
    assert_eq!(call(&alice, "status", json!({})).await?["me"], "alice");
    alice.cancel().await?;
    app.stop().await
}

#[tokio::test]
async fn instances_share_queue_preserving_literal_bodies_threads_and_receipts() -> Result<()> {
    let fixture = Fixture::new().await?;
    let one = fixture.http(0).await?;
    let two = fixture.http(0).await?;
    let alice = client(&one.url, "alice").await?;
    let bob = client(&two.url, "bob").await?;
    let charlie = client(&one.url, "charlie").await?;
    let payload = "@a-file-that-must-not-be-read\n$(touch should-not-exist) `echo literal`";
    let sent = call(
        &alice,
        "send_message",
        json!({"to":["bob"],"body":payload,"subject":"review"}),
    )
    .await?;
    assert_eq!(
        call(&charlie, "get_thread", json!({"id":sent["thread"]})).await?,
        json!([])
    );
    assert!(
        charlie
            .read_resource(ReadResourceRequestParams::new("amq://inbox/bob"))
            .await
            .is_err()
    );
    let missing = alice
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                json!({"to":["missing-recipient"],"body":"x"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    assert_eq!(missing.is_error, Some(true));
    assert!(
        serde_json::to_string(&missing)?.contains("No AMQ mailbox"),
        "{missing:?}"
    );
    assert!(!fixture.root().join("agents/missing-recipient").exists());
    assert_eq!(
        call(&bob, "check_messages", json!({"peek":true})).await?[0]["id"],
        sent["id"]
    );
    let history = call(&bob, "get_thread", json!({"id":sent["thread"]})).await?;
    assert!(
        history
            .to_string()
            .contains("@a-file-that-must-not-be-read")
    );
    assert_eq!(
        call(&bob, "check_messages", json!({"peek":true}))
            .await?
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mail = call(&bob, "check_messages", json!({})).await?;
    assert_eq!(mail["count"], 1);
    assert_eq!(
        mail["drained"][0]["body"].as_str().unwrap().trim_end(),
        payload
    );
    assert_eq!(mail["drained"][0]["from"], "alice");
    assert!(!fixture.root().join("should-not-exist").exists());
    let receipts = fixture
        .command("amq")
        .args(["receipts", "list", "--me", "bob", "--msg-id"])
        .arg(sent["id"].as_str().unwrap())
        .arg("--json")
        .output()
        .await?;
    assert!(
        receipts.status.success(),
        "{}",
        String::from_utf8_lossy(&receipts.stderr)
    );
    assert!(String::from_utf8(receipts.stdout)?.contains("drained"));
    let reply = call(&bob, "reply", json!({"id":sent["id"],"body":"Reviewed."})).await?;
    assert_eq!(reply["thread"], sent["thread"]);
    assert_eq!(reply["to"], json!(["alice"]));
    let incoming = call(&alice, "wait_for_messages", json!({"timeout_seconds":1})).await?;
    assert_eq!(incoming["count"], 1);
    assert_eq!(
        incoming["drained"][0]["body"].as_str().unwrap().trim(),
        "Reviewed."
    );
    call(
        &bob,
        "set_summary",
        json!({"summary":"Reviewing the parser."}),
    )
    .await?;
    assert!(
        call(&alice, "list_peers", json!({}))
            .await?
            .to_string()
            .contains("Reviewing the parser.")
    );
    alice.cancel().await?;
    bob.cancel().await?;
    charlie.cancel().await?;
    one.stop().await?;
    two.stop().await
}

#[tokio::test]
async fn subscriptions_preserve_unread_mail_across_reconnects() -> Result<()> {
    let fixture = Fixture::new().await?;
    let app = fixture.http(0).await?;
    let alice = client(&app.url, "alice").await?;
    let bob = client(&app.url, "bob").await?;
    let filter = SubscriptionFilter::builder()
        .resource_subscription("amq://inbox/bob")
        .build();
    let mut subscription = timeout(WAIT, bob.listen(filter.clone()))
        .await
        .context("initial subscription acknowledgment")??;
    // Drain the initial invalidation so the assertion below proves a later filesystem change.
    ensure_initial_update(&mut subscription).await?;
    let sent = call(
        &alice,
        "send_message",
        json!({"to":["bob"],"body":"An unread message."}),
    )
    .await?;
    let notification = timeout(WAIT, subscription.next())
        .await
        .context("initial subscription notification")??
        .context("resource notification")?;
    let notification = serde_json::to_value(notification)?;
    assert_eq!(notification["params"]["uri"], "amq://inbox/bob");
    assert_eq!(
        notification["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
        serde_json::to_value(subscription.id())?
    );
    let resource = bob
        .read_resource(ReadResourceRequestParams::new("amq://inbox/bob"))
        .await?;
    let resource = serde_json::to_value(resource)?;
    let unread: Value = serde_json::from_str(resource["contents"][0]["text"].as_str().unwrap())?;
    assert_eq!(unread[0]["id"], sent["id"]);
    subscription.cancel().await?;
    bob.cancel().await?;
    let reconnected = client(&app.url, "bob").await?;
    assert_eq!(
        call(&reconnected, "check_messages", json!({"peek":true})).await?[0]["id"],
        sent["id"]
    );
    call(&reconnected, "read_message", json!({"id":sent["id"]})).await?;
    call(&reconnected, "read_message", json!({"id":sent["id"]})).await?;
    assert_eq!(
        call(&reconnected, "check_messages", json!({"peek":true})).await?,
        json!([])
    );
    let mut subscription = timeout(WAIT, reconnected.listen(filter))
        .await
        .context("reconnected subscription acknowledgment")??;
    ensure_initial_update(&mut subscription).await?;
    call(
        &alice,
        "send_message",
        json!({"to":["bob"],"body":"After reconnect."}),
    )
    .await?;
    assert!(
        timeout(WAIT, subscription.next())
            .await
            .context("reconnected subscription notification")??
            .is_some()
    );
    subscription.cancel().await?;
    alice.cancel().await?;
    reconnected.cancel().await?;
    app.stop().await
}

async fn ensure_initial_update(subscription: &mut rmcp::service::Subscription) -> Result<()> {
    timeout(WAIT, subscription.next())
        .await
        .context("initial inbox invalidation")??
        .context("initial inbox notification")?;
    Ok(())
}

#[tokio::test]
async fn stdio_supports_discover_and_legacy_initialize() -> Result<()> {
    let fixture = Fixture::new().await?;
    for lifecycle in [modern(), ClientLifecycleMode::Initialize] {
        let is_modern = matches!(lifecycle, ClientLifecycleMode::Discover { .. });
        let mut child = fixture
            .binary()
            .arg("--root")
            .arg(fixture.root())
            .args(["--me", "alice"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let transport = (child.stdout.take().unwrap(), child.stdin.take().unwrap());
        let client = timeout(WAIT, ().serve_with_lifecycle(transport, lifecycle)).await??;
        assert_eq!(
            client
                .peer_info()
                .unwrap()
                .protocol_version
                .has_initialize(),
            !is_modern
        );
        assert_eq!(call(&client, "status", json!({})).await?["me"], "alice");
        assert!(
            client
                .list_tools(None)
                .await?
                .tools
                .iter()
                .any(|tool| tool.name == "reply")
        );
        client.cancel().await?;
        timeout(WAIT, child.wait()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn native_feed_recovers_from_offline_start_and_does_not_consume_mail() -> Result<()> {
    let fixture = Fixture::new().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let mut feed = fixture
        .binary()
        .arg("feed")
        .env("AMQ_MCP_URL", format!("http://127.0.0.1:{port}/mcp"))
        .env("AMQ_MCP_TOKEN", token("b"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut frames = BufReader::new(feed.stdout.take().unwrap()).lines();
    let mut errors = BufReader::new(feed.stderr.take().unwrap()).lines();
    assert!(
        !timeout(WAIT, errors.next_line())
            .await??
            .context("offline diagnostic")?
            .is_empty()
    );
    assert!(feed.try_wait()?.is_none());
    let app = fixture.http(port).await?;
    let alice = client(&app.url, "alice").await?;
    let sent = call(
        &alice,
        "send_message",
        json!({"to":["bob"],"body":"Native terminal notification."}),
    )
    .await?;
    let frame: Value = serde_json::from_str(
        &timeout(WAIT, frames.next_line())
            .await??
            .context("feed frame")?,
    )?;
    assert_eq!(frame["type"], "amq_message");
    assert!(
        frame["content"]
            .as_str()
            .unwrap()
            .contains(sent["id"].as_str().unwrap())
    );
    assert!(
        !frame["content"]
            .as_str()
            .unwrap()
            .contains("Native terminal notification.")
    );
    let bob = client(&app.url, "bob").await?;
    assert_eq!(
        call(&bob, "check_messages", json!({"peek":true})).await?[0]["id"],
        sent["id"]
    );
    call(&bob, "read_message", json!({"id":sent["id"]})).await?;
    feed.kill().await?;
    feed.wait().await?;
    alice.cancel().await?;
    bob.cancel().await?;
    app.stop().await
}

struct Channel {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
}

impl Channel {
    fn start(fixture: &Fixture, url: &str) -> Result<Self> {
        let mut child = fixture
            .binary()
            .arg("channel")
            .env("AMQ_MCP_URL", url)
            .env("AMQ_MCP_TOKEN", token("b"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        Ok(Self {
            child,
            input,
            output,
        })
    }

    async fn send(&mut self, message: Value) -> Result<()> {
        self.input
            .write_all(format!("{message}\n").as_bytes())
            .await?;
        self.input.flush().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Value> {
        Ok(serde_json::from_str(
            &timeout(WAIT, self.output.next_line())
                .await??
                .context("channel closed")?,
        )?)
    }

    async fn request(&mut self, id: u32, method: &str, params: Value) -> Result<Value> {
        self.send(json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}))
            .await?;
        loop {
            let response = self.next().await?;
            if response["id"] == id {
                assert!(response.get("error").is_none(), "{response}");
                return Ok(response["result"].clone());
            }
        }
    }

    async fn call(&mut self, id: u32, name: &str, arguments: Value) -> Result<Value> {
        let result = self
            .request(id, "tools/call", json!({"name":name,"arguments":arguments}))
            .await?;
        assert_ne!(result["isError"], true, "{result}");
        Ok(serde_json::from_str(
            result["content"][0]["text"].as_str().unwrap(),
        )?)
    }
}

#[tokio::test]
async fn channel_announces_unread_mail_and_forwards_read_and_reply() -> Result<()> {
    let fixture = Fixture::new().await?;
    let app = fixture.http(0).await?;
    let mut channel = Channel::start(&fixture, &app.url)?;
    let initialized = channel.request(1, "initialize", json!({
        "protocolVersion":"2025-11-25", "capabilities":{}, "clientInfo":{"name":"channel-test","version":"1"}
    })).await?;
    assert!(
        initialized["capabilities"]["experimental"]
            .get("claude/channel")
            .is_some()
    );
    channel
        .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .await?;
    let tools = channel.request(2, "tools/list", json!({})).await?;
    assert!(
        tools["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "reply")
    );
    let alice = client(&app.url, "alice").await?;
    let sent = call(
        &alice,
        "send_message",
        json!({"to":["bob"],"body":"Please review."}),
    )
    .await?;
    let notification = loop {
        let notification = channel.next().await?;
        if notification["method"] == "notifications/claude/channel" {
            break notification;
        }
    };
    assert!(
        notification["params"]["content"]
            .as_str()
            .unwrap()
            .contains(sent["id"].as_str().unwrap())
    );
    assert_eq!(notification["params"]["meta"]["recipient"], "bob");
    assert_eq!(
        channel
            .call(3, "check_messages", json!({"peek":true}))
            .await?[0]["id"],
        sent["id"]
    );
    channel
        .call(4, "read_message", json!({"id":sent["id"]}))
        .await?;
    channel
        .call(
            5,
            "reply",
            json!({"id":sent["id"],"body":"Reviewed through channel."}),
        )
        .await?;
    assert_eq!(
        channel
            .call(6, "check_messages", json!({"peek":true}))
            .await?,
        json!([])
    );
    let incoming = call(&alice, "wait_for_messages", json!({"timeout_seconds":1})).await?;
    assert_eq!(
        incoming["drained"][0]["body"].as_str().unwrap().trim(),
        "Reviewed through channel."
    );
    channel.child.kill().await?;
    channel.child.wait().await?;
    alice.cancel().await?;
    app.stop().await
}

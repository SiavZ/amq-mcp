use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{IsTerminal, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Harness {
    Claude,
    Codex,
    Jcode,
}

impl Harness {
    pub fn name(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Jcode => "jcode",
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Install {
    pub root: PathBuf,
    pub tokens: PathBuf,
    pub url: String,
    pub label: String,
    pub amq: PathBuf,
    pub binaries: BTreeMap<String, PathBuf>,
    pub claude_restart: Option<PathBuf>,
}

pub fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not configured")
}

pub fn install_path() -> Result<PathBuf> {
    Ok(std::env::var_os("AMQ_MCP_INSTALL")
        .map(PathBuf::from)
        .unwrap_or(home()?.join(".local/state/amq-mcp/install.json")))
}

pub fn load_install() -> Result<Install> {
    let path = install_path()?;
    serde_json::from_slice(&fs::read(&path).with_context(|| format!("Read {}", path.display()))?)
        .context("Invalid AMQ MCP installation manifest")
}

pub fn valid_handle(value: &str) -> Result<&str> {
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
            && value.as_bytes()[0].is_ascii_alphanumeric(),
        "AM_ME must be a lowercase AMQ handle of at most 64 characters"
    );
    Ok(value)
}

fn variable<'a>(env: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    env.get(key).map(String::as_str).filter(|s| !s.is_empty())
}

fn captured(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

fn session_in_command(command: &str) -> Option<String> {
    command
        .split_whitespace()
        .find_map(|part| part.strip_prefix("YARAN_SESSION_ID="))
        .map(str::to_owned)
}

// shortcut: jcode reconnects omit the lane environment; remove process lookup when jcode forwards it on every start.
fn client_yaran_session(session: &str) -> Option<String> {
    let processes = captured("/bin/ps", &["eww", "-axo", "command="])?;
    let needle = format!("jcode --resume {session} ");
    let sessions: BTreeSet<_> = processes
        .lines()
        .filter(|line| line.contains(&needle))
        .filter_map(session_in_command)
        .collect();
    (sessions.len() == 1)
        .then(|| sessions.into_iter().next())
        .flatten()
}

// shortcut: a jcode Claude provider lacks its lane identity; accept only one client in the same working directory.
fn provider_yaran_session() -> Option<String> {
    let here = std::env::current_dir().ok()?;
    let listing = captured(
        "/usr/sbin/lsof",
        &["-a", "-c", "jcode", "-d", "cwd", "-Fpn"],
    )?;
    let mut pid = "";
    let mut sessions = BTreeSet::new();
    for line in listing.lines() {
        if let Some(value) = line.strip_prefix('p') {
            pid = value;
        }
        if line
            .strip_prefix('n')
            .is_some_and(|value| Path::new(value) == here)
        {
            let Some(command) = captured("/bin/ps", &["eww", "-o", "command=", "-p", pid]) else {
                continue;
            };
            if command.contains("jcode --resume ")
                && let Some(value) = session_in_command(&command)
            {
                sessions.insert(value);
            }
        }
    }
    (sessions.len() == 1)
        .then(|| sessions.into_iter().next())
        .flatten()
}

pub fn local_handle(
    harness: Harness,
    env: &BTreeMap<String, String>,
    root: &Path,
) -> Result<String> {
    let provider = matches!(harness, Harness::Claude)
        && variable(env, "JCODE_RUNTIME_PROVIDER").is_some()
        && variable(env, "AM_ME").is_none()
        && variable(env, "JCODE_MCP_CHANNEL_SESSION").is_none();
    if variable(env, "JCODE_MCP_CHANNEL_SESSION").is_some() || provider {
        let session = if provider {
            provider_yaran_session()
        } else {
            variable(env, "YARAN_SESSION_ID")
                .map(str::to_owned)
                .or_else(|| {
                    variable(env, "JCODE_MCP_CHANNEL_SESSION").and_then(client_yaran_session)
                })
        };
        if let Some(session) = session {
            let mut matches = BTreeSet::new();
            for entry in fs::read_dir(root.join("agents"))? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                for marker in [".dux-amq-source", ".yaran-amq-source"] {
                    let path = entry.path().join(marker);
                    match fs::symlink_metadata(&path) {
                        Ok(metadata) if metadata.is_file() => {}
                        Ok(_) => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(error.into()),
                    }
                    let contents = fs::read(&path)?;
                    let Ok(owner) = serde_json::from_slice::<Value>(&contents) else {
                        continue;
                    };
                    if owner["session_id"].as_str() == Some(&session) {
                        matches.insert(entry.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            ensure!(
                matches.len() == 1,
                "Cannot resolve a unique AMQ owner for this jcode session"
            );
            let handle = matches.into_iter().next().unwrap();
            valid_handle(&handle)?;
            return Ok(handle);
        }
        ensure!(
            !provider,
            "This Claude process is jcode's provider and no single lane works in its directory"
        );
        return Ok(valid_handle(variable(env, "AMQ_MCP_JCODE_HANDLE").unwrap_or("jcode"))?.into());
    }
    Ok(valid_handle(variable(env, "AM_ME").unwrap_or(harness.name()))?.into())
}

pub fn claude_channels(args: &[OsString], enabled: bool) -> Vec<OsString> {
    let mut remaining = Vec::new();
    let mut channels = vec![OsString::from("server:amq")];
    let mut i = 0;
    while i < args.len() {
        let value = args[i].to_string_lossy();
        if value == "--" {
            remaining.extend_from_slice(&args[i..]);
            break;
        }
        if value == "--dangerously-load-development-channels" {
            while args.get(i + 1).is_some_and(|next| {
                let s = next.to_string_lossy();
                s.starts_with("server:") || s.starts_with("plugin:")
            }) {
                i += 1;
                if !channels.contains(&args[i]) {
                    channels.push(args[i].clone());
                }
            }
        } else if let Some(channel) = value.strip_prefix("--dangerously-load-development-channels=")
        {
            let channel = OsString::from(channel);
            if !channels.contains(&channel) {
                channels.push(channel);
            }
        } else {
            remaining.push(args[i].clone());
        }
        i += 1;
    }
    if enabled {
        let position = remaining
            .iter()
            .position(|s| s == "--")
            .unwrap_or(remaining.len());
        remaining.splice(
            position..position,
            std::iter::once(OsString::from("--dangerously-load-development-channels"))
                .chain(channels),
        );
    }
    remaining
}

pub fn private_write(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_file_name(format!(
        "{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> Result<()> {
        let mut file = options.open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn locked(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    file.lock()?;
    Ok(file)
}

fn mailbox(config: &Install, handle: &str) -> Result<()> {
    let _lock = locked(&config.root.join("meta/config.lock"))?;
    let directory = config.root.join("agents").join(handle);
    for suffix in [
        "inbox/new",
        "inbox/cur",
        "inbox/tmp",
        "outbox/sent",
        "receipts",
        "dlq/new",
        "dlq/cur",
        "dlq/tmp",
    ] {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(directory.join(suffix))?;
    }
    let path = config.root.join("meta/config.json");
    let mut metadata: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let agents = metadata["agents"]
        .as_array_mut()
        .context("Invalid AMQ agent registry")?;
    if !agents.iter().any(|agent| agent == handle) {
        agents.push(json!(handle));
        private_write(&path, &metadata)?;
    }
    Ok(())
}

pub fn provision(config: &Install, handle: &str) -> Result<()> {
    valid_handle(handle)?;
    let mut token_lock = config.tokens.as_os_str().to_os_string();
    token_lock.push(".lock");
    let _lock = locked(Path::new(&token_lock))?;
    mailbox(config, handle)?;
    let mut tokens: Value = serde_json::from_slice(&fs::read(&config.tokens)?)?;
    ensure!(
        tokens.is_object(),
        "Credentials must be an object of handle to token"
    );
    if tokens.get(handle).is_none() {
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|error| anyhow::anyhow!("Generate credential: {error}"))?;
        tokens[handle] = json!(
            random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        private_write(&config.tokens, &tokens)?;
        // shortcut: new handles reload the service; add credential hot reload if frequent reconnects become a problem.
        let uid = captured("/usr/bin/id", &["-u"]).context("Read current user ID")?;
        let status = Command::new("/bin/launchctl")
            .args([
                "kickstart",
                "-k",
                &format!("gui/{}/{}", uid.trim(), config.label),
            ])
            .status()?;
        ensure!(status.success(), "Could not reload the AMQ MCP LaunchAgent");
    }
    Ok(())
}

fn token_at(path: &Path, handle: &str) -> Result<String> {
    let tokens: Value = serde_json::from_slice(&fs::read(path)?)?;
    tokens
        .get(handle)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("No credential configured for {handle}"))
}

pub async fn run_local(mode: &str, harness: Harness) -> Result<()> {
    let config = load_install()?;
    let mut env: BTreeMap<_, _> = std::env::vars().collect();
    let me = local_handle(harness, &env, &config.root)?;
    if mode == "channel" {
        crate::delivery::run_channel(Some(config.url), Some(token_at(&config.tokens, &me)?)).await
    } else if mode == "tools" {
        env.insert("AMQ_BIN".into(), config.amq.to_string_lossy().into_owned());
        env.insert("AM_ROOT".into(), config.root.to_string_lossy().into_owned());
        let amq = crate::amq::Amq::connect_env(Some(config.root), Some(me), env).await?;
        crate::serve_stdio(amq).await
    } else {
        bail!("Choose local tools or channel");
    }
}

fn exec(mut command: Command) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(command.exec()).context("Launch native harness")
    }
    #[cfg(not(unix))]
    {
        let status = command.status()?;
        ensure!(status.success(), "Harness exited with {status}");
        Ok(())
    }
}

fn feed_binary() -> Result<PathBuf> {
    Ok(std::env::current_exe()?.with_file_name(if cfg!(windows) {
        "amq-inbox-feed.exe"
    } else {
        "amq-inbox-feed"
    }))
}

pub fn with_command(args: Vec<OsString>) -> Result<()> {
    let (program, arguments) = args.split_first().context("Provide a command to launch")?;
    let handle =
        std::env::var("AM_ME").context("Set AM_ME to a unique handle for this terminal")?;
    valid_handle(&handle)?;
    let tokens = std::env::var_os("AMQ_MCP_TOKENS")
        .map(PathBuf::from)
        .unwrap_or(home()?.join(".local/state/amq-mcp/tokens.json"));
    let token = std::env::var("AMQ_MCP_TOKEN")
        .ok()
        .filter(|s| !s.is_empty())
        .map(Ok)
        .unwrap_or_else(|| token_at(&tokens, &handle))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env("AM_ME", handle)
        .env("AMQ_MCP_TOKEN", token)
        .env("AMQ_MCP_TOKENS", tokens)
        .env(
            "AMQ_MCP_URL",
            std::env::var("AMQ_MCP_URL").unwrap_or_else(|_| "http://127.0.0.1:8765/mcp".into()),
        )
        .env(
            "AMQ_MCP_FEED",
            std::env::var_os("AMQ_MCP_FEED")
                .map(PathBuf::from)
                .unwrap_or(feed_binary()?),
        );
    exec(command)
}

fn command_only(harness: Harness, args: &[OsString]) -> bool {
    let before_separator = &args[..args.iter().position(|s| s == "--").unwrap_or(args.len())];
    if before_separator.iter().any(|s| {
        ["--help", "-h", "--version", "-V"]
            .iter()
            .any(|flag| s == flag)
    }) {
        return true;
    }
    let first = args
        .first()
        .map(|s| s.to_string_lossy())
        .unwrap_or_default();
    match harness {
        Harness::Claude => {
            before_separator
                .iter()
                .any(|s| ["-p", "--print", "--bare"].iter().any(|flag| s == flag))
                || [
                    "auth",
                    "mcp",
                    "plugin",
                    "install",
                    "update",
                    "doctor",
                    "agents",
                    "attach",
                    "logs",
                    "stop",
                    "rm",
                    "config",
                    "setup-token",
                    "remote-control",
                    "daemon",
                ]
                .contains(&first.as_ref())
        }
        Harness::Codex => [
            "exec",
            "review",
            "mcp",
            "mcp-server",
            "app-server",
            "login",
            "logout",
            "completion",
            "sandbox",
            "debug",
            "apply",
            "cloud",
            "features",
            "doctor",
            "update",
            "settings",
            "tasks",
            "status",
            "migrate-nfs-db",
            "unarchive",
        ]
        .contains(&first.as_ref()),
        Harness::Jcode => !first.is_empty() && !first.starts_with('-'),
    }
}

pub async fn launch(harness: Harness, args: Vec<OsString>) -> Result<()> {
    let config = load_install()?;
    let binary = config
        .binaries
        .get(harness.name())
        .context("No binary configured for this harness")?;
    let plain = if matches!(harness, Harness::Claude) {
        claude_channels(&args, false)
    } else {
        args.clone()
    };
    let mut command = Command::new(binary);
    if command_only(harness, &plain) {
        command.args(plain);
        return exec(command);
    }
    let me = std::env::var("AM_ME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| harness.name().into());
    valid_handle(&me)?;
    if let Some(root) = std::env::var_os("AM_ROOT") {
        ensure!(
            Path::new(&root) == config.root,
            "This installation serves {}; AM_ROOT names another queue",
            config.root.display()
        );
    }
    provision(&config, &me)?;
    let token = token_at(&config.tokens, &me)?;
    command
        .env("AM_ME", &me)
        .env("AM_ROOT", &config.root)
        .env("AMQ_BIN", &config.amq)
        .env("AMQ_MCP_TOKEN", &token)
        .env("AMQ_MCP_TOKENS", &config.tokens)
        .env("AMQ_MCP_URL", &config.url)
        .env("AMQ_MCP_FEED", feed_binary()?)
        .env("AMQ_MCP_NATIVE", "1");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.post(&config.url).bearer_auth(&token)
            .header("MCP-Protocol-Version", "2026-07-28").header("Mcp-Method", "tools/list")
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{
                "io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}))
            .send().await;
        if response.as_ref().is_ok_and(|r| r.status().is_success()) {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "AMQ MCP service is unavailable at {}",
            config.url
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    match harness {
        Harness::Claude => {
            command.args(claude_channels(&args, true));
            if std::env::var_os("CLAUDE_CODE_PROCESS_WRAPPER").is_none()
                && let Some(wrapper) = config.claude_restart
            {
                command.env("CLAUDE_CODE_PROCESS_WRAPPER", wrapper);
                if std::io::stdin().is_terminal()
                    && let Ok(output) = Command::new("tty").stdin(Stdio::inherit()).output()
                    && output.status.success()
                {
                    command.env(
                        "YARAN_CLAUDE_PEERS_TTY",
                        String::from_utf8_lossy(&output.stdout).trim(),
                    );
                }
            }
        }
        Harness::Codex => {
            command
                .args(["-c", "mcp_servers.amq.enabled=true"])
                .args(args);
        }
        Harness::Jcode => {
            if std::env::var("YARAN_SESSION_ID")
                .unwrap_or_default()
                .is_empty()
            {
                command.env("AMQ_MCP_JCODE_HANDLE", &me);
                // shortcut: standalone jcode uses a daemon per handle until the shared daemon forwards AM_ME.
                if !args
                    .iter()
                    .any(|s| s == "--socket" || s.to_string_lossy().starts_with("--socket="))
                {
                    let directory = config
                        .tokens
                        .parent()
                        .context("Invalid credential path")?
                        .join("jcode");
                    fs::create_dir_all(&directory)?;
                    let digest = Sha256::digest(format!("{}\0{me}", config.root.display()));
                    let id: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
                    command
                        .arg("--socket")
                        .arg(directory.join(format!("{id}.sock")));
                }
            }
            command.args(args);
        }
    }
    exec(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }
    #[test]
    fn validates_handles_and_keeps_literal_claude_arguments() {
        for bad in ["", "../other", "UPPER", "_leading"] {
            assert!(valid_handle(bad).is_err());
        }
        assert!(valid_handle(&"x".repeat(65)).is_err());
        assert_eq!(
            claude_channels(&args(&["--model", "fable", "--", "literal $(text)"]), true),
            args(&[
                "--model",
                "fable",
                "--dangerously-load-development-channels",
                "server:amq",
                "--",
                "literal $(text)"
            ])
        );
        let input = args(&[
            "--dangerously-load-development-channels",
            "server:claude-peers",
            "mcp",
            "list",
        ]);
        assert_eq!(claude_channels(&input, false), args(&["mcp", "list"]));
        assert!(command_only(
            Harness::Claude,
            &claude_channels(&input, false)
        ));
        assert!(!command_only(Harness::Codex, &args(&["an initial prompt"])));
        assert!(!command_only(Harness::Codex, &args(&["--", "--help"])));
    }
    #[test]
    fn resolves_managed_identity_without_borrowing_daemon() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = directory.path();
        fs::create_dir_all(root.join("agents/current-agent"))?;
        fs::write(
            root.join("agents/current-agent/.dux-amq-source"),
            r#"{"session_id":"current-session"}"#,
        )?;
        let mut env = BTreeMap::from([
            ("JCODE_MCP_CHANNEL_SESSION".into(), "native-session".into()),
            ("YARAN_SESSION_ID".into(), "current-session".into()),
            ("AM_ME".into(), "stale-daemon".into()),
        ]);
        assert_eq!(local_handle(Harness::Jcode, &env, root)?, "current-agent");
        assert_eq!(local_handle(Harness::Claude, &env, root)?, "current-agent");
        env.insert("YARAN_SESSION_ID".into(), "missing".into());
        assert!(local_handle(Harness::Jcode, &env, root).is_err());
        Ok(())
    }
    #[test]
    fn mailbox_registration_preserves_other_metadata() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = directory.path();
        fs::create_dir(root.join("meta"))?;
        fs::write(
            root.join("meta/config.json"),
            r#"{"agents":["existing"],"created_utc":"preserved"}"#,
        )?;
        let config = Install {
            root: root.into(),
            tokens: root.join("tokens.json"),
            url: String::new(),
            label: String::new(),
            amq: PathBuf::from("amq"),
            binaries: BTreeMap::new(),
            claude_restart: None,
        };
        mailbox(&config, "added")?;
        mailbox(&config, "added")?;
        let metadata: Value = serde_json::from_slice(&fs::read(root.join("meta/config.json"))?)?;
        assert_eq!(
            metadata,
            json!({"agents":["existing","added"],"created_utc":"preserved"})
        );
        assert!(root.join("agents/added/inbox/new").is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(root.join("meta/config.json"))?
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        Ok(())
    }
}

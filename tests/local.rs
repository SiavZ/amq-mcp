#![cfg(unix)]
use amq_mcp::http::start_http;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Stdio, time::Duration};
use tokio::process::Command;

fn isolated(command: &mut Command, root: &Path) {
    for key in [
        "AM_ROOT_ID",
        "AM_BASE_ROOT_ID",
        "AM_SESSION",
        "AM_ME",
        "AMQ_MCP_TOKEN",
        "AMQ_MCP_FEED",
        "AMQ_MCP_NATIVE",
        "AMQ_MCP_JCODE_HANDLE",
        "JCODE_MCP_CHANNEL_SESSION",
        "YARAN_SESSION_ID",
        "JCODE_RUNTIME_PROVIDER",
        "CLAUDE_CODE_PROCESS_WRAPPER",
    ] {
        command.env_remove(key);
    }
    command
        .env("AM_ROOT", root)
        .env("AM_BASE_ROOT", root)
        .env("AMQ_BIN", "amq")
        .env("AMQ_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::null())
        .kill_on_drop(true);
}

#[tokio::test]
async fn rust_launchers_preserve_native_arguments_and_oneshot_passthrough() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("queue");
    let mut init = Command::new("amq");
    isolated(&mut init, &root);
    let output = init
        .args(["init", "--root"])
        .arg(&root)
        .args(["--agents", "existing"])
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "amq init: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tokens = directory.path().join("tokens.json");
    fs::write(
        &tokens,
        serde_json::to_vec(
            &json!({"claude":"a".repeat(40),"codex":"b".repeat(40),"jcode":"c".repeat(40)}),
        )?,
    )?;
    let server = start_http(0, &tokens, Some(root.clone())).await?;
    let fake = directory.path().join("fake-harness");
    fs::write(
        &fake,
        "#!/bin/sh\nprintf 'identity=%s\\nnative=%s\\nfeed=%s\\n' \"${AM_ME-}\" \"${AMQ_MCP_NATIVE-}\" \"${AMQ_MCP_FEED-}\"\nif [ -n \"${AMQ_MCP_TOKEN-}\" ]; then printf 'token=present\\n'; else printf 'token=absent\\n'; fi\nfor arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n",
    )?;
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o700))?;
    let install = directory.path().join("install.json");
    fs::write(
        &install,
        serde_json::to_vec(
            &json!({"root":root,"tokens":tokens,"url":server.url,"label":"unused-test-label","amq":"amq","binaries":{"claude":fake,"codex":fake,"jcode":fake}}),
        )?,
    )?;
    for (harness, args, expected) in [
        (
            "claude",
            vec![
                "--dangerously-load-development-channels",
                "server:claude-peers",
                "--",
                "literal $(text)",
            ],
            vec![
                "--dangerously-load-development-channels",
                "server:amq",
                "server:claude-peers",
                "--",
                "literal $(text)",
            ],
        ),
        (
            "codex",
            vec!["resume", "a-session"],
            vec!["-c", "mcp_servers.amq.enabled=true", "resume", "a-session"],
        ),
        (
            "codex",
            vec!["an initial prompt"],
            vec!["-c", "mcp_servers.amq.enabled=true", "an initial prompt"],
        ),
        ("jcode", vec!["--resume", "a-session"], vec![]),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_amq-mcp"));
        isolated(&mut command, &root);
        command
            .env("AMQ_MCP_INSTALL", &install)
            .args(["launch", harness, "--"])
            .args(args);
        let output = tokio::time::timeout(Duration::from_secs(10), command.output()).await??;
        ensure!(
            output.status.success(),
            "{harness}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout)?;
        assert!(text.contains(&format!("identity={harness}\n")), "{text}");
        assert!(
            text.contains("native=1\n") && text.contains("token=present\n"),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|line| line.starts_with("feed=") && line.ends_with("amq-inbox-feed")),
            "{text}"
        );
        let actual: Vec<_> = text
            .lines()
            .filter_map(|line| line.strip_prefix("arg="))
            .collect();
        if harness == "jcode" {
            assert_eq!(actual[0], "--socket");
            assert!(actual[1].ends_with(".sock"));
            assert_eq!(&actual[2..], ["--resume", "a-session"]);
        } else {
            assert_eq!(actual, expected);
        }
    }
    let registry: Value = serde_json::from_slice(&fs::read(root.join("meta/config.json"))?)?;
    assert_eq!(
        registry["agents"],
        json!(["existing", "claude", "codex", "jcode"])
    );
    server.shutdown().await?;
    // No service is needed for provider CLI subcommands.
    for (harness, args, expected) in [
        ("codex", vec!["exec", "one shot"], vec!["exec", "one shot"]),
        (
            "claude",
            vec![
                "--dangerously-load-development-channels",
                "server:claude-peers",
                "mcp",
                "list",
            ],
            vec!["mcp", "list"],
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_amq-mcp"));
        isolated(&mut command, &root);
        let output = command
            .env("AMQ_MCP_INSTALL", &install)
            .args(["launch", harness, "--"])
            .args(args)
            .output()
            .await?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout)?;
        assert!(text.contains("token=absent\n"));
        assert_eq!(
            text.lines()
                .filter_map(|line| line.strip_prefix("arg="))
                .collect::<Vec<_>>(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn credential_wrapper_preserves_literal_arguments_without_printing_credentials() -> Result<()>
{
    let token = "wrapper-test-credential-never-printed";
    let output = Command::new(env!("CARGO_BIN_EXE_amq-mcp"))
        .env("AM_ME", "test-terminal")
        .env("AMQ_MCP_TOKEN", token)
        .args([
            "with",
            "--",
            "/bin/sh",
            "-c",
            "printf '%s\\n' \"$AM_ME\" \"$AMQ_MCP_FEED\" \"$1\" \"$2\"",
            "wrapped",
            "a spaced argument",
            "$(literal)",
        ])
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout)?;
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.first(), Some(&"test-terminal"));
    assert!(
        lines
            .get(1)
            .context("feed executable")?
            .ends_with("amq-inbox-feed")
    );
    assert_eq!(&lines[2..], ["a spaced argument", "$(literal)"]);
    assert!(!text.contains(token));
    Ok(())
}

use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

pub const CLI_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_OUTPUT: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct AmqError {
    pub message: String,
    pub exit_code: Option<i32>,
}

impl fmt::Display for AmqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for AmqError {}

#[derive(Clone)]
pub struct Amq {
    pub identity: Value,
    command: Arc<OsString>,
    env: Arc<BTreeMap<OsString, OsString>>,
    cwd: Arc<PathBuf>,
    root: Arc<PathBuf>,
    me: Arc<String>,
}

impl Amq {
    pub async fn connect(root: Option<PathBuf>, me: Option<String>) -> Result<Self> {
        Self::connect_with_env(root, me, std::env::vars_os().collect()).await
    }

    pub async fn connect_env(
        root: Option<PathBuf>,
        me: Option<String>,
        env: BTreeMap<String, String>,
    ) -> Result<Self> {
        Self::connect_with_env(
            root,
            me,
            env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        )
        .await
    }

    async fn connect_with_env(
        root: Option<PathBuf>,
        me: Option<String>,
        mut env: BTreeMap<OsString, OsString>,
    ) -> Result<Self> {
        env.insert("AMQ_NO_UPDATE_CHECK".into(), "1".into());
        let command = env
            .get(std::ffi::OsStr::new("AMQ_BIN"))
            .cloned()
            .unwrap_or_else(|| "amq".into());
        let mut amq = Self {
            identity: Value::Null,
            command: Arc::new(command),
            env: Arc::new(env),
            cwd: Arc::new(std::env::current_dir()?),
            root: Arc::new(PathBuf::new()),
            me: Arc::new(String::new()),
        };
        let mut args = vec!["env".into(), "--json".into()];
        if let Some(root) = root {
            args.extend([
                "--root".into(),
                root.into_os_string()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("AMQ root must be valid UTF-8"))?,
            ]);
        }
        if let Some(me) = me {
            args.extend(["--me".into(), me]);
        }
        amq.identity = amq.run(&args, None, CLI_TIMEOUT).await?;
        let me = amq.identity["me"]
            .as_str()
            .filter(|v| !v.is_empty())
            .context("Set AM_ME or pass --me with a unique AMQ handle.")?
            .to_owned();
        let root = PathBuf::from(
            amq.identity["root"]
                .as_str()
                .context("AMQ returned no queue root.")?,
        );
        let mut env = (*amq.env).clone();
        for (name, key) in [
            ("AM_ROOT", "root"),
            ("AM_ROOT_ID", "root_id"),
            ("AM_BASE_ROOT", "base_root"),
            ("AM_BASE_ROOT_ID", "base_root_id"),
            ("AM_SESSION", "session_name"),
            ("AM_ME", "me"),
        ] {
            if let Some(value) = amq.identity.get(key) {
                env.insert(
                    name.into(),
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                        .into(),
                );
            } else {
                env.remove(std::ffi::OsStr::new(name));
            }
        }
        amq.env = Arc::new(env);
        amq.root = Arc::new(root);
        amq.me = Arc::new(me);
        Ok(amq)
    }

    pub fn me(&self) -> &str {
        &self.me
    }
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn run(
        &self,
        args: &[String],
        body: Option<&str>,
        timeout: Duration,
    ) -> Result<Value> {
        let mut child = Command::new(self.command.as_ref())
            .args(args)
            .current_dir(self.cwd.as_ref())
            .env_clear()
            .envs(self.env.iter())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("Could not launch AMQ")?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let mut stdin = child.stdin.take().expect("piped stdin");
        let execution = async {
            let (stdout, stderr, status, _) = tokio::try_join!(
                read_limited(stdout),
                read_limited(stderr),
                async { child.wait().await.map_err(anyhow::Error::from) },
                async {
                    // Early CLI failures may close stdin before the body is written.
                    let _ = stdin.write_all(body.unwrap_or_default().as_bytes()).await;
                    drop(stdin);
                    Ok::<_, anyhow::Error>(())
                },
            )?;
            if !status.success() {
                let stderr = String::from_utf8_lossy(&stderr).trim().to_owned();
                return Err(AmqError {
                    message: if stderr.is_empty() {
                        format!("AMQ exited with {status}")
                    } else {
                        stderr
                    },
                    exit_code: status.code(),
                }
                .into());
            }
            if stdout.iter().all(u8::is_ascii_whitespace) {
                return Ok(Value::Null);
            }
            serde_json::from_slice(&stdout).map_err(|_| {
                AmqError {
                    message: "AMQ returned invalid JSON. Check your AMQ version.".into(),
                    exit_code: Some(1),
                }
                .into()
            })
        };
        tokio::time::timeout(timeout, execution)
            .await
            .map_err(|_| anyhow::anyhow!("AMQ timed out after {} seconds", timeout.as_secs()))?
    }
}

async fn read_limited(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    reader.take(MAX_OUTPUT + 1).read_to_end(&mut output).await?;
    if output.len() as u64 > MAX_OUTPUT {
        bail!("AMQ output exceeded the 8 MiB limit.");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_cli_output() {
        assert_eq!(read_limited(&b"{}"[..]).await.unwrap(), b"{}");
        assert!(
            read_limited(std::io::Cursor::new(vec![b'x'; MAX_OUTPUT as usize + 1]))
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn freezes_resolved_cli_context_and_preserves_literal_stdin() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let command = directory.path().join("amq");
        std::fs::write(&command, r#"#!/bin/sh
case "$1" in
env) printf '%s\n' "$FAKE_IDENTITY" ;;
context) printf '{"me":"%s","root":"%s","session":"%s","root_id":"%s","base_root_id":"%s","updates":"%s"}\n' "$AM_ME" "$AM_ROOT" "$AM_SESSION" "$AM_ROOT_ID" "${AM_BASE_ROOT_ID-unset}" "$AMQ_NO_UPDATE_CHECK" ;;
body) /bin/cat ;;
invalid) printf 'invalid-json' ;;
fail) printf 'AMQ test failure' >&2; exit 4 ;;
pause) exec /bin/sleep 2 ;;
esac
"#).unwrap();
        std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o700)).unwrap();
        let env = BTreeMap::from([
            ("AMQ_BIN".into(), command.to_str().unwrap().into()),
            ("FAKE_IDENTITY".into(), serde_json::json!({"me":"alice","root":"/resolved/root","root_id":"root-id","session_name":"resolved-session"}).to_string()),
            ("AM_ME".into(), "wrong-identity".into()),
            ("AM_BASE_ROOT_ID".into(), "must-be-removed".into()),
        ]);
        let amq = Amq::connect_env(None, None, env).await.unwrap();
        assert_eq!(amq.me(), "alice");
        assert_eq!(amq.root(), Path::new("/resolved/root"));
        assert_eq!(
            amq.run(&["context".into()], None, CLI_TIMEOUT)
                .await
                .unwrap(),
            serde_json::json!({"me":"alice","root":"/resolved/root","session":"resolved-session","root_id":"root-id","base_root_id":"unset","updates":"1"})
        );
        let body = serde_json::json!("@do-not-read\n$(literal) `literal`").to_string();
        assert_eq!(
            amq.run(&["body".into()], Some(&body), CLI_TIMEOUT)
                .await
                .unwrap(),
            serde_json::from_str::<Value>(&body).unwrap()
        );
        let error = amq
            .run(&["fail".into()], None, CLI_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(error.downcast_ref::<AmqError>().unwrap().exit_code, Some(4));
        assert_eq!(error.to_string(), "AMQ test failure");
        assert!(
            amq.run(&["invalid".into()], None, CLI_TIMEOUT)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid JSON")
        );
        assert!(
            amq.run(&["pause".into()], None, Duration::from_millis(25))
                .await
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
    }
}

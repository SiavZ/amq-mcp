use amq_mcp::{
    amq::Amq,
    delivery, http,
    local::{self, Harness},
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{ffi::OsString, path::PathBuf, process::ExitCode};

#[derive(Parser)]
#[command(
    version,
    about = "Stateless AMQ MCP service and native terminal delivery"
)]
struct Cli {
    #[arg(long)]
    root: Option<PathBuf>,
    #[arg(long)]
    me: Option<String>,
    #[arg(long, value_name = "PORT")]
    http: Option<u16>,
    #[arg(long, value_name = "FILE")]
    tokens: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Mode>,
}

#[derive(Subcommand)]
enum Mode {
    /// Emit typed unread notices to the patched terminal's private pipe.
    Feed,
    /// Expose AMQ tools and Claude/jcode native channel notifications.
    Channel,
    /// Use an existing local installation manifest.
    Local {
        mode: String,
        #[arg(value_enum)]
        harness: Harness,
    },
    /// Launch a terminal using the local installation's native delivery.
    Launch {
        #[arg(value_enum)]
        harness: Harness,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<OsString>,
    },
    /// Load the current AMQ credential and execute a command.
    With {
        #[arg(last = true, required = true)]
        args: Vec<OsString>,
    },
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Mode::Feed) => delivery::run_feed(None, None).await,
        Some(Mode::Channel) => delivery::run_channel(None, None).await,
        Some(Mode::Local { mode, harness }) => local::run_local(&mode, harness).await,
        Some(Mode::Launch { harness, args }) => local::launch(harness, args).await,
        Some(Mode::With { args }) => local::with_command(args),
        None => {
            if let Some(port) = cli.http {
                let tokens = cli
                    .tokens
                    .context("HTTP requires --tokens FILE with one credential per mailbox")?;
                let server = http::start_http(port, &tokens, cli.root).await?;
                amq_mcp::shutdown_signal().await;
                server.shutdown().await?;
                Ok(())
            } else {
                anyhow::ensure!(cli.tokens.is_none(), "--tokens requires --http");
                amq_mcp::serve_stdio(Amq::connect(cli.root, cli.me).await?).await
            }
        }
    }
}

fn main() -> ExitCode {
    amq_mcp::runtime_main(run(Cli::parse()))
}

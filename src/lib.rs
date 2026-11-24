pub mod amq;
pub mod delivery;
pub mod http;
pub mod local;
pub mod server;

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub async fn serve_stdio(amq: amq::Amq) -> anyhow::Result<()> {
    use rmcp::ServiceExt;
    let cancel = tokio_util::sync::CancellationToken::new();
    let signal = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            shutdown_signal().await;
            cancel.cancel();
        }
    });
    let result = async {
        let service = server::Server::new(amq)?
            .serve_with_ct(rmcp::transport::stdio(), cancel)
            .await?;
        service.waiting().await?;
        Ok(())
    }
    .await;
    signal.abort();
    result
}

pub fn runtime_main(
    future: impl std::future::Future<Output = anyhow::Result<()>>,
) -> std::process::ExitCode {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("start async runtime");
    let result = runtime.block_on(future);
    // A cancelled stdio read can still hold Tokio's blocking stdin thread.
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("amq-mcp: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

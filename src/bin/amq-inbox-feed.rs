fn main() -> std::process::ExitCode {
    amq_mcp::runtime_main(amq_mcp::delivery::run_feed(None, None))
}

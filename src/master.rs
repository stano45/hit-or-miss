use clap::Parser;
use tracing::{event, Level};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Network port to use
    #[arg(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
}

fn main() {
    let args = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    event!(
        Level::INFO,
        "Starting master service on port: {0}",
        args.port
    );
}

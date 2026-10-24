use anyhow::Result;
use attosend::{
    config::Config,
    model::{Event, MessageId, Request, Status},
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[arg(long, global = true, default_value = "/etc/attosend/config.json")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Run the delivery daemon, Alertmanager receiver and private control socket.
    Serve,
    /// Probe configured peers, readiness endpoints and failed systemd units.
    Check,
    /// List pending and failed messages as JSON.
    Queue,
    /// Retry an unsent message without changing its payload or idempotency key.
    Retry { id: MessageId },
    /// Record a failed systemd unit (used by `OnFailure` hooks).
    UnitFailure { unit: String },
    /// Submit an incident transition.
    Submit {
        #[arg(long)]
        key: String,
        #[arg(long, default_value = "manual")]
        source: String,
        #[arg(long, value_enum, default_value = "firing")]
        status: Status,
        #[arg(long)]
        summary: String,
        #[arg(long, default_value = "")]
        details: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::read(&args.config)?;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_new(config.log_level())?)
        .init();
    let request = match args.command {
        Command::Serve => return attosend::server::serve(config).await,
        Command::Check => return attosend::checks::check(&config).await,
        Command::Queue => Request::Queue,
        Command::Retry { id } => Request::Retry { id },
        Command::UnitFailure { unit } => {
            let Some(event) = attosend::checks::unit_failure(&unit).await? else {
                return Ok(());
            };
            Request::Submit {
                events: vec![event],
            }
        }
        Command::Submit {
            key,
            source,
            status,
            summary,
            details,
        } => Request::Submit {
            events: vec![Event::new(key, source, status, summary, details)?],
        },
    };
    let output = attosend::server::request(&config, &request).await?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

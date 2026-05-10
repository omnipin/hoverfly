//! isheika CLI — `discover` / `fetch` / `upload` against the Swarm network
//! over libp2p WebSocket only, with DoH-only DNS resolution.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use isheika::client::{fetch_manifest_path, list_manifest};
use isheika::{
    discover, fetch_bytes, upload_bytes, Doh, PeerStore, SwarmSigner, Transport, TransportConfig,
    DEFAULT_DOH_URL, MAINNET_BOOTNODE,
};
use libp2p::Multiaddr;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "isheika", version, about = "Swarm micro-client (WS-only, WASM-portable)")]
struct Cli {
    /// Verbose output (info-level logging)
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Debug output (trace-level logging)
    #[arg(short, long, global = true)]
    debug: bool,

    /// DoH endpoint to use for DNS resolution
    #[arg(long, global = true, default_value = DEFAULT_DOH_URL, value_name = "URL")]
    doh_url: String,

    /// Network ID (1 = mainnet, 10 = testnet)
    #[arg(long, global = true, default_value_t = 1, value_name = "ID")]
    network_id: u64,

    /// Connection timeout in seconds
    #[arg(long, global = true, default_value_t = 10, value_name = "SECS")]
    timeout: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Discover peers from a bootstrap multiaddr or /dnsaddr/.
    Discover {
        /// Bootstrap peer (defaults to mainnet bootnode dnsaddr)
        #[arg(value_name = "PEER", default_value = MAINNET_BOOTNODE)]
        peer: String,

        /// Output peers.json path
        #[arg(short, long, default_value = "peers.json", value_name = "FILE")]
        output: PathBuf,

        /// How long to listen for hive announcements per peer (seconds)
        #[arg(long, default_value_t = 30)]
        wait: u64,

        /// Append to existing peers.json instead of overwriting
        #[arg(long)]
        append: bool,
    },

    /// Fetch content addressed by a 32-byte hex root hash.
    Fetch {
        /// Swarm root hash (hex)
        #[arg(value_name = "HASH")]
        hash: String,

        /// Output file (required unless --list)
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,

        /// Treat the root as a mantaray manifest and resolve this path.
        #[arg(long, value_name = "PATH")]
        path: Option<String>,

        /// List all entries in the manifest at the root and exit.
        #[arg(long, conflicts_with = "path")]
        list: bool,

        /// peers.json path
        #[arg(long, default_value = "peers.json", value_name = "FILE")]
        peerlist: PathBuf,

        /// Number of peers to try per chunk before giving up
        #[arg(long, default_value_t = 10)]
        max_retries: usize,
    },

    /// Upload a file using an existing postage batch.
    Upload {
        /// Input file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Postage batch ID (hex, 32 bytes)
        #[arg(long, value_name = "BATCH_ID")]
        batch: String,

        /// Batch depth (typically 17-24)
        #[arg(long, default_value_t = 20)]
        depth: u8,

        /// Private key (hex, 32 bytes — batch owner's signer)
        #[arg(long, value_name = "KEY")]
        key: String,

        /// peers.json path
        #[arg(long, default_value = "peers.json", value_name = "FILE")]
        peerlist: PathBuf,

        /// Number of peers to try per chunk before giving up
        #[arg(long, default_value_t = 10)]
        max_retries: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let level = if cli.debug {
        Level::DEBUG
    } else if cli.verbose {
        Level::INFO
    } else {
        Level::WARN
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cfg = TransportConfig {
        timeout: Duration::from_secs(cli.timeout),
        network_id: cli.network_id,
    };
    let doh = Doh::with_url(&cli.doh_url);

    match cli.command {
        Commands::Discover { peer, output, wait, append } => {
            let signer = SwarmSigner::random(cli.network_id);
            let transport = Transport::new(signer, cfg);
            let bootstrap: Multiaddr = peer.parse()?;
            let discovered = discover(&transport, &doh, &bootstrap, Duration::from_secs(wait)).await?;
            println!("discovered {} peers", discovered.len());

            let mut store = if append { PeerStore::load_or_create(&output) } else { PeerStore::new() };
            for p in discovered {
                store.upsert(p);
            }
            store.save(&output)?;
            println!("wrote {} peers to {}", store.len(), output.display());
        }

        Commands::Fetch { hash, output, path, list, peerlist, max_retries } => {
            let signer = SwarmSigner::random(cli.network_id);
            let transport = Transport::new(signer, cfg);
            let peers = PeerStore::load_or_create(&peerlist);
            if peers.is_empty() {
                return Err(format!("peerlist {} is empty — run `isheika discover` first", peerlist.display()).into());
            }

            if list {
                let entries = list_manifest(&transport, &peers, &hash, max_retries).await?;
                println!("{} entries:", entries.len());
                for e in entries {
                    let ct = e.content_type.as_deref().unwrap_or("-");
                    println!("  {}  {}  [{}]", e.reference, e.path, ct);
                }
            } else {
                let output = output.ok_or("--output is required (omit only with --list)")?;
                if let Some(p) = path {
                    let (bytes, content_type) = fetch_manifest_path(&transport, &peers, &hash, &p, max_retries).await?;
                    std::fs::write(&output, &bytes)?;
                    let ct = content_type.as_deref().unwrap_or("-");
                    println!("fetched {} bytes ({}) -> {}", bytes.len(), ct, output.display());
                } else {
                    let bytes = fetch_bytes(&transport, &peers, &hash, max_retries).await?;
                    std::fs::write(&output, &bytes)?;
                    println!("fetched {} bytes -> {}", bytes.len(), output.display());
                }
            }
        }

        Commands::Upload { file, batch, depth, key, peerlist, max_retries } => {
            let data = std::fs::read(&file)?;
            let signer = SwarmSigner::from_hex(&key, cli.network_id)?;
            let transport = Transport::new(signer.clone(), cfg);
            let peers = PeerStore::load_or_create(&peerlist);
            if peers.is_empty() {
                return Err(format!("peerlist {} is empty", peerlist.display()).into());
            }
            let root = upload_bytes(&transport, &peers, &signer, &batch, depth, &data, max_retries).await?;
            println!("uploaded {} bytes — root: {}", data.len(), hex::encode(root.as_bytes()));
        }
    }

    Ok(())
}

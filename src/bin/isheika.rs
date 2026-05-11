//! isheika CLI — `discover` / `fetch` / `upload` against the Swarm network
//! over libp2p WebSocket only, with DoH-only DNS resolution.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use isheika::client::{
    fetch_bytes_ex, fetch_manifest_path_ex, list_manifest_ex, upload_bytes_ex, upload_collection,
    upload_file_with_manifest_ex, DEFAULT_DISCOVER_CONCURRENCY, DEFAULT_FETCH_CONCURRENCY,
    DEFAULT_UPLOAD_CONCURRENCY,
};
use isheika::client::discover_recursive_with_concurrency;
use isheika::{
    Doh, PeerStore, SwarmSigner, Transport, TransportConfig, UploadFile,
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

    /// Per-operation timeout in seconds (one pushsync or retrieval
    /// round-trip on an already-open session).
    #[arg(long, global = true, default_value_t = 10, value_name = "SECS")]
    timeout: u64,

    /// Wall-clock budget for opening a session to a peer (libp2p dial +
    /// identify + handshake + pricing). Set low (1-3 s) so dead/NAT'd
    /// peers fail fast; live peers usually answer in well under a
    /// second. Independent of `--timeout`.
    #[arg(long, global = true, default_value_t = 3, value_name = "SECS")]
    dial_timeout: u64,

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

        /// Number of recursive discovery hops. 1 = bootnode only. 2-3 is
        /// recommended for upload workloads — chunk pushes need a peer
        /// near each chunk's address, so a broader peerlist matters.
        #[arg(long, default_value_t = 1)]
        rounds: usize,

        /// Peers to dial in parallel per round. Each dial holds the hive
        /// stream open for `--wait` seconds, so a higher value finishes a
        /// 70-peer round in `ceil(70/N) × wait` seconds instead of
        /// `70 × wait` seconds.
        #[arg(long, default_value_t = DEFAULT_DISCOVER_CONCURRENCY)]
        discover_concurrency: usize,

        /// After discovery, probe each peer's underlay to verify
        /// reachability. The results are written into peers.json's
        /// reachability cache so future operations skip known-dead
        /// peers up-front (saving ~timeout seconds per dead peer).
        #[arg(long)]
        healthcheck: bool,

        /// Concurrency for the healthcheck probe phase.
        #[arg(long, default_value_t = 64)]
        healthcheck_concurrency: usize,
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

        /// Cap on peers tried per chunk before giving up. `0` means no
        /// cap — march through every peer in the peerlist (proximity-
        /// ordered) until one yields the chunk. Useful when the chunk
        /// neighborhood isn't densely represented in your peerlist:
        /// non-neighbor Bee nodes will forward the request through their
        /// own kademlia table.
        #[arg(long, default_value_t = 0)]
        max_retries: usize,

        /// Number of peers to race in parallel per chunk request.
        /// Slow/dead peers no longer block faster ones — whoever responds
        /// first with a valid chunk wins. Set to 1 for strict closest-first
        /// sequential behavior.
        #[arg(long, default_value_t = DEFAULT_FETCH_CONCURRENCY)]
        concurrency: usize,
    },

    /// Upload a file using an existing postage batch. Wraps the file in a
    /// single-entry mantaray manifest with the file's basename as path. The
    /// returned root resolves to the file via `fetch <root> --path <name>`.
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

        /// Upload raw chunks only, skip manifest creation.
        #[arg(long)]
        raw: bool,

        /// Override the manifest path (default: file basename).
        #[arg(long, value_name = "PATH")]
        manifest_path: Option<String>,

        /// Override the Content-Type metadata (default: auto-detected from extension).
        #[arg(long, value_name = "MIME")]
        content_type: Option<String>,

        /// Number of peer sessions to keep open in parallel during upload.
        /// Each session reuses a single libp2p connection for many chunks,
        /// so a small pool (4-16) is usually plenty.
        #[arg(long, default_value_t = DEFAULT_UPLOAD_CONCURRENCY)]
        concurrency: usize,

        /// Treat the input as a tar archive (bee's `application/x-tar`
        /// collection upload): unpack and upload each regular file as its
        /// own entry in a multi-entry mantaray, addressable individually.
        /// Auto-enabled for `*.tar` and `application/x-tar`.
        #[arg(long)]
        collection: bool,

        /// (Collection only) Filename served when the root manifest is
        /// fetched without a sub-path. Equivalent to bee's
        /// `Swarm-Index-Document` header. Typically `index.html`.
        #[arg(long, value_name = "FILE", requires = "collection")]
        index_document: Option<String>,

        /// (Collection only) Filename served on lookups that miss.
        /// Equivalent to bee's `Swarm-Error-Document` header.
        #[arg(long, value_name = "FILE", requires = "collection")]
        error_document: Option<String>,
    },
}

/// Parse a tar archive into a flat list of `UploadFile`s (regular files
/// only), matching bee's `pkg/api/dirs.go::tarReader::Next` semantics.
fn read_tar_files(bytes: &[u8]) -> Result<Vec<UploadFile>, Box<dyn std::error::Error>> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
    let mut out = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let header = entry.header().clone();
        if !header.entry_type().is_file() {
            continue;
        }
        let path = entry.path()?.to_string_lossy().into_owned();
        let path = path.trim_start_matches("./").to_string();
        if path.is_empty() || path == "." {
            continue;
        }
        let mut data = Vec::with_capacity(header.size().unwrap_or(0) as usize);
        std::io::Read::read_to_end(&mut entry, &mut data)?;
        let content_type = guess_content_type(&path).map(str::to_string);
        out.push(UploadFile { path, content_type, data });
    }
    Ok(out)
}

fn guess_content_type(path: &str) -> Option<&'static str> {
    let lower = path.to_ascii_lowercase();
    let ext = lower.rsplit('.').next()?;
    match ext {
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "svg" => Some("image/svg+xml"),
        "txt" => Some("text/plain"),
        "html" | "htm" => Some("text/html"),
        "css" => Some("text/css"),
        "js" | "mjs" => Some("application/javascript"),
        "json" => Some("application/json"),
        "pdf" => Some("application/pdf"),
        "zip" => Some("application/zip"),
        "tar" => Some("application/x-tar"),
        "mp4" => Some("video/mp4"),
        "mp3" => Some("audio/mpeg"),
        "wav" => Some("audio/wav"),
        _ => None,
    }
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
        dial_timeout: Duration::from_secs(cli.dial_timeout),
        network_id: cli.network_id,
    };
    let doh = Doh::with_url(&cli.doh_url);

    match cli.command {
        Commands::Discover { peer, output, wait, append, rounds, discover_concurrency, healthcheck, healthcheck_concurrency } => {
            let signer = SwarmSigner::random(cli.network_id);
            let transport = Transport::new(signer, cfg);
            let bootstrap: Multiaddr = peer.parse()?;
            let discovered = discover_recursive_with_concurrency(
                &transport,
                &doh,
                &bootstrap,
                Duration::from_secs(wait),
                rounds.max(1),
                discover_concurrency,
            )
            .await?;
            println!("discovered {} peers ({} hop(s))", discovered.len(), rounds);

            let mut store = if append { PeerStore::load_or_create(&output) } else { PeerStore::new() };
            for p in discovered {
                store.upsert(p);
            }

            if healthcheck {
                println!("probing {} peers for reachability...", store.len());
                isheika::client::healthcheck_peers(&transport, &store, healthcheck_concurrency).await;
                isheika::peers::apply_log(&mut store, transport.reachability_log());
            }

            store.save(&output)?;
            println!("wrote {} peers to {}", store.len(), output.display());
        }

        Commands::Fetch { hash, output, path, list, peerlist, max_retries, concurrency } => {
            let signer = SwarmSigner::random(cli.network_id);
            let transport = Transport::new(signer, cfg);
            let mut peers = PeerStore::load_or_create(&peerlist);
            if peers.is_empty() {
                return Err(format!("peerlist {} is empty — run `isheika discover` first", peerlist.display()).into());
            }

            let result: Result<(), Box<dyn std::error::Error>> = (async {
                if list {
                    let entries = list_manifest_ex(&transport, &peers, &hash, max_retries, concurrency).await?;
                    println!("{} entries:", entries.len());
                    for e in entries {
                        let ct = e.content_type.as_deref().unwrap_or("-");
                        println!("  {}  {}  [{}]", e.reference, e.path, ct);
                    }
                    Ok(())
                } else {
                    let output = output.ok_or("--output is required (omit only with --list)")?;
                    if let Some(p) = path {
                        let (bytes, content_type) = fetch_manifest_path_ex(&transport, &peers, &hash, &p, max_retries, concurrency).await?;
                        std::fs::write(&output, &bytes)?;
                        let ct = content_type.as_deref().unwrap_or("-");
                        println!("fetched {} bytes ({}) -> {}", bytes.len(), ct, output.display());
                    } else {
                        let bytes = fetch_bytes_ex(&transport, &peers, &hash, max_retries, concurrency).await?;
                        std::fs::write(&output, &bytes)?;
                        println!("fetched {} bytes -> {}", bytes.len(), output.display());
                    }
                    Ok(())
                }
            }).await;

            // Persist reachability observations back to peers.json on
            // both success and error so the next run starts faster.
            isheika::peers::apply_log(&mut peers, transport.reachability_log());
            let _ = peers.save(&peerlist);
            result?;
        }

        Commands::Upload {
            file,
            batch,
            depth,
            key,
            peerlist,
            max_retries,
            raw,
            manifest_path,
            content_type,
            concurrency,
            collection,
            index_document,
            error_document,
        } => {
            let signer = SwarmSigner::from_hex(&key, cli.network_id)?;
            let transport = Transport::new(signer.clone(), cfg);
            let mut peers = PeerStore::load_or_create(&peerlist);
            if peers.is_empty() {
                return Err(format!("peerlist {} is empty", peerlist.display()).into());
            }

            // Auto-detect tar by extension if the user didn't pass --raw.
            let is_tar = file
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("tar"))
                .unwrap_or(false);
            if (collection || (is_tar && !raw)) && !raw {
                let bytes = std::fs::read(&file)?;
                let files = read_tar_files(&bytes)?;
                if files.is_empty() {
                    return Err("tar archive contains no regular files".into());
                }
                let n_files = files.len();
                let total: usize = files.iter().map(|f| f.data.len()).sum();
                let root = upload_collection(
                    &transport,
                    &peers,
                    &signer,
                    &batch,
                    depth,
                    files,
                    index_document.as_deref(),
                    error_document.as_deref(),
                    max_retries,
                    concurrency,
                )
                .await?;
                println!(
                    "uploaded {} files ({} bytes) — manifest root: {}",
                    n_files,
                    total,
                    hex::encode(root.as_bytes())
                );
                println!("retrieve a file with: isheika fetch {} --path <name> -o <out>",
                    hex::encode(root.as_bytes()));
                println!("list contents with: isheika fetch {} --list", hex::encode(root.as_bytes()));
                isheika::peers::apply_log(&mut peers, transport.reachability_log());
                let _ = peers.save(&peerlist);
                return Ok(());
            }

            let data = std::fs::read(&file)?;
            if raw {
                let root = upload_bytes_ex(
                    &transport,
                    &peers,
                    &signer,
                    &batch,
                    depth,
                    &data,
                    max_retries,
                    concurrency,
                )
                .await?;
                println!("uploaded {} bytes — root (raw): {}", data.len(), hex::encode(root.as_bytes()));
            } else {
                let path = manifest_path.unwrap_or_else(|| {
                    file.file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| "file".to_string())
                });
                let ct = content_type.or_else(|| guess_content_type(&path).map(str::to_string));
                let root = upload_file_with_manifest_ex(
                    &transport,
                    &peers,
                    &signer,
                    &batch,
                    depth,
                    &data,
                    &path,
                    ct.as_deref(),
                    max_retries,
                    concurrency,
                )
                .await?;
                let display_ct = ct.as_deref().unwrap_or("-");
                println!(
                    "uploaded {} bytes ({}) — manifest root: {}",
                    data.len(),
                    display_ct,
                    hex::encode(root.as_bytes())
                );
                println!("retrieve with: isheika fetch {} --path {} -o {}", hex::encode(root.as_bytes()), path, path);
            }

            isheika::peers::apply_log(&mut peers, transport.reachability_log());
            let _ = peers.save(&peerlist);
        }
    }

    Ok(())
}

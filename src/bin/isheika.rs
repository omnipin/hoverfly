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

        /// Connect to a running daemon instead of executing the fetch
        /// in this process. See `isheika daemon`.
        #[cfg(unix)]
        #[arg(long, value_name = "SOCKET")]
        daemon: Option<PathBuf>,
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

        /// Connect to a running daemon instead of executing the upload
        /// in this process. The daemon must own a warm session pool —
        /// see `isheika daemon`. Mutually exclusive with the in-process
        /// peerlist; the daemon's own peerlist is used.
        #[cfg(unix)]
        #[arg(long, value_name = "SOCKET")]
        daemon: Option<PathBuf>,
    },

    /// Run a long-lived daemon that holds a warm session pool across
    /// upload/fetch requests. Listens on a unix socket; the same CLI
    /// (`upload --daemon <socket>` / `fetch --daemon <socket>`)
    /// connects in client mode. Reduces per-request session pool
    /// fill from ~3-10 s to ~0 s.
    #[cfg(unix)]
    Daemon {
        /// Unix socket path the daemon listens on. Removed on graceful
        /// shutdown; stale sockets from crashed daemons are unlinked
        /// at startup.
        #[arg(long, value_name = "PATH")]
        socket: PathBuf,

        /// peers.json path (loaded at startup, saved on shutdown).
        #[arg(long, default_value = "peers.json", value_name = "FILE")]
        peerlist: PathBuf,

        /// Target size of the warm session pool. The daemon dials this
        /// many sessions on the first upload and keeps them open
        /// (auto-rotated via pre-warm) for all subsequent requests.
        #[arg(long, default_value_t = 16)]
        pool_size: usize,

        /// libp2p multiaddr to bind for inbound bee-protocol
        /// connections. When set (default `/ip4/0.0.0.0/tcp/1634/ws`),
        /// the daemon serves retrieval requests from its local cache of
        /// uploaded chunks, so freshly-uploaded roots are immediately
        /// retrievable by any bee peer / gateway that routes back to
        /// us. Requires `--identity`.
        #[arg(long, value_name = "MULTIADDR",
            default_value = "/ip4/0.0.0.0/tcp/1634/ws")]
        listen: String,

        /// Disable the inbound listener. Daemon becomes a pure
        /// outbound client (legacy behaviour).
        #[arg(long, conflicts_with = "listen")]
        no_listen: bool,

        /// Daemon identity (hex secp256k1 private key, 32 bytes).
        /// Required when the inbound listener is active — fixes the
        /// daemon's overlay address across restarts so peers that
        /// learn of us via hive can re-dial.
        #[arg(long, value_name = "HEX")]
        identity: Option<String>,

        /// Publicly-routable multiaddr to advertise to bee peers
        /// (without the `/p2p/<peer-id>` tail — appended automatically
        /// from `--identity`). e.g. `/ip4/167.17.40.160/tcp/1634/ws`.
        /// Required for the listener to actually be useful: peers add
        /// us to their kademlia tables based on this address, and
        /// retrieval lookups route here. When omitted, listener still
        /// binds but no bee will dial us back.
        #[arg(long, value_name = "MULTIADDR")]
        advertise: Option<String>,
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
        advertise: None,
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

        Commands::Fetch { hash, output, path, list, peerlist, max_retries, concurrency,
                          #[cfg(unix)] daemon } => {
            #[cfg(unix)]
            if let Some(sock) = daemon {
                let output = output.ok_or("--output is required when using --daemon")?;
                let req = isheika::daemon::Request::Fetch(isheika::daemon::FetchRequest {
                    hash,
                    path,
                    output: output.clone(),
                    max_retries,
                    concurrency,
                });
                let resp = isheika::daemon::call(&sock, &req).await?;
                match resp {
                    isheika::daemon::Response::Fetched { bytes_written, content_type } => {
                        let ct = content_type.as_deref().unwrap_or("-");
                        println!("fetched {} bytes ({}) -> {} (via daemon)",
                            bytes_written, ct, output.display());
                        return Ok(());
                    }
                    isheika::daemon::Response::Err { message } => {
                        return Err(format!("daemon error: {message}").into());
                    }
                    other => return Err(format!("unexpected daemon response: {:?}", other).into()),
                }
            }
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
            #[cfg(unix)] daemon,
        } => {
            #[cfg(unix)]
            if let Some(sock) = daemon {
                let req = isheika::daemon::Request::Upload(isheika::daemon::UploadRequest {
                    file: file.clone(),
                    batch: batch.clone(),
                    depth,
                    key: key.clone(),
                    max_retries,
                    concurrency,
                    raw,
                    collection,
                    manifest_path: manifest_path.clone(),
                    content_type: content_type.clone(),
                    index_document: index_document.clone(),
                    error_document: error_document.clone(),
                });
                let resp = isheika::daemon::call(&sock, &req).await?;
                match resp {
                    isheika::daemon::Response::Uploaded { root, bytes } => {
                        println!("uploaded {} bytes — manifest root: {} (via daemon)", bytes, root);
                        return Ok(());
                    }
                    isheika::daemon::Response::Err { message } => {
                        return Err(format!("daemon error: {message}").into());
                    }
                    other => return Err(format!("unexpected daemon response: {:?}", other).into()),
                }
            }

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

        #[cfg(unix)]
        Commands::Daemon { socket, peerlist, pool_size, listen, no_listen, identity, advertise } => {
            // Install a Ctrl-C handler that sends a shutdown request to
            // ourselves via the socket, triggering graceful peerlist save.
            let sock_path = socket.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let _ = isheika::daemon::call(
                        &sock_path,
                        &isheika::daemon::Request::Shutdown,
                    )
                    .await;
                }
            });

            let listen_cfg = if no_listen {
                None
            } else {
                let ma: Multiaddr = listen.parse()
                    .map_err(|e| format!("invalid --listen multiaddr: {e}"))?;
                let id_hex = identity
                    .ok_or("--identity <HEX> is required when the inbound listener is active (or pass --no-listen)")?;
                let signer = SwarmSigner::from_hex(&id_hex, cli.network_id)?;
                // Derive our libp2p peer-id from the same identity so we
                // can append `/p2p/<id>` to the user's advertise addr.
                let advertised = advertise
                    .map(|s| -> Result<Multiaddr, Box<dyn std::error::Error>> {
                        let base: Multiaddr = s.parse()
                            .map_err(|e| format!("invalid --advertise multiaddr: {e}"))?;
                        let already_has_p2p = base.iter()
                            .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_)));
                        if already_has_p2p {
                            Ok(base)
                        } else {
                            let peer_id = isheika::inbound::peer_id_from_identity(&signer);
                            Ok(base.with(libp2p::multiaddr::Protocol::P2p(peer_id)))
                        }
                    })
                    .transpose()?;
                println!(
                    "daemon identity: overlay={} eth={}{}",
                    hex::encode(signer.overlay()),
                    hex::encode(signer.eth_address()),
                    advertised.as_ref().map(|a| format!(" advertise={a}")).unwrap_or_default(),
                );
                Some(isheika::daemon::ListenConfig {
                    listen: ma,
                    advertise: advertised,
                    identity: signer,
                })
            };

            isheika::daemon::run(
                socket,
                peerlist,
                cli.network_id,
                pool_size,
                Duration::from_secs(cli.dial_timeout),
                Duration::from_secs(cli.timeout),
                listen_cfg,
            )
            .await?;
        }
    }

    Ok(())
}

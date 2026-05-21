//! isheika CLI — `discover` / `fetch` / `upload` against the Swarm network
//! over libp2p WebSocket only, with DoH-only DNS resolution.

use std::path::PathBuf;
use std::time::Duration;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use isheika::client::{
    fetch_bytes_ex, fetch_manifest_path_ex, list_manifest_ex, upload_bytes_ex, upload_collection,
    upload_file_with_manifest_ex, ProgressFn, DEFAULT_DISCOVER_CONCURRENCY,
    DEFAULT_FETCH_CONCURRENCY, DEFAULT_UPLOAD_CONCURRENCY,
};

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

    /// Debug output (debug-level logging)
    #[arg(short, long, global = true)]
    debug: bool,

    /// Trace output (trace-level logging; very noisy, intended for
    /// profile/diagnostic targets like `isheika::profile`).
    #[arg(long, global = true)]
    trace: bool,

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

    /// Per-connection cap on concurrent outbound substream upgrades.
    /// Higher means more substream opens negotiate in parallel
    /// (~lower open latency); lower means less yamux flow-control
    /// contention per-stream (~lower push latency). Sweet spot is
    /// workload-dependent; see `PERFORMANCE.md`. Default 64.
    #[arg(long, global = true,
          default_value_t = isheika::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES,
          value_name = "N")]
    substream_upgrade_cap: usize,

    /// Multiplier applied to the dispatcher's in-flight chunk
    /// buffer. The buffer's base cap is `128 × mult`, floored at
    /// pool size. At `mult=1` (default) the buffer matches the
    /// original 128 cap. Increase together with `--concurrency`:
    /// per-session in-flight stays ~constant while total grows.
    /// Sweet spot empirically `--concurrency 512 --buffer-multiplier 4`
    /// on a 3000+ peer pool (≈ 1 MB/s on a VPS); larger overshoots
    /// into yamux contention and regresses. See PERFORMANCE.md
    /// "Pool + buffer scaling" for the sweep.
    #[arg(long, global = true, default_value_t = 1, value_name = "N")]
    buffer_multiplier: usize,

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

        /// Hard deadline (seconds) for the per-peer hive listen
        /// window. We accept multiple `peers` envelopes from each
        /// queried peer (bee splits gossip into 30-peer batches over
        /// separate streams) and short-circuit out 750 ms after the
        /// last batch lands. Bee finishes its `Announce` burst in
        /// well under 1 s on a healthy mainnet path, so this is a
        /// timeout for stragglers and bad links, not the expected
        /// per-peer cost.
        #[arg(long, default_value_t = 5)]
        wait: u64,

        /// Append to existing peers.json instead of overwriting
        #[arg(long)]
        append: bool,

        /// Number of recursive discovery hops. 1 = bootnode only. 2-3 is
        /// recommended for upload workloads — chunk pushes need a peer
        /// near each chunk's address, so a broader peerlist matters.
        #[arg(long, default_value_t = 1)]
        rounds: usize,

        /// Peers to dial in parallel per round. Each dial holds the
        /// hive stream open until bee finishes its gossip burst
        /// (typically ~1 s; capped at `--wait`), so a higher value
        /// finishes a 70-peer round in roughly `ceil(70/N) × ~1 s`
        /// instead of `70 × ~1 s`.
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
        /// Auto-enabled for `*.tar`. Pass to force collection mode on
        /// inputs without a `.tar` extension.
        #[arg(long)]
        collection: bool,

        /// (Collection mode) Filename served when the root manifest is
        /// fetched without a sub-path. Equivalent to bee's
        /// `Swarm-Index-Document` header. Defaults to `index.html` when
        /// running in collection mode; pass `--index-document ""` to
        /// disable. Ignored for non-collection uploads.
        #[arg(long, value_name = "FILE")]
        index_document: Option<String>,

        /// (Collection mode) Filename served on lookups that miss.
        /// Equivalent to bee's `Swarm-Error-Document` header. Ignored
        /// for non-collection uploads.
        #[arg(long, value_name = "FILE")]
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

        /// (Experimental) libp2p multiaddr to bind an inbound
        /// bee-protocol listener on. When set together with
        /// `--identity` and `--advertise`, the daemon serves
        /// retrieval requests from its local upload cache. Marginal
        /// help in practice — bee peers route retrieval by proximity
        /// to the chunk address, so a single-overlay daemon is
        /// rarely the closest peer. Default behaviour (no flag) is
        /// outbound-only.
        #[arg(long, value_name = "MULTIADDR")]
        listen: Option<String>,

        /// Daemon identity (hex secp256k1 private key, 32 bytes).
        /// Required only when `--listen` is set.
        #[arg(long, value_name = "HEX")]
        identity: Option<String>,

        /// Publicly-routable multiaddr to advertise. Only used with
        /// `--listen`. Without it the listener accepts dial-backs
        /// from bee but bee never adds us to its routing tables.
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
        let content_type = guess_content_type(&path);
        out.push(UploadFile { path, content_type, data });
    }
    Ok(out)
}

/// Build a CLI-side `indicatif` progress bar wrapped in a [`ProgressFn`]
/// callback the library can call after each successful chunk push.
///
/// Returns `None` when stdout isn't a TTY (piping / redirecting to file)
/// so we don't pollute log captures with terminal escape codes; the
/// library then falls back to the existing tracing `pushed N/M chunks`
/// lines, which work fine in log files.
///
/// The bar's length starts at 0 because the library only knows the
/// final chunk count after stamping. The first callback invocation
/// resizes via `set_length`. The bar finalises itself the moment
/// `done == total`, then the closure's `ProgressBar` clone is dropped
/// when the caller releases the `ProgressFn` Arc.
fn make_progress_bar() -> Option<ProgressFn> {
    let pb = ProgressBar::new(0);
    if pb.is_hidden() {
        return None;
    }
    pb.set_style(
        ProgressStyle::with_template(
            "  pushing {bar:40.cyan/blue} {pos}/{len} chunks  ({percent}%, eta {eta})",
        )
        .ok()?
        .progress_chars("##-"),
    );
    pb.enable_steady_tick(Duration::from_millis(250));
    let cb: ProgressFn = Arc::new(move |done: usize, total: usize| {
        if pb.length() != Some(total as u64) {
            pb.set_length(total as u64);
        }
        pb.set_position(done as u64);
        if done >= total {
            pb.finish_and_clear();
        }
    });
    Some(cb)
}

/// Convert a 64-char hex Swarm reference to a multibase-encoded CIDv1
/// (`b...` lowercase base32). Returns `None` on malformed input.
fn root_hex_to_cid(root_hex: &str) -> Option<String> {
    let bytes = hex::decode(root_hex).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Some(isheika::cid::reference_to_cid(&arr))
}

fn guess_content_type(path: &str) -> Option<String> {
    isheika::mime::guess_from_path(path)
}

/// Print the session-retirement-cause counters to stderr at upload end.
/// See `isheika::transport::diag` for what each counter means.
fn print_session_retire_diag() {
    use std::sync::atomic::Ordering;
    use isheika::transport::diag;
    let dead_low = diag::DEAD_RETIRE_LOW_GHOST.load(Ordering::Relaxed);
    let dead_prewarm = diag::DEAD_RETIRE_PREWARM_GHOST.load(Ordering::Relaxed);
    let dead_high = diag::DEAD_RETIRE_HIGH_GHOST.load(Ordering::Relaxed);
    let ghost_retire = diag::GHOST_RETIRE.load(Ordering::Relaxed);
    let max_pushes_retire = diag::MAX_PUSHES_RETIRE.load(Ordering::Relaxed);
    let total = dead_low + dead_prewarm + dead_high + ghost_retire + max_pushes_retire;
    if total > 0 {
        eprintln!(
            "session-retire: dead_low_ghost={} dead_prewarm_ghost={} dead_high_ghost={} ghost_threshold={} max_pushes={} total={}",
            dead_low, dead_prewarm, dead_high, ghost_retire, max_pushes_retire, total,
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // The library reads ISHEIKA_BUFFER_MULT from the environment at
    // upload dispatch time (`src/client.rs::push_chunks_inner`). The
    // `--buffer-multiplier` CLI flag plumbs the same knob without
    // requiring the user to export an env var. CLI value > 1
    // overrides the env var; CLI value 1 (default) defers to env so
    // an explicit `ISHEIKA_BUFFER_MULT=N` from the shell still works.
    if cli.buffer_multiplier > 1 {
        // Safety: single-threaded at this point (main hasn't done
        // anything else yet); unsafe in nightly's edition-2024
        // because env::set_var is process-wide.
        unsafe {
            std::env::set_var("ISHEIKA_BUFFER_MULT", cli.buffer_multiplier.to_string());
        }
    }

    let level = if cli.trace {
        Level::TRACE
    } else if cli.debug {
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
        max_concurrent_substream_upgrades: cli.substream_upgrade_cap,
    };
    let doh = Doh::with_url(&cli.doh_url);

    match cli.command {
        Commands::Discover { peer, output, wait, append, rounds, discover_concurrency, healthcheck, healthcheck_concurrency } => {
            let signer = SwarmSigner::random(cli.network_id);
            let transport = Transport::new(signer, cfg);
            let bootstrap: Multiaddr = peer.parse()?;
            let progress: isheika::client::DiscoverProgressFn = Arc::new(|ev| {
                use isheika::client::DiscoverEvent::*;
                match ev {
                    RoundStarted { round, total_rounds, frontier_size, total_peers_so_far } => {
                        println!(
                            "  round {round}/{total_rounds}: dialing {frontier_size} peer(s) (have {total_peers_so_far} so far)"
                        );
                    }
                    RoundFinished { round, total_rounds, new_peers_this_round, total_peers } => {
                        println!(
                            "  round {round}/{total_rounds} done: +{new_peers_this_round} new (total {total_peers})"
                        );
                    }
                }
            });
            let discovered = isheika::client::discover_recursive_with_progress(
                &transport,
                &doh,
                &bootstrap,
                Duration::from_secs(wait),
                rounds.max(1),
                discover_concurrency,
                Some(progress),
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
                        let cid = root_hex_to_cid(&root);
                        println!("uploaded {} bytes — manifest root: {} (via daemon)", bytes, root);
                        if let Some(c) = cid.as_deref() {
                            println!("bzz.limo:   https://bzz.limo/bzz/{root}/");
                            println!("subdomain:  https://{c}.bzz.limo/");
                        }
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
                // Default the website index to `index.html`. An empty
                // string explicitly opts out (no root entry written).
                let index_doc = index_document
                    .as_deref()
                    .map(|s| if s.is_empty() { None } else { Some(s) })
                    .unwrap_or(Some("index.html"));
                let progress = make_progress_bar();
                let root = upload_collection(
                    &transport,
                    &peers,
                    &signer,
                    &batch,
                    depth,
                    files,
                    index_doc,
                    error_document.as_deref(),
                    max_retries,
                    concurrency,
                    progress.as_ref(),
                )
                .await?;
                drop(progress);
                let root_hex = hex::encode(root.as_bytes());
                println!(
                    "uploaded {} files ({} bytes) — manifest root: {}",
                    n_files, total, root_hex,
                );
                if let Some(c) = root_hex_to_cid(&root_hex) {
                    println!("bzz.limo:   https://bzz.limo/bzz/{root_hex}/");
                    println!("subdomain:  https://{c}.bzz.limo/");
                }
                println!("retrieve a file with: isheika fetch {root_hex} --path <name> -o <out>");
                println!("list contents with: isheika fetch {root_hex} --list");
                isheika::peers::apply_log(&mut peers, transport.reachability_log());
                let _ = peers.save(&peerlist);
                print_session_retire_diag();
                return Ok(());
            }

            let data = std::fs::read(&file)?;
            if raw {
                let progress = make_progress_bar();
                let root = upload_bytes_ex(
                    &transport,
                    &peers,
                    &signer,
                    &batch,
                    depth,
                    &data,
                    max_retries,
                    concurrency,
                    progress.as_ref(),
                )
                .await?;
                drop(progress);
                let root_hex = hex::encode(root.as_bytes());
                println!("uploaded {} bytes — root (raw): {}", data.len(), root_hex);
                if let Some(c) = root_hex_to_cid(&root_hex) {
                    println!("cid: {c}");
                }
            } else {
                let path = manifest_path.unwrap_or_else(|| {
                    file.file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| "file".to_string())
                });
                let ct = content_type.or_else(|| guess_content_type(&path));
                let progress = make_progress_bar();
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
                    progress.as_ref(),
                )
                .await?;
                drop(progress);
                let display_ct = ct.as_deref().unwrap_or("-");
                let root_hex = hex::encode(root.as_bytes());
                println!(
                    "uploaded {} bytes ({}) — manifest root: {}",
                    data.len(), display_ct, root_hex,
                );
                if let Some(c) = root_hex_to_cid(&root_hex) {
                    println!("bzz.limo:   https://bzz.limo/bzz/{root_hex}/{path}");
                    println!("subdomain:  https://{c}.bzz.limo/{path}");
                }
                println!("retrieve with: isheika fetch {root_hex} --path {path} -o {path}");
            }

            isheika::peers::apply_log(&mut peers, transport.reachability_log());
            let _ = peers.save(&peerlist);
            print_session_retire_diag();
        }

        #[cfg(unix)]
        Commands::Daemon { socket, peerlist, pool_size, listen, identity, advertise } => {
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

            let listen_cfg = match listen {
                Some(s) => {
                    let ma: Multiaddr = s.parse()
                        .map_err(|e| format!("invalid --listen multiaddr: {e}"))?;
                    let id_hex = identity
                        .ok_or("--identity <HEX> is required when --listen is set")?;
                    let signer = SwarmSigner::from_hex(&id_hex, cli.network_id)?;
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
                }
                None => None,
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

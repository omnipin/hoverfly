// Boot shell — the top document served for any navigation to a content
// subdomain (<cid>.bzz.<host>). It cannot stream the site's top-level HTML
// itself (the hoverfly node lives in a cross-origin daemon and the SW can't
// reach it before a client exists), so instead it:
//   1. registers + waits for the content-origin service worker,
//   2. embeds the cross-origin daemon broker iframe and opens an RPC channel,
//   3. mints a second daemon port and hands it to the service worker,
//   4. renders the actual website in a full-viewport child iframe, whose
//      requests the SW now serves as Swarm content via the daemon.

import { CONTENT_MARKER, DAEMON_FRAME_PATH, GW_VERSION, SW_SCRIPT } from '../shared/config.ts'
import { DaemonRpc, mintDaemonPort, type DaemonStatus } from '../shared/protocol.ts'
import { daemonOrigin, parseHost } from '../shared/parse-request.ts'
import { cidToReference } from '../shared/swarm-cid.ts'

/** This shell's OWN Swarm reference (hex), derived from its <cid>.bzz.<host>
 *  subdomain. The shared daemon broadcasts per-fetch progress phases tagged
 *  with the requesting CID (`phaseRef`); the shell shows a phase only when it's
 *  a daemon-lifecycle phase (no ref) or matches THIS reference — otherwise one
 *  site's loading overlay would display another site's in-flight file progress,
 *  since the daemon is shared across all content origins. Null if the host isn't
 *  a content subdomain (shouldn't happen for the boot shell) or the CID is
 *  unparseable — in which case we fall back to showing all phases. */
const OWN_REF: string | null = (() => {
  try {
    const host = parseHost(location.host)
    if (host.kind !== 'subdomain' || host.id == null) return null
    return cidToReference(host.id).refHex
  } catch { return null }
})()

async function main (): Promise<void> {
  if (window.top !== window) return // only the top shell bootstraps

  // Catch-all: log EVERY postMessage the shell receives, to debug the bridge.
  window.addEventListener('message', (e) => {
    if (e.data?.type === 'frame-error') {
      // frame.ts reuses this message type for a benign 'loaded' heartbeat as
      // well as real 'error'/'rejection' reports — only the latter are errors.
      if (e.data?.tag === 'error' || e.data?.tag === 'rejection') {
        console.error('[boot] FRAME ERROR', e.data?.tag, '·', e.data?.detail)
      } else {
        console.log('[boot] frame', e.data?.tag, '·', e.data?.detail)
      }
      return
    }
    // Only log messages that carry a recognizable type. The content site (and
    // its libraries) post plenty of internal, typeless self-messages; logging
    // every one floods the console (esp. since syncHead polls on an interval).
    if (e.data?.type != null) {
      console.log('[boot] MSG from', e.origin, 'type=', e.data.type, 'src?', e.source === window ? 'self' : 'other')
    }
  })
  console.log('[boot] shell start', location.href)
  const ui = renderShell()

  // The whole gateway hinges on a shared cross-origin daemon that lives in a
  // SharedWorker. SharedWorker is unsupported on Chrome for Android (and some
  // other mobile/embedded browsers), where the broker iframe's `new
  // SharedWorker(...)` throws — previously surfacing only as a generic 30s
  // "daemon frame did not load" timeout (blank page). Detect it up front and
  // show a clear, actionable message instead of a blank/hung page.
  if (typeof SharedWorker === 'undefined') {
    console.error('[boot] SharedWorker unsupported in this browser')
    ui.fail('This browser doesn\u2019t support SharedWorker, which the gateway needs to run the shared Swarm node. Try desktop Chrome, Edge, or Firefox.')
    return
  }
  // The wasm node uses shared memory (atomics), which requires the page to be
  // cross-origin isolated. If isolation headers didn't apply (e.g. a proxy
  // stripped them), wasm instantiation fails inside the daemon and every fetch
  // 503s. Surface that as a clear message rather than a stuck overlay.
  if (self.crossOriginIsolated === false) {
    console.error('[boot] page is not crossOriginIsolated')
    ui.fail('This page isn\u2019t cross-origin isolated, so the Swarm node can\u2019t start. This usually means the isolation headers were stripped by a proxy or extension.')
    return
  }

  let controller: ServiceWorker
  try {
    controller = await ensureControllingSW()
    console.log('[boot] SW controlling')
    ui.phase('Connecting to the Swarm daemon…', 'Bridging to the shared node…')
  } catch (e) {
    console.error('[boot] SW failed', e)
    ui.fail('Service worker failed to take control: ' + (e as Error).message)
    return
  }

  let rpcPort: MessagePort
  try {
    rpcPort = await connectDaemon()
    console.log('[boot] daemon bridge connected')
  } catch (e) {
    console.error('[boot] daemon bridge failed', e)
    ui.fail('Could not reach the shared daemon: ' + (e as Error).message)
    return
  }

  const rpc = new DaemonRpc(rpcPort)
  rpc.onStatus((s) => ui.status(s))
  void rpc.status() // nudge the daemon to start warming

  // Give the service worker its own direct channel to the daemon.
  controller.postMessage({ type: 'daemon-port' }, [mintDaemonPort(rpcPort)])
  console.log('[boot] handed daemon-port to SW; loading content', location.pathname + location.search)

  // The port above lives only in the SW's in-memory state, and the browser
  // terminates idle service workers at will (Chrome: ~30s idle). A restarted
  // SW instance broadcasts `request-daemon-port`; answer it by minting a fresh
  // port off our daemon channel. Without this, every fetch after an SW restart
  // waited out a 25s daemon-bridge timeout and 504'd — a dead black page even
  // though the daemon itself was still happily pulling chunks.
  navigator.serviceWorker.addEventListener('message', (e: MessageEvent) => {
    if (e.data?.type !== 'request-daemon-port') return
    const target = (e.source as ServiceWorker | null) ?? navigator.serviceWorker.controller
    if (target == null) return
    console.log('[boot] SW asked for a daemon port — re-minting')
    target.postMessage({ type: 'daemon-port' }, [mintDaemonPort(rpcPort)])
  })

  // Keepalive: ping the SW on an interval. Each ping dispatches a real
  // `message` event, which is a functional event and so extends the SW's
  // lifetime in Chrome (a pending MessagePort reply from the daemon does NOT
  // count as activity). This stops the browser from idle-killing the worker in
  // the middle of a long Swarm retrieval. The whole gateway needs a live SW for
  // every request anyway, so keeping it warm while a shell tab is open is the
  // intended behaviour, not a leak — the worker can still die the moment the
  // tab closes.
  setInterval(() => {
    navigator.serviceWorker.controller?.postMessage({ type: 'ping' })
  }, 20_000)

  // Load the real site into the content iframe. The marker tells the SW this
  // document navigation is Swarm content (not the boot shell) — see
  // CONTENT_MARKER. Without it the iframe nav passes through to the network and
  // re-loads boot.html, so the page stays blank and nothing is fetched.
  ui.loadContent(contentUrl(location.pathname, location.search))
}

/** Build the content-iframe URL: the requested path/query plus the SW content
 *  marker. Preserves any existing query params. */
function contentUrl (pathname: string, search: string): string {
  const params = new URLSearchParams(search)
  params.set(CONTENT_MARKER, '1')
  return pathname + '?' + params.toString()
}

async function ensureControllingSW (): Promise<ServiceWorker> {
  const reg = await navigator.serviceWorker.register(SW_SCRIPT, { scope: '/' })
  // Force an update check. A normal (non-private) tab that visited an EARLIER
  // deploy is already controlled by that deploy's (outdated) sw.js — e.g. one
  // built before the content-iframe routing fix, which passed the iframe nav
  // through and left the page blank. `controller` is non-null immediately in
  // that case, so without this we'd run against the stale SW. update() fetches
  // the new sw.js; if it differs, a new worker installs and we reload once it
  // takes control (below). This is why a fresh private tab worked but a normal
  // tab didn't.
  try { await reg.update() } catch { /* offline / transient — proceed */ }

  // A new worker is installing or waiting (i.e. the active controller is stale).
  // It calls skipWaiting()+clients.claim() on activate, which fires
  // `controllerchange`; reload once so the page runs under the new SW. Guard
  // the reload per DEPLOY VERSION so we reload at most once per upgrade, never
  // in a loop.
  const pending = reg.installing ?? reg.waiting
  const reloadKey = 'gw-reloaded-' + GW_VERSION
  if (pending != null && navigator.serviceWorker.controller != null &&
      sessionStorage.getItem(reloadKey) == null) {
    return await new Promise<ServiceWorker>((resolve) => {
      let reloaded = false
      const maybeReload = (): void => {
        if (reloaded) return
        reloaded = true
        sessionStorage.setItem(reloadKey, '1')
        location.reload()
      }
      navigator.serviceWorker.addEventListener('controllerchange', maybeReload)
      // Fallback: if the new worker never activates within 5s, proceed with
      // whatever controls the page rather than hanging.
      setTimeout(() => {
        if (reloaded) return
        navigator.serviceWorker.removeEventListener('controllerchange', maybeReload)
        if (navigator.serviceWorker.controller != null) {
          resolve(navigator.serviceWorker.controller)
        }
      }, 5000)
    })
  }

  await navigator.serviceWorker.ready
  if (navigator.serviceWorker.controller != null) return navigator.serviceWorker.controller
  return await new Promise<ServiceWorker>((resolve, reject) => {
    const onChange = (): void => {
      if (navigator.serviceWorker.controller != null) {
        navigator.serviceWorker.removeEventListener('controllerchange', onChange)
        resolve(navigator.serviceWorker.controller)
      }
    }
    navigator.serviceWorker.addEventListener('controllerchange', onChange)
    setTimeout(() => {
      if (navigator.serviceWorker.controller != null) { resolve(navigator.serviceWorker.controller); return }
      // One-shot reload so a freshly-installed SW can claim the page.
      if (sessionStorage.getItem('gw-reloaded') == null) {
        sessionStorage.setItem('gw-reloaded', '1')
        location.reload()
      } else {
        reject(new Error('service worker did not take control'))
      }
    }, 3000)
  })
}

function connectDaemon (): Promise<MessagePort> {
  const origin = daemonOrigin(location)
  const iframe = document.createElement('iframe')
  iframe.src = origin + DAEMON_FRAME_PATH
  iframe.style.display = 'none'
  iframe.setAttribute('allow', 'cross-origin-isolated')

  iframe.addEventListener('load', () => console.log('[boot] daemon iframe load event fired'))
  iframe.addEventListener('error', (e) => console.log('[boot] daemon iframe error event', e))
  console.log('[boot] embedding daemon frame', iframe.src)
  return new Promise<MessagePort>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('daemon frame did not load (no frame-ready from ' + origin + ')')), 30_000)
    const done = (): void => { window.removeEventListener('message', onMessage); clearTimeout(timeout) }
    const onMessage = (e: MessageEvent): void => {
      if (e.origin !== origin) return
      // The broker frame reports a fatal init failure (e.g. SharedWorker
      // unsupported — notably Firefox without module-worker support, where
      // `new SharedWorker(url, {type:'module'})` throws). Surface it
      // immediately with the real reason instead of waiting out the 30s
      // frame-ready timeout and showing a bare blank page.
      if (e.data?.type === 'frame-error' && e.data?.tag === 'error') {
        done()
        reject(new Error('daemon frame failed: ' + String(e.data?.detail ?? 'unknown error')))
        return
      }
      if (e.data?.type !== 'frame-ready') return
      console.log('[boot] got frame-ready from', e.origin)
      done()
      const channel = new MessageChannel()
      iframe.contentWindow?.postMessage({ type: 'connect' }, origin, [channel.port2])
      resolve(channel.port1)
    }
    window.addEventListener('message', onMessage)
    document.body.appendChild(iframe)
  })
}

// ---------------- minimal shell UI ----------------
interface ShellUi {
  loadContent: (path: string) => void
  status: (s: DaemonStatus) => void
  phase: (primary: string, sub?: string) => void
  fail: (msg: string) => void
}

function renderShell (): ShellUi {
  const root = document.getElementById('gw-root') ?? document.body

  const frame = document.createElement('iframe')
  frame.className = 'gw-content'
  frame.title = 'Swarm site'

  const pill = document.createElement('div')
  pill.className = 'gw-pill'
  pill.innerHTML = '<span class="gw-dot"></span><span class="gw-pill-text">starting daemon…</span>'
  pill.title = 'hoverfly gateway status — click to hide'
  pill.addEventListener('click', () => pill.classList.toggle('gw-hidden'))

  // Full-viewport loading overlay shown over the (initially blank, white)
  // content iframe until the real site finishes loading — or boot fails.
  const loading = document.createElement('div')
  loading.className = 'gw-loading'
  loading.innerHTML =
    '<div class="gw-spinner"></div>' +
    '<div class="gw-loading-text">Starting gateway…</div>' +
    '<div class="gw-loading-sub">Registering service worker…</div>'

  root.appendChild(frame)
  root.appendChild(pill)
  root.appendChild(loading)

  const text = pill.querySelector('.gw-pill-text') as HTMLElement
  const dot = pill.querySelector('.gw-dot') as HTMLElement
  const loadingText = loading.querySelector('.gw-loading-text') as HTMLElement
  const loadingSub = loading.querySelector('.gw-loading-sub') as HTMLElement

  let loaded = false
  let contentRequested = false

  // Put the overlay into a terminal error state with a message. Local so both
  // the returned `fail` and the in-shell content watchdog can call it.
  const failShell = (msg: string): void => {
    if (loaded) return // a revealed page shouldn't be hidden behind an error
    pill.classList.remove('gw-min', 'gw-hidden')
    dot.style.background = '#f85149'
    text.textContent = msg
    loading.classList.remove('gw-hide')
    loading.classList.add('gw-error')
    loadingText.textContent = 'Failed to load site'
    loadingSub.textContent = msg
  }

  // Reveal the loaded site by fading the overlay out (idempotent).
  const reveal = (): void => {
    if (loaded) return
    loaded = true
    loading.classList.add('gw-hide')
    setTimeout(() => loading.remove(), 500)
    setTimeout(() => pill.classList.add('gw-min'), 1500)
  }

  // Mirror the content iframe's <title> onto the top document so the browser
  // tab/titlebar shows the SITE's title, not the shell's static "Loading…"
  // placeholder. The tab only ever reflects the top-level document's title, and
  // the real site renders in a child iframe whose title never propagates — so
  // without this the tab is stuck on the boot placeholder forever. The content
  // frame is same-origin (<cid>.bzz.host, like the shell), so we can read its
  // title; client-side route changes (VitePress et al. are SPAs) update the
  // child's title, so we re-sync on an interval and via a MutationObserver.
  const syncTitle = (): void => {
    try {
      const doc = frame.contentDocument
      if (doc == null || (doc.URL ?? 'about:').startsWith('about:')) return
      const t = doc.title.trim()
      if (t !== '' && t !== document.title) document.title = t
    } catch { /* cross-origin or transiently inaccessible — leave the tab title as-is */ }
  }

  // Mirror the content iframe's favicon onto the top document, same rationale as
  // the title: the tab icon reflects the TOP document (the boot shell), whose
  // hardcoded /__gw__/favicon.svg is meant only as a fallback while loading. The
  // real site's <link rel="icon"> lives in the child iframe and the browser
  // ignores it for the tab, so the gateway icon would otherwise never give way
  // to the site's own. We lazily create a dedicated top-document <link> the
  // first time the site declares an icon, so the shell's fallback icon stays put
  // when a site ships none (rather than blanking the tab).
  let currentIconHref = ''
  const syncIcon = (): void => {
    try {
      const doc = frame.contentDocument
      if (doc == null || (doc.URL ?? 'about:').startsWith('about:')) return
      // Only adopt an icon the site EXPLICITLY declares. We don't probe
      // /favicon.ico: the content SW answers that with a 204 (see sw.ts), so
      // adopting it would blank the tab instead of keeping the shell fallback.
      const link = doc.querySelector<HTMLLinkElement>('link[rel~="icon"]')
      const href = link?.href ?? ''
      if (href === '' || href === currentIconHref) return
      currentIconHref = href

      // Chrome only re-reads the tab favicon when an icon <link> is (re)inserted
      // into <head>; mutating an existing link's .href is silently ignored. So
      // we remove ALL existing icon links (the shell fallback + any prior site
      // icon we added) and append a freshly-created node every time the href
      // changes, which reliably forces a re-evaluation.
      document.head.querySelectorAll('link[rel~="icon"], link[rel="apple-touch-icon"], link[rel="mask-icon"]')
        .forEach((el) => el.remove())
      const fresh = document.createElement('link')
      fresh.rel = 'icon'
      if (link?.type != null && link.type !== '') fresh.type = link.type
      fresh.href = href
      document.head.appendChild(fresh)
    } catch { /* cross-origin or transiently inaccessible — keep the fallback icon */ }
  }

  const syncHead = (): void => { syncTitle(); syncIcon() }
  const watchHead = (): void => {
    syncHead()
    // Prefer a <head> MutationObserver for instant, event-driven title/icon
    // updates (incl. SPA client-side navigations). Only if we can't attach one
    // (head not ready yet, or cross-origin) do we fall back to polling — and we
    // poll just long enough for the head to become observable, then stop, so we
    // don't run a 1s interval for the life of the page.
    let observing = false
    const tryObserve = (): boolean => {
      if (observing) return true
      try {
        const head = frame.contentDocument?.head
        if (head != null) {
          new MutationObserver(syncHead).observe(head, { subtree: true, childList: true, characterData: true })
          observing = true
        }
      } catch { /* cross-origin: stay on the poll fallback */ }
      return observing
    }
    if (!tryObserve()) {
      let ticks = 0
      const poll = setInterval(() => {
        syncHead()
        // Stop once observing, or after ~30s (a frame we still can't observe by
        // then is cross-origin/unreadable, so further polling is pointless).
        if (tryObserve() || ++ticks > 30) clearInterval(poll)
      }, 1000)
    }
  }

  // Full-load gate. We want the overlay to lift only once the WHOLE site is
  // ready — HTML, CSS, fonts, AND images — so the page never appears
  // half-painted (text reflowing as fonts swap in, images popping in late).
  // The iframe `load` event is exactly that signal: the browser fires it only
  // after every subresource it knows about (stylesheets, scripts, images,
  // iframes) has settled. We additionally await `document.fonts.ready` so
  // web-font swaps have happened before the reveal. The only resources that do
  // NOT block this — by design — are background route prefetches (rel=prefetch
  // / speculative SPA fetches), which browsers run at idle and which never gate
  // `load`; those keep streaming behind the revealed page.
  //
  // Two safety rails on top of "wait for everything":
  //   * Stylesheet failures still fail the shell (never reveal unstyled HTML).
  //     Unlike a normal origin server this gateway serves CSS over the same
  //     slow Swarm path as everything else, so a missing sheet is a real,
  //     visible breakage — not a late nicety like an image. A sheet whose
  //     fetch errors gets ONE transparent retry (transient chunk-retrieval
  //     failures are common on the thin browser peer pool); a second failure
  //     surfaces the error overlay.
  //   * A FULL_LOAD_BUDGET cap: a single image that's slow or unretrievable
  //     from Swarm must not pin the spinner forever. Once the document has
  //     committed and its stylesheets are ready, if `load` still hasn't fired
  //     after the budget we reveal anyway (the late image will pop in over the
  //     shown page) rather than hang.
  type LinkEl = HTMLLinkElement & { __gwState?: 'loaded' | 'failed' }
  const sheetState = (doc: Document): 'ready' | 'pending' | 'failed' => {
    let failed = false
    for (const link of Array.from(doc.querySelectorAll<LinkEl>('link[rel~="stylesheet"]'))) {
      if (link.disabled) continue
      try { if (link.sheet != null) continue } catch { /* inaccessible sheet: rely on listener state */ }
      if (link.__gwState === 'failed') { failed = true; continue }
      return 'pending' // not loaded, not failed -> still in flight (or untracked yet)
    }
    return failed ? 'failed' : 'ready'
  }

  // Has the document finished loading EVERYTHING it gates on — i.e. the iframe
  // `load` event has fired (all stylesheets, scripts, images and sub-iframes
  // settled) and all web fonts have resolved? `document.readyState === 'complete'`
  // is the same condition that fires `load`, and `document.fonts.status` is
  // 'loaded' once font faces stop loading. Background route prefetches are not
  // part of either signal, so they don't hold this back.
  const fullyLoaded = (doc: Document): boolean => {
    if (doc.readyState !== 'complete') return false
    try { if (doc.fonts != null && doc.fonts.status !== 'loaded') return false } catch { /* no FontFaceSet: ignore */ }
    return true
  }

  // Time budgets.
  //   FULL_LOAD_BUDGET: once the document has committed AND its stylesheets are
  //     ready, how long we additionally wait for the FULL load (images, fonts,
  //     remaining subresources). If a late/slow/dead image from Swarm hasn't
  //     settled by then we reveal anyway rather than pin the spinner forever.
  //   STYLE_BUDGET: how long, after the document commits, we keep waiting for
  //     its stylesheets. Must cover the SW's whole per-request budget (~290s:
  //     daemon worst case 3 candidates × 90s, see sw.ts) — if it expires we show
  //     a clear failure, NEVER the unstyled page.
  //   DOC_CAP: how long we wait for ANY real document to commit before giving
  //     up. Must comfortably exceed the daemon's per-fetch ceiling (90s) since
  //     on a cold node the first HTML fetch can legitimately take that long;
  //     revealing earlier would just dismiss the overlay over a blank iframe.
  const FULL_LOAD_BUDGET_MS = 60_000
  const STYLE_BUDGET_MS = 300_000
  const DOC_CAP_MS = 100_000
  // How many times to re-attempt a stylesheet whose fetch errored before
  // declaring it permanently failed (and failing the shell). Swarm retrieval on
  // the thin browser pool routinely needs a few attempts for a given chunk, so a
  // single retry was too eager. The overall STYLE_BUDGET_MS still caps total
  // wait, so more retries can't hang forever — they just avoid a premature fail.
  const STYLE_RETRIES = 5
  const watchContentReady = (): void => {
    const started = Date.now()
    let docCommittedAt: number | null = null
    let sheetsReadyAt: number | null = null
    const tracked = new WeakSet<HTMLLinkElement>()
    const poll = setInterval(() => {
      let state: 'ready' | 'pending' | 'failed' = 'pending'
      let docCommitted = false
      let loadedAll = false
      try {
        const doc = frame.contentDocument
        const rs = doc?.readyState
        // Ignore the initial about:blank document still present mid-navigation.
        const docReady = doc != null && !(doc.URL ?? '').startsWith('about:') &&
          (rs === 'interactive' || rs === 'complete')
        if (docReady && doc != null) {
          docCommitted = true
          // Attach one-shot load/error listeners to any stylesheet links we
          // haven't seen yet. `load` marks the link loaded; `error` retries the
          // fetch by swapping in a fresh clone (re-setting the same href is not
          // a reliable refetch). Swarm retrieval is slow and flaky on the thin
          // browser /ws pool — a single stylesheet chunk commonly hits a couple
          // of transient `storage: not found` / dial failures before a live
          // forwarder serves it — so we retry SEVERAL times with a short backoff
          // before declaring the sheet permanently failed. A premature single-
          // retry budget surfaced "Failed to load site" on sites whose CSS was
          // in fact retrievable, just slow.
          for (const link of doc.querySelectorAll<LinkEl>('link[rel~="stylesheet"]')) {
            if (tracked.has(link)) continue
            tracked.add(link)
            link.addEventListener('load', () => { link.__gwState = 'loaded' }, { once: true })
            link.addEventListener('error', () => {
              const tries = Number(link.dataset.gwRetries ?? '0')
              if (tries < STYLE_RETRIES) {
                const next = tries + 1
                console.warn(`[boot] stylesheet failed, retry ${next}/${STYLE_RETRIES}:`, link.href)
                // Brief backoff before re-inserting so a transiently-unreachable
                // chunk has a moment for a fresh forwarder to come online, rather
                // than burning all retries against the same dead candidates in a
                // tight loop.
                const delay = 1000 * next
                setTimeout(() => {
                  const clone = link.cloneNode() as LinkEl
                  clone.dataset.gwRetries = String(next)
                  link.replaceWith(clone) // the next poll tick tracks the clone
                }, delay)
              } else {
                console.error(`[boot] stylesheet failed after ${STYLE_RETRIES} retries:`, link.href)
                link.__gwState = 'failed'
              }
            }, { once: true })
          }
          state = sheetState(doc)
          loadedAll = fullyLoaded(doc)
        }
      } catch { state = 'pending' } // transiently inaccessible while navigating
      if (docCommitted && docCommittedAt == null) docCommittedAt = Date.now()
      if (docCommitted && state === 'ready' && sheetsReadyAt == null) sheetsReadyAt = Date.now()
      const now = Date.now()
      if (docCommitted && state === 'failed') {
        // A stylesheet is permanently unretrievable (failed twice). Showing the
        // page would mean blank/unstyled HTML — surface a failure instead.
        clearInterval(poll)
        failShell('The site\u2019s HTML arrived but a stylesheet could not be retrieved from Swarm, so the page would render unstyled. Reload to retry.')
      } else if (docCommitted && state === 'ready' && loadedAll) {
        // Everything the page gates on (CSS, fonts, images, scripts) is in.
        clearInterval(poll)
        setTimeout(reveal, 200) // brief grace for first paint / hydration
      } else if (sheetsReadyAt != null && now - sheetsReadyAt > FULL_LOAD_BUDGET_MS) {
        // Stylesheets are in and the page is styled, but the full `load` is
        // still pending — typically a slow or unretrievable image. We've waited
        // the full-load budget; reveal the (styled) page rather than hang. The
        // straggler will pop in over the shown page if it ever arrives.
        console.warn('[boot] full-load budget elapsed with subresources pending; revealing styled page')
        clearInterval(poll)
        reveal()
      } else if (docCommittedAt != null && now - docCommittedAt > STYLE_BUDGET_MS) {
        // Stylesheets still in flight after the SW's whole request budget —
        // they're not coming. Never reveal unstyled HTML; fail clearly.
        clearInterval(poll)
        failShell('Timed out fetching this site\u2019s stylesheets from Swarm. Reload to retry.')
      } else if (docCommittedAt == null && now - started > DOC_CAP_MS) {
        // No document ever committed: the fetch is wedged (no peers / no chunks).
        // Don't reveal a blank iframe — surface a clear failure so the user knows
        // to retry or check peer connectivity, and keep the live daemon phase.
        clearInterval(poll)
        failShell('Timed out fetching this site from Swarm. The shared node may have no reachable peers for this content — try again, or open the gateway home to check peer status.')
      }
    }, 150)
  }

  // The iframe `load` event is the happy path: the browser fires it only once
  // every subresource (stylesheets, scripts, images, sub-iframes) has settled,
  // so by here the full page is loaded. Guards: (1) the intermediate
  // about:blank document, whose `load` fires before the real navigation commits
  // — revealing then would dismiss the overlay over a blank frame; (2) `load`
  // also fires when a stylesheet ERRORED (an errored subresource doesn't block
  // the event), so only reveal here if every sheet really loaded — otherwise
  // leave it to the poll above, which retries the sheet and otherwise fails the
  // shell rather than ever revealing unstyled HTML. We still wait on
  // `document.fonts.ready` so a font swap doesn't reflow text right after the
  // reveal; if fonts never settle the poll's full-load budget reveals anyway.
  frame.addEventListener('load', () => {
    if (!contentRequested) return
    let doc: Document | null = null
    try {
      doc = frame.contentDocument
      if (doc == null) return
      if ((doc.URL ?? 'about:').startsWith('about:')) return
      if (sheetState(doc) !== 'ready') return // unstyled: let the poll handle it
    } catch { reveal(); syncHead(); return } // cross-origin/unreadable: reveal as before
    const fonts = doc.fonts
    if (fonts == null) { reveal(); syncHead(); return }
    fonts.ready.then(() => { reveal(); syncHead() }).catch(() => { reveal(); syncHead() })
  })

  return {
    loadContent (path) {
      contentRequested = true
      loadingText.textContent = 'Loading Swarm site…'
      frame.src = path
      watchContentReady()
      watchHead()
    },
    phase (primary, sub) {
      loadingText.textContent = primary
      if (sub != null) loadingSub.textContent = sub
    },
    status (s) {
      // Show the count of peers we hold a live retrieval session to (the warm
      // forwarder set), not the `dialable` count (peers that merely advertise a
      // /ws[s] underlay we may never have connected to) — matching the landing
      // page and the documented "connected peers" semantics (see protocol.ts).
      // This count legitimately ramps 0 → PREWARM_SESSIONS as prewarm() opens
      // sessions in the background after readiness. Readiness/colour, though,
      // keys off `dialable`: having reachable peers means the node is healthy
      // even before any session is warm.
      let msg: string
      if (s.lastError != null) {
        dot.style.background = '#f85149'
        msg = `daemon error · ${s.connected} peers`
      } else if (s.warming || !s.ready) {
        dot.style.background = '#d29922'
        msg = s.ready ? `discovering · ${s.connected} peers` : 'starting daemon…'
      } else {
        dot.style.background = s.dialable > 0 ? '#3fb950' : '#d29922'
        msg = `${s.connected} peers`
      }
      text.textContent = msg
      // Mirror live daemon status into the overlay while the site is loading.
      // Prefer the daemon's PHASE (e.g. "fetching index.html · 24/209 nodes…",
      // "got index.html (40000 bytes, …)") over the bare peer count: on mobile
      // the slow path otherwise looks frozen at "152 peers" with no signal that
      // retrieval is actually progressing (or where it's stuck). Fall back to
      // the peer count before any fetch phase exists.
      //
      // The daemon is SHARED across every content origin, so its `phase` is
      // global. Only show a phase that's either a daemon-lifecycle phase (no
      // `phaseRef` — warming/ready/discovery, relevant to everyone) or one
      // tagged with THIS shell's own reference; otherwise we'd display another
      // site's in-flight file progress. If we couldn't determine our own ref,
      // fall back to showing all phases (degrades to the old behaviour).
      const phaseForUs = s.phase != null && s.phase !== '' &&
        (s.phaseRef == null || OWN_REF == null || s.phaseRef === OWN_REF)
      if (!loaded) loadingSub.textContent = phaseForUs ? (s.phase as string) : msg
      if (loaded) setTimeout(() => pill.classList.add('gw-min'), 1500)
    },
    fail (msg) { failShell(msg) }
  }
}

void main()

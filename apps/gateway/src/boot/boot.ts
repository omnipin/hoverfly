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
import { daemonOrigin } from '../shared/parse-request.ts'

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

  // True once the iframe document has parsed AND every render-blocking
  // stylesheet it declares has actually loaded. We deliberately do NOT wait for
  // the full `load` event: images and other late subresources are retrieved
  // from Swarm chunk-by-chunk and can lag badly or never arrive, so `load`
  // would leave the overlay stuck over an already-usable page.
  //
  // But `interactive`/DOMContentLoaded alone is too early HERE: unlike a normal
  // origin server, this gateway serves CSS over the same slow Swarm-via-daemon
  // path as everything else, so at DOMContentLoaded the stylesheets are
  // typically still in flight. Revealing then shows parsed-but-unstyled (blank)
  // HTML. So we additionally require all <link rel="stylesheet"> elements to be
  // resolved. A stylesheet link exposes a non-null `.sheet` once it has loaded
  // and parsed; it stays null while pending. A link that errored fires `load`'s
  // sibling `error` and its `.sheet` stays null forever — we treat a link as
  // "resolved" if its sheet is present OR it has finished (load/error) so a
  // 404'd stylesheet can't wedge the overlay open.
  const stylesheetsReady = (doc: Document): boolean => {
    const links = Array.from(doc.querySelectorAll<HTMLLinkElement>('link[rel~="stylesheet"]'))
    return links.every((link) => {
      if (link.disabled) return true
      try { if (link.sheet != null) return true } catch { /* cross-origin sheet: treat via settled flag below */ }
      // `settled` is stamped by the per-link load/error listeners attached in
      // watchContentReady; absent that, fall back to "not ready yet".
      return (link as HTMLLinkElement & { __gwSettled?: boolean }).__gwSettled === true
    })
  }

  // Poll the iframe document until it's parsed and its stylesheets have loaded,
  // then reveal. `load` still acts as a happy-path accelerator, and a hard cap
  // guards against a wholly stalled navigation (e.g. a stylesheet that never
  // arrives from Swarm).
  // Time budgets. STYLE_CAP: once the document has committed, how long we wait
  // for its stylesheets before revealing the parsed (possibly unstyled) page
  // anyway — a stylesheet may never arrive from Swarm, and a parsed page beats
  // a stuck overlay. DOC_CAP: how long we wait for ANY real document to commit
  // before giving up. This must comfortably exceed the daemon's per-fetch
  // ceiling (90s) since on a cold node the first HTML fetch can legitimately
  // take that long; revealing earlier would just dismiss the overlay over a
  // blank iframe (the observed "blank page" symptom).
  const STYLE_CAP_MS = 20_000
  const DOC_CAP_MS = 100_000
  const watchContentReady = (): void => {
    const started = Date.now()
    let docCommittedAt: number | null = null
    const tracked = new WeakSet<HTMLLinkElement>()
    const poll = setInterval(() => {
      let ready = false
      let docCommitted = false
      try {
        const doc = frame.contentDocument
        const rs = doc?.readyState
        // Ignore the initial about:blank document still present mid-navigation.
        const docReady = doc != null && !(doc.URL ?? '').startsWith('about:') &&
          (rs === 'interactive' || rs === 'complete')
        if (docReady && doc != null) {
          docCommitted = true
          // Attach one-shot load/error listeners to any stylesheet links we
          // haven't seen yet, so an errored sheet is recorded as settled and
          // can't hold the overlay open indefinitely.
          for (const link of doc.querySelectorAll<HTMLLinkElement>('link[rel~="stylesheet"]')) {
            if (tracked.has(link)) continue
            tracked.add(link)
            const mark = (): void => { (link as HTMLLinkElement & { __gwSettled?: boolean }).__gwSettled = true }
            link.addEventListener('load', mark, { once: true })
            link.addEventListener('error', mark, { once: true })
          }
          ready = stylesheetsReady(doc)
        }
      } catch { ready = false } // transiently inaccessible while navigating
      if (docCommitted && docCommittedAt == null) docCommittedAt = Date.now()
      const now = Date.now()
      if (ready) {
        clearInterval(poll)
        setTimeout(reveal, 400) // brief grace for first paint / hydration
      } else if (docCommittedAt != null && now - docCommittedAt > STYLE_CAP_MS) {
        // Document is here but stylesheets stalled — reveal the page as-is
        // rather than holding a blank overlay over usable (if unstyled) content.
        clearInterval(poll)
        reveal()
      } else if (docCommittedAt == null && now - started > DOC_CAP_MS) {
        // No document ever committed: the fetch is wedged (no peers / no chunks).
        // Don't reveal a blank iframe — surface a clear failure so the user knows
        // to retry or check peer connectivity, and keep the live daemon phase.
        clearInterval(poll)
        failShell('Timed out fetching this site from Swarm. The shared node may have no reachable peers for this content — try again, or open the gateway home to check peer status.')
      }
    }, 150)
  }

  // The full `load` event is the happy path: by then stylesheets are loaded
  // too. Guard against the intermediate about:blank document whose `load` fires
  // before the real navigation commits — revealing then would dismiss the
  // overlay over a blank frame.
  frame.addEventListener('load', () => {
    if (!contentRequested) return
    let realDoc = false
    try { realDoc = !(frame.contentDocument?.URL ?? 'about:').startsWith('about:') } catch { realDoc = true }
    if (realDoc) { reveal(); syncHead() }
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
      let msg: string
      if (s.lastError != null) {
        dot.style.background = '#f85149'
        msg = `daemon error · ${s.dialable} peers`
      } else if (s.warming || !s.ready) {
        dot.style.background = '#d29922'
        msg = s.ready ? `discovering · ${s.dialable} peers` : 'starting daemon…'
      } else {
        dot.style.background = s.dialable > 0 ? '#3fb950' : '#d29922'
        msg = `${s.dialable} peers`
      }
      text.textContent = msg
      // Mirror live daemon status into the overlay while the site is loading.
      if (!loaded) loadingSub.textContent = msg
      if (loaded) setTimeout(() => pill.classList.add('gw-min'), 1500)
    },
    fail (msg) { failShell(msg) }
  }
}

void main()

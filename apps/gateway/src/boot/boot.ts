// Boot shell — the top document served for any navigation to a content
// subdomain (<cid>.bzz.<host>). It cannot stream the site's top-level HTML
// itself (the isheika node lives in a cross-origin daemon and the SW can't
// reach it before a client exists), so instead it:
//   1. registers + waits for the content-origin service worker,
//   2. embeds the cross-origin daemon broker iframe and opens an RPC channel,
//   3. mints a second daemon port and hands it to the service worker,
//   4. renders the actual website in a full-viewport child iframe, whose
//      requests the SW now serves as Swarm content via the daemon.

import { DAEMON_FRAME_PATH, SW_SCRIPT } from '../shared/config.ts'
import { DaemonRpc, mintDaemonPort, type DaemonStatus } from '../shared/protocol.ts'
import { daemonOrigin } from '../shared/parse-request.ts'

async function main (): Promise<void> {
  if (window.top !== window) return // only the top shell bootstraps

  // Catch-all: log EVERY postMessage the shell receives, to debug the bridge.
  window.addEventListener('message', (e) => {
    console.log('[boot] MSG from', e.origin, 'type=', e.data?.type, 'src?', e.source === window ? 'self' : 'other')
  })
  console.log('[boot] shell start', location.href)
  const ui = renderShell()

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

  // Load the real site.
  ui.loadContent(location.pathname + location.search)
}

async function ensureControllingSW (): Promise<ServiceWorker> {
  await navigator.serviceWorker.register(SW_SCRIPT, { scope: '/' })
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
    const onMessage = (e: MessageEvent): void => {
      if (e.data?.type === 'frame-ready') console.log('[boot] got frame-ready from', e.origin)
      if (e.origin !== origin || e.data?.type !== 'frame-ready') return
      window.removeEventListener('message', onMessage)
      clearTimeout(timeout)
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
  pill.title = 'isheika gateway status — click to hide'
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

  // Reveal the loaded site by fading the overlay out (idempotent).
  const reveal = (): void => {
    if (loaded) return
    loaded = true
    loading.classList.add('gw-hide')
    setTimeout(() => loading.remove(), 500)
    setTimeout(() => pill.classList.add('gw-min'), 1500)
  }

  // Dismiss the overlay as soon as the site's document has parsed and its entry
  // scripts have run (DOMContentLoaded ⇒ readyState 'interactive'), NOT on the
  // full `load` event. Images and other late subresources are retrieved from
  // Swarm chunk-by-chunk and can lag badly or never arrive; `load` waits for
  // all of them, so gating on it leaves the overlay stuck over an already
  // interactive page. `load` still acts as a happy-path accelerator, and a hard
  // cap guards against a wholly stalled navigation.
  const watchContentReady = (): void => {
    const started = Date.now()
    const poll = setInterval(() => {
      let ready = false
      try {
        const doc = frame.contentDocument
        const rs = doc?.readyState
        // Ignore the initial about:blank document still present mid-navigation.
        ready = !(doc?.URL ?? '').startsWith('about:') && (rs === 'interactive' || rs === 'complete')
      } catch { ready = false } // transiently inaccessible while navigating
      if (ready) {
        clearInterval(poll)
        setTimeout(reveal, 400) // brief grace for first paint / hydration
      } else if (Date.now() - started > 20_000) {
        clearInterval(poll)
        reveal()
      }
    }, 150)
  }

  frame.addEventListener('load', () => { if (contentRequested) reveal() })

  return {
    loadContent (path) {
      contentRequested = true
      loadingText.textContent = 'Loading Swarm site…'
      frame.src = path
      watchContentReady()
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
    fail (msg) {
      pill.classList.remove('gw-min', 'gw-hidden')
      dot.style.background = '#f85149'
      text.textContent = msg
      loading.classList.remove('gw-hide')
      loading.classList.add('gw-error')
      loadingText.textContent = 'Failed to load site'
      loadingSub.textContent = msg
    }
  }
}

void main()

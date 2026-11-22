// Pure-logic self-tests (no DOM): run with `npm run selftest`.
// Validates the swarm CID codec against hoverfly's `src/cid.rs` test vector,
// CID<->reference round-trips, reference normalization, and host parsing.

import assert from 'node:assert/strict'
import { base32LowerDecode, base32LowerEncode, cidToReference, decodeSwarmContenthash, referenceToCid } from '../src/shared/swarm-cid.ts'
import { normalizeRef } from '../src/shared/swarm-ref.ts'
import { parseHost } from '../src/shared/parse-request.ts'
import { looksLikeEnsName } from '../src/shared/ens.ts'

let passed = 0
function check (name: string, fn: () => void): void {
  fn()
  passed++
  console.log('  ✓', name)
}

// base32 round-trip
check('base32 round-trips arbitrary bytes', () => {
  for (let len = 0; len < 40; len++) {
    const a = new Uint8Array(len)
    for (let i = 0; i < len; i++) a[i] = (i * 37 + 11) & 0xff
    const dec = base32LowerDecode(base32LowerEncode(a))
    assert.deepEqual([...dec.slice(0, len)], [...a])
  }
})

// hoverfly src/cid.rs known vector
const VECTOR_HEX = 'fd33fdeb04ad97c4ae1894077da75bee6f69bc7cbada0e95c3acad74c6dbef35'
const VECTOR_CID = 'bah5acgza7uz732yevwl4jlqysqdx3j235zxwtpd4xlna5fodvswxjrw3542q'

check('referenceToCid matches the cid.rs test vector', () => {
  assert.equal(referenceToCid(VECTOR_HEX), VECTOR_CID)
})

check('cidToReference inverts referenceToCid', () => {
  const { refHex, codec } = cidToReference(VECTOR_CID)
  assert.equal(refHex, VECTOR_HEX)
  assert.equal(codec, 0xfa)
})

check('cid <-> ref round-trips for random refs', () => {
  for (let t = 0; t < 50; t++) {
    let hex = ''
    for (let i = 0; i < 32; i++) hex += (((t * 7 + i * 13) & 0xff)).toString(16).padStart(2, '0')
    assert.equal(cidToReference(referenceToCid(hex)).refHex, hex)
  }
})

// normalizeRef accepts hex and CID, strips prefixes
check('normalizeRef accepts 64-hex', () => {
  const r = normalizeRef(VECTOR_HEX)
  assert.equal(r.refHex, VECTOR_HEX)
  assert.equal(r.cid, VECTOR_CID)
})
check('normalizeRef accepts 0x-prefixed hex', () => {
  assert.equal(normalizeRef('0x' + VECTOR_HEX).cid, VECTOR_CID)
})
check('normalizeRef accepts a CID', () => {
  assert.equal(normalizeRef(VECTOR_CID).refHex, VECTOR_HEX)
})
check('normalizeRef strips bzz:// and trailing path', () => {
  assert.equal(normalizeRef(`bzz://${VECTOR_HEX}/index.html`).refHex, VECTOR_HEX)
  assert.equal(normalizeRef(`/bzz/${VECTOR_CID}/a/b`).refHex, VECTOR_HEX)
})
check('normalizeRef rejects garbage', () => {
  assert.throws(() => normalizeRef('not-a-ref'))
})

// host parsing
check('parseHost: root', () => {
  const h = parseHost('bzz.localhost:3000')
  assert.equal(h.kind, 'root')
  assert.equal(h.rootHost, 'bzz.localhost:3000')
})
check('parseHost: subdomain', () => {
  const h = parseHost(`${VECTOR_CID}.bzz.localhost:3000`)
  assert.equal(h.kind, 'subdomain')
  assert.equal(h.id, VECTOR_CID)
  assert.equal(h.rootHost, 'bzz.localhost:3000')
})
check('parseHost: infix-less root (production apex)', () => {
  const h = parseHost('browserbzz.link')
  assert.equal(h.kind, 'root')
  assert.equal(h.rootHost, 'browserbzz.link')
})
check('parseHost: infix-less content subdomain (production)', () => {
  const h = parseHost(`${VECTOR_CID}.browserbzz.link`)
  assert.equal(h.kind, 'subdomain')
  assert.equal(h.id, VECTOR_CID)
  assert.equal(h.rootHost, 'browserbzz.link')
})
check('parseHost: infix-less non-CID subdomain is treated as root', () => {
  // e.g. www.<apex> — not a content origin, so it falls through to root.
  assert.equal(parseHost('www.browserbzz.link').kind, 'root')
})

// ENS contenthash decoding, against records read from mainnet.
check('decodeSwarmContenthash: swarm.eth', () => {
  assert.equal(
    decodeSwarmContenthash('0xe40101fa011b2003b80b08353b288c08b161fcf0e904eff3001c88006bcc7087290929197eb69f').refHex,
    '03b80b08353b288c08b161fcf0e904eff3001c88006bcc7087290929197eb69f'
  )
})
check('decodeSwarmContenthash: ens.eth', () => {
  assert.equal(
    decodeSwarmContenthash('0xe40101fa011b20f5909b5286d9279a1f6a23d5155ddd72bb0f28f56b6b82c32de54a6529e6b1b0').refHex,
    'f5909b5286d9279a1f6a23d5155ddd72bb0f28f56b6b82c32de54a6529e6b1b0'
  )
})
check('decodeSwarmContenthash names non-Swarm protocols', () => {
  // vitalik.eth (ipfs)
  assert.throws(
    () => decodeSwarmContenthash('0xe301017012201d261cc098861d9952e6c8954801800e56aa34aabd8a9651b04b4e2498a45c53'),
    /points at ipfs, not Swarm/
  )
  assert.throws(() => decodeSwarmContenthash('0xe50101720024080112'), /points at ipns, not Swarm/)
  assert.throws(() => decodeSwarmContenthash('0x90b2ca05'), /points at arweave, not Swarm/)
  assert.throws(() => decodeSwarmContenthash('0x7f00'), /points at codec 0x7f, not Swarm/)
})
check('decodeSwarmContenthash rejects truncated and garbage values', () => {
  assert.throws(() => decodeSwarmContenthash('0x'))
  assert.throws(() => decodeSwarmContenthash('0xe40101fa011b20f5909b52'))
  assert.throws(() => decodeSwarmContenthash('0xe40102fa011b20' + '00'.repeat(32)))
})

check('looksLikeEnsName', () => {
  assert.equal(looksLikeEnsName('swarm.eth'), true)
  assert.equal(looksLikeEnsName('  Swarm.ETH '), true)
  assert.equal(looksLikeEnsName('ñandú.eth'), true)
  assert.equal(looksLikeEnsName(VECTOR_HEX), false)
  assert.equal(looksLikeEnsName(VECTOR_CID), false)
  assert.equal(looksLikeEnsName('a b.eth'), false)
  assert.equal(looksLikeEnsName('bzz://swarm.eth/'), false)
})

console.log(`\n${passed} checks passed.`)

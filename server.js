import http from 'node:http'
import crypto from 'node:crypto'
import { WebSocketServer } from 'ws'

const PORT = Number(process.env.PORT || 10000)
const SESSION_TTL_MS = 2 * 60 * 60 * 1000
const sessions = new Map()

function json(ws, message) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(message))
}

function close(ws, code, reason) {
  try { if (ws) ws.close(code, reason) } catch {}
}

function validId(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{12,80}$/.test(value)
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ ok: true, sessions: sessions.size }))
    return
  }
  res.writeHead(404)
  res.end('not found')
})

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 })

wss.on('connection', ws => {
  ws.session = null
  ws.role = null
  ws.isAlive = true
  ws.on('pong', () => { ws.isAlive = true })

  ws.on('message', raw => {
    let msg
    try { msg = JSON.parse(raw.toString()) } catch { return close(ws, 1003, 'invalid json') }

    if (!ws.session) {
      if (msg.type === 'create' && msg.role === 'desktop' && validId(msg.session) && validId(msg.token)) {
        if (sessions.has(msg.session)) return close(ws, 1008, 'session exists')
        const record = { token: msg.token, desktop: ws, phone: null, expiresAt: Date.now() + SESSION_TTL_MS }
        sessions.set(msg.session, record)
        ws.session = msg.session; ws.role = 'desktop'
        json(ws, { type: 'ready', role: 'desktop', expiresAt: record.expiresAt })
        return
      }

      if (msg.type === 'join' && msg.role === 'phone' && validId(msg.session) && validId(msg.token)) {
        const record = sessions.get(msg.session)
        if (!record || record.expiresAt < Date.now() || record.token !== msg.token || record.phone) {
          return close(ws, 1008, 'invalid session')
        }
        record.phone = ws
        ws.session = msg.session; ws.role = 'phone'
        json(ws, { type: 'ready', role: 'phone', expiresAt: record.expiresAt })
        json(record.desktop, { type: 'phone_connected' })
        return
      }

      return close(ws, 1008, 'join required')
    }

    const record = sessions.get(ws.session)
    if (!record || record.expiresAt < Date.now()) return close(ws, 1008, 'session expired')

    if (msg.type === 'command' && ws.role === 'phone') {
      if (['next', 'prev', 'start', 'end'].includes(msg.action)) json(record.desktop, msg)
    } else if (msg.type === 'state' && ws.role === 'desktop') {
      json(record.phone, msg)
    }
  })

  ws.on('close', () => {
    const record = ws.session && sessions.get(ws.session)
    if (!record) return
    if (record.desktop === ws) record.desktop = null
    if (record.phone === ws) {
      record.phone = null
      json(record.desktop, { type: 'phone_disconnected' })
    }
    if (!record.desktop && !record.phone) sessions.delete(ws.session)
  })
})

setInterval(() => {
  const now = Date.now()
  for (const [id, record] of sessions) {
    if (record.expiresAt < now) {
      close(record.desktop, 1008, 'session expired')
      close(record.phone, 1008, 'session expired')
      sessions.delete(id)
    }
  }
  for (const ws of wss.clients) {
    if (!ws.isAlive) ws.terminate()
    else { ws.isAlive = false; ws.ping() }
  }
}, 30_000).unref()

server.listen(PORT, '0.0.0.0', () => console.log(`PPT relay listening on ${PORT}`))

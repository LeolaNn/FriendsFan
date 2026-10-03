const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'public', 'index.html');

async function iceServers() {
  const base = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  const { METERED_APP, METERED_KEY, TURN_URLS, TURN_USER, TURN_PASS } = process.env;
  if (METERED_APP && METERED_KEY) {
    try {
      const r = await fetch(`https://${METERED_APP}.metered.live/api/v1/turn/credentials?apiKey=${METERED_KEY}`);
      const j = await r.json();
      if (Array.isArray(j)) return base.concat(j);
    } catch (e) { console.log('metered error', e.message); }
  }
  if (TURN_URLS) return base.concat([{ urls: TURN_URLS.split(','), username: TURN_USER, credential: TURN_PASS }]);
  return base;
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  if (req.url === '/ice') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify(await iceServers()));
  }
  fs.readFile(page, (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> { clients:Map(id->ws), caster:id|null }

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const all = (room, o, exceptId) => { for (const [id, c] of room.clients) if (id !== exceptId) send(c, o); };
const peers = (room) => all(room, { type: 'peers', n: room.clients.size });

function dropCaster(room) { room.caster = null; all(room, { type: 'caster', id: null }); }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null;
  ws.id = Math.random().toString(36).slice(2, 10);
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      const code = String(m.room || '').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 32);
      if (!code) return;
      let room = rooms.get(code);
      if (!room) { room = { clients: new Map(), caster: null }; rooms.set(code, room); }
      room.clients.set(ws.id, ws); ws.room = code;
      send(ws, { type: 'hello', id: ws.id, caster: room.caster });
      if (room.caster) send(room.clients.get(room.caster), { type: 'viewer', id: ws.id });
      return peers(room);
    }

    const room = ws.room && rooms.get(ws.room);
    if (!room) return;

    if (m.type === 'cast') {
      room.caster = ws.id;
      all(room, { type: 'caster', id: ws.id });
      send(ws, { type: 'viewers', ids: [...room.clients.keys()].filter(i => i !== ws.id) });
    } else if (m.type === 'signal') {
      send(room.clients.get(m.to), { type: 'signal', from: ws.id, data: m.data });
    } else if (m.type === 'cmd') {
      if (room.caster && room.caster !== ws.id) send(room.clients.get(room.caster), { type: 'cmd', a: m.a, t: m.t });
    } else if (m.type === 'info' && room.caster === ws.id) {
      all(room, { type: 'info', t: m.t, d: m.d, p: m.p }, ws.id);
    }
  });

  ws.on('close', () => {
    const room = ws.room && rooms.get(ws.room);
    if (!room) return;
    room.clients.delete(ws.id);
    if (room.caster === ws.id) dropCaster(room);
    else if (room.caster) send(room.clients.get(room.caster), { type: 'gone', id: ws.id });
    if (room.clients.size === 0) rooms.delete(ws.room); else peers(room);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Listening on ' + PORT));

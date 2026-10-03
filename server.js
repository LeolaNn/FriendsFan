const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(page, (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> { clients:Set, state:{url,a,t,at} }

function broadcast(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const c of room.clients) if (c !== except && c.readyState === 1) c.send(s);
}
function count(room) { broadcast(room, { type: 'peers', n: room.clients.size }); }

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'join') {
      const code = String(m.room || '').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 32);
      if (!code) return;
      let room = rooms.get(code);
      if (!room) { room = { clients: new Set(), state: null }; rooms.set(code, room); }
      room.clients.add(ws); ws.room = code;
      const st = room.state;
      if (st) {
        const t = st.a === 'play' ? st.t + (Date.now() - st.at) / 1000 : st.t;
        if (st.url) ws.send(JSON.stringify({ type: 'url', src: st.url }));
        ws.send(JSON.stringify({ type: 'ctl', a: st.a, t }));
      }
      return count(room);
    }
    const room = ws.room && rooms.get(ws.room);
    if (!room) return;
    if (m.type === 'ctl' && typeof m.t === 'number' && (m.a === 'play' || m.a === 'pause')) {
      room.state = { ...(room.state || {}), a: m.a, t: m.t, at: Date.now() };
      broadcast(room, { type: 'ctl', a: m.a, t: m.t }, ws);
    } else if (m.type === 'url' && typeof m.src === 'string' && m.src.length < 2000) {
      room.state = { url: m.src, a: 'pause', t: 0, at: Date.now() };
      broadcast(room, { type: 'url', src: m.src }, ws);
    }
  });

  ws.on('close', () => {
    const room = ws.room && rooms.get(ws.room);
    if (!room) return;
    room.clients.delete(ws);
    if (room.clients.size === 0) rooms.delete(ws.room); else count(room);
  });
});

// keep connections alive behind free-tier proxies
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Listening on ' + PORT));

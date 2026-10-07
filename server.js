'use strict';
/**
 * Pásalo — servidor (v2.0.1): señalización + modo puente + página.
 *
 * Lo que hace: presenta a tus equipos entre sí (intercambio de
 * ofertas/respuestas WebRTC por WebSocket), sirve la página y, si la
 * conexión directa falla, reenvía los trozos en modo puente.
 *
 * Lo que NO hace: nunca almacena tus archivos. En modo directo la
 * transferencia es de navegador a navegador (WebRTC, cifrada) sin que
 * el servidor vea nada; en modo puente reenvía los trozos por
 * WebSocket sin guardarlos.
 *
 * Seguridad (v2.0.1):
 * - Vinculación por código aleatorio de 6 dígitos (estilo TeamViewer),
 *   con límite de intentos POR IP (10/min, bloqueo 10 min) — no se elude
 *   reconectando.
 * - Host permitido: solo localhost o IPs literales (+ ALLOWED_HOSTS).
 *   WebSocket exige Origin válido de este mismo servidor (anti
 *   DNS-rebinding y anti cross-site).
 * - Identidad sellada por el servidor: al reenviar una oferta, el
 *   servidor pone el nombre y deviceId reales del emisor (el cliente
 *   no puede falsificarlos). Cada dispositivo muestra su ID.
 * - Las conexiones sin vincular se cierran a los 60 s y no ocupan
 *   plazas indefinidamente.
 * - Tokens persistidos en disco (tokens.json): sobreviven reinicios.
 * - Un token solo puede estar activo en una conexión a la vez.
 * - Desvinculación remota de dispositivos ({type:'unpair'}).
 */
const http = require('http');
const fs = require('fs');
const os = require('os');
const net = require('net');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = parseInt(process.env.PORT || '3000', 10);
const PUBLIC_DIR = path.join(__dirname, 'public');
const TOKENS_FILE = process.env.PASALO_TOKENS_FILE || path.join(__dirname, 'tokens.json');
const MAX_CLIENTS = 50;
const MAX_MSG_PER_WINDOW = 120;
const RATE_WINDOW_MS = 10 * 1000;
const RELAY_MAX_MSG_BYTES = 512 * 1024; // tope por mensaje del modo puente
const MAX_PAIR_PER_IP = 10;
const PAIR_IP_WINDOW_MS = parseInt(process.env.PAIR_WINDOW_MS || '60000', 10);
const PAIR_IP_BLOCK_MS = parseInt(process.env.PAIR_BLOCK_MS || String(10 * 60 * 1000), 10);
const MAX_CONNS_PER_IP = 5;
// Administración: solo la PC (localhost) puede revocar equipos.
// Si defines PASALO_ADMIN_IPS (IPs separadas por coma), esa lista
// REEMPLAZA a localhost (útil en Docker/VPS).
const ADMIN_IPS = (process.env.PASALO_ADMIN_IPS || '')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);
function isAdminIp(ip) {
  const v = (ip || '').toLowerCase();
  if (ADMIN_IPS.length) return ADMIN_IPS.includes(v);
  return v === '127.0.0.1' || v === '::1' || v === '::ffff:127.0.0.1';
}
const UNPAIRED_TIMEOUT_MS = parseInt(process.env.PAIR_TIMEOUT_MS || '60000', 10);
const EXTRA_HOSTS = (process.env.ALLOWED_HOSTS || '')
  .split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

let pairingCode = process.env.PASALO_FIXED_CODE || newPairingCode();
// token -> { name, deviceId, created }
const pairedTokens = new Map();
// token -> id de cliente con la conexión activa
const tokenSockets = new Map();
// ip -> { count, windowStart, blockedUntil }
const pairAttemptsByIp = new Map();
// ip -> conexiones activas (tope por IP, anti-saturación)
const connsByIp = new Map();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

function newPairingCode() {
  return crypto.randomInt(0, 1000000).toString().padStart(6, '0');
}

function newDeviceId() {
  return crypto.randomBytes(4).toString('hex');
}

function fmtCode(c) {
  return c.slice(0, 3) + ' ' + c.slice(3);
}

function lanIps() {
  const out = [];
  const VIRTUAL_RE = /virtual|vmware|vbox|hyper-v|wsl|tap|tun|docker|veth|br-/i;
  for (const [name, addrs] of Object.entries(os.networkInterfaces())) {
    if (VIRTUAL_RE.test(name)) continue;
    for (const a of addrs) {
      if (a.family === 'IPv4' && !a.internal) out.push(a.address);
    }
  }
  return out;
}

function printBanner() {
  console.log('');
  console.log('  ============================================================');
  const ips = lanIps();
  if (ips.length) {
    console.log('  Abre en tu celular:');
    for (const ip of ips) console.log('    http://' + ip + ':' + PORT);
    console.log('  ------------------------------------------------------------');
  }
  console.log('  CÓDIGO DE VINCULACIÓN: ' + fmtCode(pairingCode));
  console.log('  (escríbelo en el celular; cambia con cada vinculación)');
  console.log('  ============================================================');
  console.log('');
}

function rotateCode(reason) {
  pairingCode = newPairingCode();
  console.log('  [código nuevo generado: ' + reason + ']');
  printBanner();
}

// ---- tokens persistidos ----
function loadTokens() {
  try {
    const data = JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf8'));
    if (Array.isArray(data)) {
      for (const t of data) {
        if (t && typeof t.token === 'string' && typeof t.deviceId === 'string') {
          pairedTokens.set(t.token, {
            name: typeof t.name === 'string' ? t.name.slice(0, 40) : 'Equipo',
            deviceId: t.deviceId,
            created: t.created || Date.now(),
          });
        }
      }
    }
  } catch {}
}

function saveTokens() {
  try {
    const arr = [...pairedTokens.entries()].map(([token, v]) => ({ token, ...v }));
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(arr));
  } catch (e) {
    console.error('  [no se pudo guardar tokens.json: ' + e.message + ']');
  }
}

// ---- hosts permitidos: localhost o IPs literales (+ ALLOWED_HOSTS) ----
function hostPart(hostHeader) {
  let h = (hostHeader || '').trim().toLowerCase();
  if (h.startsWith('[')) {
    const i = h.indexOf(']');
    return i > 0 ? h.slice(1, i) : '';
  }
  const first = h.indexOf(':');
  const last = h.lastIndexOf(':');
  if (first !== -1 && first !== last) return h; // IPv6 sin corchetes
  return last > 0 ? h.slice(0, last) : h;
}

function hostAllowed(hostname) {
  const h = (hostname || '').toLowerCase();
  if (h === 'localhost' || h === '127.0.0.1' || h === '::1') return true;
  if (EXTRA_HOSTS.includes(h)) return true;
  return net.isIP(h) !== 0; // IPs literales sí; nombres DNS no (anti DNS-rebinding)
}

function requestHostOk(req) {
  return hostAllowed(hostPart(req.headers.host));
}

// WebSocket: Origin obligatorio y del mismo servidor (con su puerto).
// WebSocket: Origin obligatorio y con el MISMO Host:puerto al que se
// conecta la petición (anti DNS-rebinding y anti cross-site estricto).
// El puerto se compara con el del Host real, no con el interno: en Docker
// el puerto externo (p. ej. 8002) difiere del interno (3000).
function originOk(req) {
  const origin = req.headers.origin;
  if (!origin) return false;
  try {
    const u = new URL(origin);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return false;
    const oh = u.hostname.toLowerCase().replace(/^\[|\]$/g, '');
    const oport = u.port || (u.protocol === 'https:' ? '443' : '80');
    const hh = hostPart(req.headers.host);
    if (!hostAllowed(hh)) return false;
    return oh === hh && oport === hostPort(req.headers.host);
  } catch {
    return false;
  }
}

function isLocalRequest(req) {
  const ra = req.socket.remoteAddress || '';
  return ra === '127.0.0.1' || ra === '::1' || ra === '::ffff:127.0.0.1';
}

function serveStatic(req, res) {
  if (!requestHostOk(req)) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  let urlPath;
  try {
    urlPath = decodeURIComponent(req.url.split('?')[0]);
  } catch {
    res.writeHead(400); res.end('bad request'); return;
  }
  if (urlPath === '/') urlPath = '/index.html';
  if (urlPath === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok');
    return;
  }
  if (urlPath === '/pairing-code') {
    if (!isLocalRequest(req)) { res.writeHead(403); res.end('forbidden'); return; }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ code: pairingCode }));
    return;
  }
  if (urlPath === '/lan-urls') {
    // Para el QR: la página lo pide desde localhost o desde la LAN.
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ urls: lanIps().map((ip) => 'http://' + ip + ':' + PORT) }));
    return;
  }
  const safe = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!safe.startsWith(PUBLIC_DIR + path.sep)) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  fs.readFile(safe, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'content-type': MIME[path.extname(safe)] || 'application/octet-stream' });
    res.end(data);
  });
}

function isSafeObject(v) {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

function hostPort(hostHeader) {
  try {
    const u = new URL('http://' + (hostHeader || '').trim());
    return u.port || '80';
  } catch {
    return '80';
  }
}
function clientIp(req) {
  return (req.socket.remoteAddress || 'desconocida').toLowerCase();
}

// Límite de intentos de vinculación POR IP (no se elude reconectando).
// El bloqueo manda sobre la ventana: no se resetea hasta que vence.
function pairAttemptAllowed(ip) {
  const now = Date.now();
  let e = pairAttemptsByIp.get(ip);
  if (e && now < e.blockedUntil) return false;
  if (!e || now - e.windowStart > PAIR_IP_WINDOW_MS) {
    e = { count: 0, windowStart: now, blockedUntil: 0 };
    pairAttemptsByIp.set(ip, e);
  }
  e.count += 1;
  if (e.count > MAX_PAIR_PER_IP) {
    e.blockedUntil = now + PAIR_IP_BLOCK_MS;
    return false;
  }
  if (pairAttemptsByIp.size > 1000) {
    for (const [k, v] of pairAttemptsByIp) {
      if (now - v.windowStart > PAIR_IP_WINDOW_MS && now >= v.blockedUntil) {
        pairAttemptsByIp.delete(k);
      }
    }
  }
  return true;
}

const server = http.createServer(serveStatic);
const wss = new WebSocket.Server({ server, maxPayload: 1024 * 1024 });

// id -> { ws, name, deviceId, paired, token }
const clients = new Map();

function roster() {
  return [...clients.entries()]
    .filter(([, c]) => c.paired)
    .map(([id, c]) => ({ id, name: c.name, deviceId: c.deviceId }));
}

function broadcastRoster() {
  const msg = JSON.stringify({ type: 'roster', clients: roster() });
  for (const c of clients.values()) {
    if (c.paired && c.ws.readyState === WebSocket.OPEN) c.ws.send(msg);
  }
}

function pairSocket(id, me, name) {
  const token = crypto.randomBytes(16).toString('hex');
  const deviceId = newDeviceId();
  me.paired = true;
  me.token = token;
  me.deviceId = deviceId;
  if (typeof name === 'string' && name.trim()) me.name = name.trim().slice(0, 40);
  pairedTokens.set(token, { name: me.name, deviceId, created: Date.now() });
  tokenSockets.set(token, id);
  saveTokens();
  return token;
}

wss.on('connection', (ws, req) => {
  if (!originOk(req)) {
    ws.close(1008, 'origen no permitido');
    return;
  }
  if (clients.size >= MAX_CLIENTS) {
    ws.close(1013, 'servidor lleno');
    return;
  }
  const ip = clientIp(req);
  const activeConns = connsByIp.get(ip) || 0;
  if (activeConns >= MAX_CONNS_PER_IP) {
    ws.close(1013, 'demasiadas conexiones');
    return;
  }
  const id = crypto.randomBytes(8).toString('hex');
  const me = { ws, name: 'Equipo nuevo', deviceId: null, paired: false, token: null, isAdmin: false };
  clients.set(id, me);
  connsByIp.set(ip, activeConns + 1);
  ws.send(JSON.stringify({ type: 'welcome', id }));

  let msgCount = 0;
  let windowStart = Date.now();

  const heartbeat = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, 25000);

  // Sin vincular no se puede quedar para siempre ocupando una plaza.
  let pairTimer = setTimeout(() => {
    if (!me.paired && ws.readyState === WebSocket.OPEN) {
      try { ws.close(1008, 'sin vincular'); } catch {}
    }
  }, UNPAIRED_TIMEOUT_MS);
  const rearmPairTimer = () => {
    clearTimeout(pairTimer);
    pairTimer = setTimeout(() => {
      if (!me.paired && ws.readyState === WebSocket.OPEN) {
        try { ws.close(1008, 'sin vincular'); } catch {}
      }
    }, UNPAIRED_TIMEOUT_MS);
  };

  ws.on('message', (raw) => {
    const now = Date.now();
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!isSafeObject(msg)) return;

    // Modo puente: los trozos del archivo viajan como muchas señales
    // seguidas. No cuentan para el límite de señalización (si no, sería
    // imposible enviar nada grande), pero cada mensaje tiene un tope de
    // tamaño y solo viaja entre equipos vinculados (lo verifica el
    // reenvío de 'signal' más abajo).
    const isRelay = msg.type === 'signal' && isSafeObject(msg.payload) &&
      typeof msg.payload.kind === 'string' && msg.payload.kind.startsWith('relay-');
    if (isRelay) {
      if (raw.length > RELAY_MAX_MSG_BYTES) return;
    } else {
      if (now - windowStart > RATE_WINDOW_MS) {
        windowStart = now;
        msgCount = 0;
      }
      if (++msgCount > MAX_MSG_PER_WINDOW) {
        try { ws.close(1008, 'demasiados mensajes'); } catch {}
        return;
      }
    }

    // Vinculación con código (estilo TeamViewer).
    if (msg.type === 'pair' && !me.paired) {
      if (!pairAttemptAllowed(ip)) {
        try { ws.close(1008, 'demasiados intentos'); } catch {}
        return;
      }
      const code = typeof msg.code === 'string' ? msg.code.replace(/\D/g, '') : '';
      if (code === pairingCode) {
        const token = pairSocket(id, me, msg.name);
        me.isAdmin = isAdminIp(ip);
        clearTimeout(pairTimer);
        ws.send(JSON.stringify({ type: 'paired', token, deviceId: me.deviceId, admin: me.isAdmin }));
        broadcastRoster();
        rotateCode('vinculación exitosa');
      } else {
        ws.send(JSON.stringify({ type: 'pair_error' }));
      }
      return;
    }

    // Reconexión automática con token.
    if (msg.type === 'hello' && !me.paired) {
      const t = typeof msg.token === 'string' ? pairedTokens.get(msg.token) : null;
      if (t) {
        // Un token, una conexión activa: la nueva reemplaza a la anterior.
        const oldId = tokenSockets.get(msg.token);
        if (oldId && oldId !== id) {
          const old = clients.get(oldId);
          if (old && old.ws.readyState === WebSocket.OPEN) {
            try { old.ws.close(1008, 'reemplazada por nueva conexión'); } catch {}
          }
        }
        me.paired = true;
        me.token = msg.token;
        me.deviceId = t.deviceId;
        me.isAdmin = isAdminIp(ip);
        if (typeof msg.name === 'string' && msg.name.trim()) {
          me.name = msg.name.trim().slice(0, 40);
        } else {
          me.name = t.name;
        }
        t.name = me.name;
        tokenSockets.set(msg.token, id);
        saveTokens();
        clearTimeout(pairTimer);
        ws.send(JSON.stringify({ type: 'paired', deviceId: me.deviceId, admin: me.isAdmin }));
        broadcastRoster();
      } else {
        ws.send(JSON.stringify({ type: 'auth_required' }));
      }
      return;
    }

    // De aquí en adelante, solo equipos vinculados.
    if (!me.paired) return;

    // Desvincular este equipo (p. ej. iPhone perdido).
    if (msg.type === 'unpair' && me.token) {
      pairedTokens.delete(me.token);
      tokenSockets.delete(me.token);
      saveTokens();
      me.paired = false;
      me.token = null;
      me.deviceId = null;
      ws.send(JSON.stringify({ type: 'auth_required' }));
      broadcastRoster();
      rearmPairTimer();
      return;
    }

    // Revocar el acceso de OTRO equipo vinculado (p. ej. iPhone perdido).
    // SOLO la PC (localhost) administra: un iPhone vinculado no puede
    // revocar a nadie, ni siquiera conociendo el deviceId.
    if (msg.type === 'revoke' && typeof msg.deviceId === 'string') {
      if (!me.paired || !me.isAdmin) return;
      let targetToken = null;
      for (const [tok, t] of pairedTokens) {
        if (t.deviceId === msg.deviceId && tok !== me.token) {
          targetToken = tok;
          break;
        }
      }
      if (targetToken) {
        pairedTokens.delete(targetToken);
        const sid = tokenSockets.get(targetToken);
        if (sid) {
          const s = clients.get(sid);
          if (s && s.ws.readyState === WebSocket.OPEN) {
            try { s.ws.close(1008, 'acceso revocado'); } catch {}
          }
          tokenSockets.delete(targetToken);
        }
        saveTokens();
        broadcastRoster();
      }
      return;
    }

    // Lista de TODOS los equipos vinculados (conectados o no), para poder
    // revocar incluso a los que están apagados o fuera de casa.
    if (msg.type === 'linked') {
      if (!me.paired) return;
      const devices = [];
      for (const [tok, t] of pairedTokens) {
        const sid = tokenSockets.get(tok);
        devices.push({
          deviceId: t.deviceId,
          name: t.name,
          connected: !!sid && clients.has(sid),
        });
      }
      try { ws.send(JSON.stringify({ type: 'linked', devices, youAreAdmin: !!me.isAdmin })); } catch {}
      return;
    }

    if (msg.type === 'rename' && typeof msg.name === 'string') {
      me.name = msg.name.trim().slice(0, 40) || me.name;
      const t = me.token ? pairedTokens.get(me.token) : null;
      if (t) t.name = me.name;
      saveTokens();
      broadcastRoster();
      return;
    }
    if (msg.type === 'signal' &&
        typeof msg.to === 'string' && msg.to.length > 0 && msg.to.length <= 64 &&
        isSafeObject(msg.payload)) {
      const target = clients.get(msg.to);
      if (target && target.paired && target.ws.readyState === WebSocket.OPEN) {
        // Identidad sellada por el servidor: el emisor no puede falsificar
        // su nombre ni su ID en la oferta.
        const stamped = Object.assign({}, msg.payload, {
          name: me.name,
          deviceId: me.deviceId,
        });
        target.ws.send(JSON.stringify({ type: 'signal', from: id, payload: stamped }));
      }
      return;
    }
  });

  // cleanup() puede dispararse dos veces para la misma conexión (eventos
  // 'error' y 'close'): el guard evita descontar el contador dos veces.
  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    clearInterval(heartbeat);
    clearTimeout(pairTimer);
    const n = (connsByIp.get(ip) || 1) - 1;
    if (n <= 0) connsByIp.delete(ip); else connsByIp.set(ip, n);
    const wasPaired = me.paired;
    if (me.token && tokenSockets.get(me.token) === id) tokenSockets.delete(me.token);
    if (clients.delete(id) && wasPaired) {
      broadcastRoster();
      const anyPaired = [...clients.values()].some((c) => c.paired);
      if (!anyPaired) rotateCode('todos los equipos se desconectaron');
    }
  };
  ws.on('close', cleanup);
  ws.on('error', cleanup);
});

loadTokens();
server.listen(PORT, () => {
  console.log('Pásalo escuchando en el puerto ' + PORT +
    ' (' + pairedTokens.size + ' equipo(s) vinculado(s) recordados)');
  printBanner();
});

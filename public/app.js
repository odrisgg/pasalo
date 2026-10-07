/* Pásalo — cliente (v2.0.1).
 * 1) Se conecta al servidor por WebSocket (solo señalización).
 * 2) Intercambia oferta/respuesta/candidatos con el otro equipo.
 * 3) El receptor APRUEBA cada transferencia antes de que empiece.
 * 4) El archivo viaja directo por WebRTC DataChannel, en trozos de 64 KB.
 * El servidor jamás ve el contenido de tus archivos.
 */
(() => {
'use strict';

const $ = (s) => document.querySelector(s);
const PASALO_VERSION = '2.0.1';
const CHUNK_SIZE = 64 * 1024;              // 64 KB por trozo
const MAX_BUFFERED = 512 * 1024;           // pausa el envío si el canal lleva 512 KB en cola
// 1 GB por archivo: en el iPhone los trozos se guardan en memoria antes de
// armar el archivo, y Safari se cierra con archivos mucho más grandes.
const MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024;
const ANSWER_TIMEOUT_MS = 30000;           // 30 s esperando que el otro equipo acepte
const ACK_TIMEOUT_MS = 15000;              // 15 s esperando confirmación de cada archivo
const ICE_TIMEOUT_MS = 20000;              // 20 s para establecer la conexión directa
const MAX_BUFFERED_WS = 4 * 1024 * 1024;   // pausa el puente si el socket lleva 4 MB en cola

const els = {
  status: $('#status'),
  myName: $('#myName'),
  qrSection: $('#qrSection'),
  qrImg: $('#qrImg'),
  qrUrls: $('#qrUrls'),
  devices: $('#devices'),
  empty: $('#emptyDevices'),
  transfers: $('#transfers'),
  received: $('#received'),
  fileInput: $('#fileInput'),
  pairView: $('#pairView'),
  app: $('#app'),
  codeInput: $('#codeInput'),
  pairBtn: $('#pairBtn'),
  pairError: $('#pairError'),
  localCode: $('#localCode'),
  localCodeVal: $('#localCodeVal'),
};

let ws = null;
let myId = null;
let myDeviceId = null;
let myAdmin = false;
let myName = localStorage.getItem('pasalo_name') || guessName();
let token = localStorage.getItem('pasalo_token') || null;
let paired = false;
let pendingPeer = null;
let seq = 0;
const pcs = new Map();        // "peerId|session" -> RTCPeerConnection
const sendMeta = new Map();   // "peerId|session" -> { row, timer }
const authorizedFiles = new Map(); // "peerId|session" -> [{name,size}] aprobados al aceptar
const iceBuffer = createIceBuffer(); // candidatos ICE que llegan antes de tiempo
const relaySends = new Map();   // "peerId|session" -> { peerId, session, files, row, acks, timer }
const relayRecvs = new Map();   // "peerId|session" -> { peerId, session, authorized, fileIndex, incoming }
const acceptTimers = new Map(); // peerId -> watchdog de conexión directa tras aceptar
const offerQueue = [];
let offerDialogOpen = false;
const receivedUrls = [];
// Al cerrar la página se liberan los enlaces de descarga creados.
window.addEventListener('beforeunload', () => {
  for (const u of receivedUrls) { try { URL.revokeObjectURL(u); } catch (e) {} }
});

els.myName.value = myName;

function setPairedUI(on) {
  paired = on;
  els.pairView.hidden = on;
  els.app.hidden = !on;
  if (on) { loadPersistedReceived(); loadQr(); }
}

/* -------- QR para abrir en el celular sin escribir la IP --------
 * Solo se muestra en la PC (localhost): el celular ya está en la página.
 * Librería: qrcode-generator (MIT, Kazuhiko Arase), vendored en
 * public/qrcode.min.js — ver public/qrcode-LICENSE.txt.
 */
async function loadQr() {
  try {
    if (!/^(localhost|127\.0\.0\.1)$/.test(location.hostname)) return;
    if (typeof qrcode === 'undefined') return;
    const r = await fetch('lan-urls');
    if (!r.ok) return;
    const d = await r.json();
    const urls = d && Array.isArray(d.urls) ? d.urls.filter((u) => typeof u === 'string') : [];
    if (!urls.length) return;
    // Dibuja el QR con la dirección elegida. Si la PC tiene LAN + Wi-Fi,
    // el primero de la lista puede no ser el que alcanza el celular:
    // tocar una dirección regenera el QR con esa.
    const draw = (url) => {
      const qr = qrcode(0, 'M');
      qr.addData(url);
      qr.make();
      els.qrImg.innerHTML = qr.createSvgTag(4, 0);
    };
    els.qrUrls.innerHTML = '';
    urls.forEach((u, i) => {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'qr-url' + (i === 0 ? ' active' : '');
      b.textContent = u;
      b.title = 'Usar esta dirección en el QR';
      b.addEventListener('click', () => {
        draw(u);
        els.qrUrls.querySelectorAll('.qr-url').forEach((x) => x.classList.remove('active'));
        b.classList.add('active');
      });
      els.qrUrls.appendChild(b);
    });
    draw(urls[0]);
    els.qrSection.hidden = false;
  } catch (e) { /* sin QR: la URL escrita sigue funcionando */ }
}

/* -------- Recibidos persistentes (IndexedDB) --------
 * Los archivos recibidos se guardan en el navegador y sobreviven
 * al refresh. Cada item se puede quitar de la lista (y del disco).
 */
const idbRecv = {
  _db: null,
  _nope: typeof indexedDB === 'undefined',
  open() {
    return new Promise((resolve, reject) => {
      if (this._nope) return reject(new Error('sin IndexedDB'));
      if (this._db) return resolve(this._db);
      let req;
      try { req = indexedDB.open('pasalo', 1); }
      catch (e) { return reject(e); }
      req.onupgradeneeded = () => {
        req.result.createObjectStore('received', { keyPath: 'id' });
      };
      req.onsuccess = () => { this._db = req.result; resolve(this._db); };
      req.onerror = () => reject(req.error);
    });
  },
  async put(rec) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction('received', 'readwrite');
      tx.objectStore('received').put(rec);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  },
  async list() {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const req = db.transaction('received', 'readonly').objectStore('received').getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  },
  async del(id) {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction('received', 'readwrite');
      tx.objectStore('received').delete(id);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  },
};

function addReceivedItem(name, size, blob, rid) {
  const url = URL.createObjectURL(blob);
  receivedUrls.push(url);
  const hint = els.received.querySelector('.hint');
  if (hint) hint.remove();
  const item = document.createElement('div');
  item.className = 'received-item';
  const link = document.createElement('a');
  link.href = url;
  link.download = name;
  link.textContent = '⬇ ' + name;
  const sizeEl = document.createElement('span');
  sizeEl.className = 'r-size';
  sizeEl.textContent = ' (' + fmtSize(size) + ')';
  const del = document.createElement('button');
  del.className = 'r-del';
  del.textContent = '✕';
  del.title = 'Quitar de la lista';
  del.addEventListener('click', async () => {
    if (rid) { try { await idbRecv.del(rid); } catch (e) {} }
    const i = receivedUrls.indexOf(url);
    if (i !== -1) receivedUrls.splice(i, 1);
    try { URL.revokeObjectURL(url); } catch (e) {}
    item.remove();
    if (!els.received.querySelector('.received-item')) {
      const h = document.createElement('div');
      h.className = 'hint';
      h.textContent = 'Aquí aparecen los archivos que recibas.';
      els.received.appendChild(h);
    }
  });
  item.appendChild(link);
  item.appendChild(sizeEl);
  item.appendChild(del);
  els.received.prepend(item);
  return url;
}

let persistedLoaded = false;
async function loadPersistedReceived() {
  if (persistedLoaded) return;
  persistedLoaded = true;
  try {
    const recs = await idbRecv.list();
    recs.sort((a, b) => (b.date || 0) - (a.date || 0));
    for (const r of recs) {
      if (r && r.blob) addReceivedItem(r.name, r.size, r.blob, r.id);
    }
  } catch (e) { /* sin persistencia: la lista en memoria sigue funcionando */ }
}

// Si esta página se abrió en la propia PC, muestra el código y lo pre-rellena.
fetch('pairing-code').then((r) => {
  if (!r.ok) throw new Error('no local');
  return r.json();
}).then((d) => {
  if (d && typeof d.code === 'string') {
    els.localCode.hidden = false;
    els.localCodeVal.textContent = d.code.slice(0, 3) + ' ' + d.code.slice(3);
    els.codeInput.value = d.code;
  }
}).catch(() => {});

els.pairBtn.addEventListener('click', () => {
  const code = els.codeInput.value.replace(/\D/g, '').slice(0, 6);
  if (code.length !== 6 || !ws || ws.readyState !== WebSocket.OPEN) return;
  els.pairError.hidden = true;
  els.pairBtn.disabled = true;
  send({ type: 'pair', code });
});
els.codeInput.addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter') els.pairBtn.click();
});

$('#unpairBtn').addEventListener('click', () => {
  if (!paired) return;
  if (confirm('¿Desvincular este equipo? Tendrás que escribir el código de nuevo para usarlo.')) {
    send({ type: 'unpair' });
  }
});

function guessName() {
  const ua = navigator.userAgent || '';
  if (/iPhone/i.test(ua)) return 'iPhone';
  if (/iPad/i.test(ua)) return 'iPad';
  if (/Android/i.test(ua)) return 'Android';
  if (/Windows/i.test(ua)) return 'PC Windows';
  if (/Macintosh|Mac OS/i.test(ua)) return 'Mac';
  if (/Linux/i.test(ua)) return 'PC Linux';
  return 'Mi equipo';
}

function deviceIcon(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('iphone') || n.includes('ipad') || n.includes('android') ||
      n.includes('teléfono') || n.includes('celular') || n.includes('phone')) return '📱';
  if (n.includes('windows') || n.includes('mac') || n.includes('linux') ||
      n.includes('pc') || n.includes('laptop')) return '💻';
  return '🖥️';
}

/* ---------------- señalización ---------------- */

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  setStatus('connecting', 'Conectando…');
  ws = new WebSocket(proto + '//' + location.host);

  ws.onopen = () => {
    setStatus('online', 'Conectado');
    // Reconexión automática si este equipo ya se vinculó antes.
    if (token) send({ type: 'hello', token, name: myName });
  };
  ws.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (!msg || typeof msg !== 'object') return;
    if (msg.type === 'welcome') myId = msg.id;
    else if (msg.type === 'paired') {
      if (msg.token) {
        token = msg.token;
        localStorage.setItem('pasalo_token', token);
        // Avisar el nombre elegido junto con la vinculación.
        send({ type: 'rename', name: myName });
      }
      if (msg.deviceId) myDeviceId = msg.deviceId;
      myAdmin = !!msg.admin;
      els.pairBtn.disabled = false;
      els.pairError.hidden = true;
      setPairedUI(true);
      setStatus('online', 'Conectado');
      refreshLinked();
    }
    else if (msg.type === 'pair_error') {
      els.pairBtn.disabled = false;
      els.pairError.hidden = false;
    }
    else if (msg.type === 'auth_required') {
      // El servidor se reinició: hay que vincular de nuevo con el código.
      token = null;
      localStorage.removeItem('pasalo_token');
      setPairedUI(false);
    }
    else if (msg.type === 'roster') { if (paired) { renderDevices(msg.clients || []); refreshLinked(); } }
    else if (msg.type === 'linked') { if (paired) renderLinked(msg.devices || [], msg.youAreAdmin); }
    else if (msg.type === 'signal') { if (paired) handleSignal(msg.from, msg.payload); }
  };
  ws.onclose = () => {
    setStatus('offline', 'Desconectado — reintentando…');
    setTimeout(connect, 2500);
  };
  ws.onerror = () => { try { ws.close(); } catch (e) {} };
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function setStatus(cls, text) {
  els.status.className = 'status ' + cls;
  els.status.textContent = text;
}

els.myName.addEventListener('change', () => {
  myName = els.myName.value.trim().slice(0, 40) || guessName();
  els.myName.value = myName;
  localStorage.setItem('pasalo_name', myName);
  send({ type: 'rename', name: myName });
});

/* ---------------- equipos ---------------- */

function renderDevices(clients) {
  const others = clients.filter((c) => c.id !== myId);
  els.devices.innerHTML = '';
  els.empty.style.display = others.length ? 'none' : 'block';
  for (const c of others) {
    const card = document.createElement('div');
    card.className = 'device';
    const icon = document.createElement('div');
    icon.className = 'device-icon';
    icon.textContent = deviceIcon(c.name);
    const name = document.createElement('div');
    name.className = 'device-name';
    name.textContent = c.name;
    const did = c.deviceId ? document.createElement('div') : null;
    if (did) {
      did.className = 'device-id';
      did.textContent = 'ID ' + c.deviceId;
    }
    const btn = document.createElement('button');
    btn.className = 'btn';
    btn.textContent = 'Enviar archivos';
    btn.addEventListener('click', () => {
      pendingPeer = c.id;
      els.fileInput.click();
    });
    card.appendChild(icon);
    card.appendChild(name);
    if (did) card.appendChild(did);
    card.appendChild(btn);
    els.devices.appendChild(card);
  }
}

function refreshLinked() {
  if (paired && ws && ws.readyState === WebSocket.OPEN) send({ type: 'linked' });
}

// Panel "Equipos vinculados": TODOS los equipos con acceso, conectados o no.
// El botón de revocar solo lo ve la PC (admin) y nunca para sí misma.
function renderLinked(devices, youAreAdmin) {
  const box = document.getElementById('linked');
  if (!box) return;
  box.innerHTML = '';
  const admin = !!(youAreAdmin || myAdmin);
  for (const d of devices) {
    const card = document.createElement('div');
    card.className = 'device';
    const icon = document.createElement('div');
    icon.className = 'device-icon';
    icon.textContent = deviceIcon(d.name || 'Equipo');
    const name = document.createElement('div');
    name.className = 'device-name';
    name.textContent = d.name || 'Equipo';
    const did = document.createElement('div');
    did.className = 'device-id';
    did.textContent = 'ID ' + (d.deviceId || '?') + (d.connected ? '' : ' · desconectado');
    card.appendChild(icon);
    card.appendChild(name);
    card.appendChild(did);
    const isMe = !!(d.deviceId && myDeviceId && d.deviceId === myDeviceId);
    if (isMe) {
      const me = document.createElement('div');
      me.className = 'device-id';
      me.textContent = '(este equipo)';
      card.appendChild(me);
    } else if (admin && d.deviceId) {
      const rev = document.createElement('button');
      rev.className = 'revoke-btn';
      rev.textContent = 'Revocar acceso';
      rev.addEventListener('click', () => {
        if (confirm('¿Revocar el acceso de "' + (d.name || 'este equipo') + '"? Ya no podrá conectarse.')) {
          send({ type: 'revoke', deviceId: d.deviceId });
        }
      });
      card.appendChild(rev);
    }
    box.appendChild(card);
  }
  if (!devices.length) {
    const p = document.createElement('p');
    p.className = 'hint';
    p.textContent = 'Aún no hay equipos vinculados.';
    box.appendChild(p);
  }
}

els.fileInput.addEventListener('change', () => {
  const files = Array.from(els.fileInput.files || []);
  els.fileInput.value = '';
  if (files.length && pendingPeer) sendFiles(pendingPeer, files);
  pendingPeer = null;
});

/* ---------------- WebRTC ---------------- */

function newPC(peerId, session) {
  // Sin servidores STUN/TURN externos: conexión directa en tu red.
  const pc = new RTCPeerConnection({ iceServers: [] });
  pc.onicecandidate = (ev) => {
    if (ev.candidate) {
      send({ type: 'signal', to: peerId, payload: { kind: 'ice', session, candidate: ev.candidate } });
    }
  };
  return pc;
}

async function handleSignal(from, p) {
  if (!p || typeof p !== 'object') return;
  const key = from + '|' + (p.session || '');
  try {
    if (p.kind === 'offer') {
      // Nada se acepta solo: el receptor decide con un diálogo visible.
      // El nombre y el ID vienen sellados por el servidor (no falsificables).
      // La lista de archivos la autoriza el receptor al aceptar (se verifica
      // al recibir: el emisor no puede cambiarla después).
      const files = sanitizeFileList(p.files);
      offerQueue.push({
        from,
        name: typeof p.name === 'string' ? p.name.slice(0, 40) : 'Un equipo',
        deviceId: typeof p.deviceId === 'string' ? p.deviceId : '?',
        files,
        session: p.session,
        sdp: p.sdp,
      });
      pumpOfferQueue();
    } else if (p.kind === 'rejected') {
      const meta = sendMeta.get(key);
      const pc = pcs.get(key);
      if (pc) { try { pc.close(); } catch (e) {} pcs.delete(key); }
      iceBuffer.drop(key);
      if (meta) {
        clearTimeout(meta.timer);
        meta.row.label('El otro equipo rechazó la transferencia');
        meta.row.set(0, false, true);
        sendMeta.delete(key);
      }
    } else if (p.kind === 'answer' || p.kind === 'ice') {
      if (p.kind === 'ice' && p.candidate) {
        const pc = pcs.get(key);
        // Los candidatos pueden llegar antes de que exista la conexión
        // (el receptor aún no aceptó) o antes de la descripción remota:
        // se guardan y se aplican en cuanto se pueda, no se descartan.
        if (!pc || !pc.remoteDescription) {
          iceBuffer.add(key, p.candidate);
          return;
        }
        try { await pc.addIceCandidate(new RTCIceCandidate(p.candidate)); } catch (e) {}
      } else if (p.kind === 'answer' && p.sdp) {
        const pc = pcs.get(key);
        if (!pc) return;
        await pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
        for (const c of iceBuffer.take(key)) {
          try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) {}
        }
        // La respuesta llegó: ahora el límite es lograr la conexión ICE.
        const meta = sendMeta.get(key);
        if (meta) {
          clearTimeout(meta.timer);
          meta.timer = setTimeout(() => {
            if (pcs.has(key)) {
              try { pc.close(); } catch (e) {}
              pcs.delete(key);
              iceBuffer.drop(key);
              sendMeta.delete(key);
              // La red bloquea la conexión directa: recordarlo y cambiar
              // al modo puente (los archivos viajan por tu propia PC).
              try { localStorage.setItem('pasaloNoP2P', '1'); } catch (e) {}
              sendViaRelay(meta.peerId, meta.files, meta.row);
            }
          }, ICE_TIMEOUT_MS);
        }
      }
    } else if (typeof p.kind === 'string' && p.kind.startsWith('relay-')) {
        handleRelaySignal(from, key, p);
      }
    } catch (err) {
    console.error('Señalización:', err);
  }
}

/* -------- diálogo de aceptación -------- */

function pumpOfferQueue() {
  if (offerDialogOpen || !offerQueue.length) return;
  showOfferDialog(offerQueue.shift());
}

function showOfferDialog(o) {
  offerDialogOpen = true;
  const overlay = document.createElement('div');
  overlay.className = 'offer-overlay';
  const box = document.createElement('div');
  box.className = 'offer-box';
  const title = document.createElement('div');
  title.className = 'offer-title';
  title.textContent = '📥 ' + o.name + ' · ' + o.deviceId + ' quiere enviarte archivos' +
    (o.viaRelay ? ' (vía tu PC)' : '');
  const sub = document.createElement('div');
  sub.className = 'offer-sub';
  let list = null;
  if (o.files && o.files.length) {
    sub.textContent = 'Quiere enviarte:';
    list = document.createElement('ul');
    list.className = 'offer-files';
    for (const f of o.files) {
      const li = document.createElement('li');
      const nm = document.createElement('span');
      nm.textContent = f.name;
      const sz = document.createElement('span');
      sz.className = 'r-size';
      sz.textContent = ' (' + fmtSize(f.size) + ')';
      li.appendChild(nm);
      li.appendChild(sz);
      list.appendChild(li);
    }
  } else {
    sub.textContent = '¿Aceptas la transferencia?';
  }
  const row = document.createElement('div');
  row.className = 'offer-btns';
  const ok = document.createElement('button');
  ok.className = 'btn ok';
  ok.textContent = 'Aceptar';
  const no = document.createElement('button');
  no.className = 'btn no';
  no.textContent = 'Rechazar';
  row.appendChild(ok);
  row.appendChild(no);
  box.appendChild(title);
  box.appendChild(sub);
  if (list) box.appendChild(list);
  box.appendChild(row);
  overlay.appendChild(box);
  document.body.appendChild(overlay);

  const done = () => {
    overlay.remove();
    offerDialogOpen = false;
    pumpOfferQueue();
  };
  ok.addEventListener('click', () => { done(); o.viaRelay ? acceptRelayOffer(o) : acceptOffer(o); });
  no.addEventListener('click', () => {
    done();
    send({ type: 'signal', to: o.from, payload: { kind: o.viaRelay ? 'relay-reject' : 'rejected', session: o.session } });
  });
}

async function acceptOffer(o) {
  const key = o.from + '|' + (o.session || '');
  // Guardar lo autorizado: al recibir se verifica que cada archivo sea
  // EXACTAMENTE el aprobado (nombre + tamaño + cantidad).
  authorizedFiles.set(key, sanitizeFileList(o.files));
  let iceTimer = null;
  try {
    const pc = newPC(o.from, o.session);
    pcs.set(key, pc);
    // Si el canal no se abre, la conexión directa falló: avisar en vez
    // de quedarse esperando para siempre.
    iceTimer = setTimeout(() => {
      acceptTimers.delete(o.from);
      const row = addTransferRow('No se pudo establecer la conexión directa con el otro equipo');
      row.set(0, false, true);
      try { pc.close(); } catch (e) {}
      pcs.delete(key);
      authorizedFiles.delete(key);
      iceBuffer.drop(key);
    }, ICE_TIMEOUT_MS + 5000);
    acceptTimers.set(o.from, iceTimer);
    pc.ondatachannel = (ev) => {
      clearTimeout(iceTimer);
      acceptTimers.delete(o.from);
      setupReceiver(key, pc, ev.channel);
    };
    await pc.setRemoteDescription(new RTCSessionDescription(o.sdp));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    // Aplicar los candidatos que llegaron antes de aceptar.
    for (const c of iceBuffer.take(key)) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) {}
    }
    send({ type: 'signal', to: o.from, payload: { kind: 'answer', session: o.session, sdp: pc.localDescription } });
  } catch (err) {
    console.error('Aceptar oferta:', err);
    if (iceTimer) clearTimeout(iceTimer);
    acceptTimers.delete(o.from);
    pcs.delete(key);
    authorizedFiles.delete(key);
    iceBuffer.drop(key);
  }
}

/* ---------------- envío ---------------- */

function addTransferRow(label, noStage) {
  const emptyHint = $('#emptyTransfers');
  if (emptyHint) emptyHint.remove();
  const row = document.createElement('div');
  row.className = 'transfer';
  const lab = document.createElement('div');
  lab.className = 't-label';
  lab.textContent = label;
  row.appendChild(lab);
  if (!noStage) {
    // Escena animada: la cajita viaja entre los equipos.
    const stage = document.createElement('div');
    stage.className = 't-stage';
    const devL = document.createElement('span');
    devL.className = 't-dev';
    devL.textContent = '📤';
    const track = document.createElement('div');
    track.className = 't-track';
    const pack = document.createElement('span');
    pack.className = 't-pack';
    pack.textContent = '📦';
    track.appendChild(pack);
    const devR = document.createElement('span');
    devR.className = 't-dev';
    devR.textContent = '📥';
    stage.appendChild(devL);
    stage.appendChild(track);
    stage.appendChild(devR);
    row.appendChild(stage);
  }
  const bar = document.createElement('div');
  bar.className = 'bar';
  const fill = document.createElement('div');
  fill.className = 'bar-fill';
  bar.appendChild(fill);
  const pct = document.createElement('div');
  pct.className = 't-pct';
  pct.textContent = '0%';
  row.appendChild(bar);
  row.appendChild(pct);
  els.transfers.prepend(row);
  return {
    label(t) { lab.textContent = t; },
    set(p, done, failed) {
      fill.style.width = Math.min(100, p) + '%';
      pct.textContent = failed ? 'Falló ✕' : (done ? 'Listo ✓' : Math.round(p) + '%');
      if (done) row.classList.add('done');
      if (failed) row.classList.add('failed');
      if (done || failed) row.classList.remove('st-sending', 'st-confirming');
    },
    // Estado de la animación: 'st-sending' | 'st-confirming' | null.
    anim(s) {
      row.classList.remove('st-sending', 'st-confirming');
      if (s) row.classList.add(s);
    },
    // Dirección de la animación: 'recv' para lo que llega.
    dir(d) { if (d === 'recv') row.classList.add('dir-recv'); },
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// "Enviando: video.mp4 · 8,2 MB/s · ~12 s" (vacío si aún no hay medida).
function speedSuffix(meter, done, total) {
  const sp = meter.speed();
  if (sp <= 0) return '';
  return ' · ' + fmtSpeed(sp) + ' · ' + fmtETA((total - done) / sp);
}

async function sendFiles(peerId, files) {
  // Si la conexión directa ya falló antes en esta red, ir directo al puente.
  try {
    if (localStorage.getItem('pasaloNoP2P') === '1') { sendViaRelay(peerId, files, null); return; }
  } catch (e) {}
  const session = 's' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const key = peerId + '|' + session;
  const pc = newPC(peerId, session);
  pcs.set(key, pc);
  const row = addTransferRow('Esperando que el otro equipo acepte…');
  const meta = { row, timer: null, peerId, files };
  sendMeta.set(key, meta);

  // Si el otro equipo no responde, no quedarse colgado.
  meta.timer = setTimeout(() => {
    if (pcs.has(key)) {
      try { pc.close(); } catch (e) {}
      pcs.delete(key);
      iceBuffer.drop(key);
      row.label('Sin respuesta del otro equipo');
      row.set(0, false, true);
      sendMeta.delete(key);
    }
  }, ANSWER_TIMEOUT_MS);

  const dc = pc.createDataChannel('pasalo');
  dc.binaryType = 'arraybuffer';

  // El receptor confirma cada archivo ({t:'ack', id}) tras verificar los
  // bytes: solo entonces se muestra "Listo". Sin confirmación se avisa.
  const acks = createAckTracker(ACK_TIMEOUT_MS);
  dc.onmessage = (ev) => {
    if (typeof ev.data !== 'string') return;
    try {
      const m = JSON.parse(ev.data);
      if (m && m.t === 'ack' && typeof m.id === 'string') acks.got(m.id);
    } catch (e) {}
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'failed') {
      clearTimeout(meta.timer);
      acks.clear();
      try { pc.close(); } catch (e) {}
      pcs.delete(key);
      iceBuffer.drop(key);
      sendMeta.delete(key);
    }
  };

  dc.onopen = async () => {
    clearTimeout(meta.timer);
    try { localStorage.removeItem('pasaloNoP2P'); } catch (e) {}
    row.anim('st-sending');
    try {
      for (const file of files) {
        const id = 'f' + (++seq) + Date.now().toString(36);
        row.label('Enviando: ' + file.name);
        dc.send(JSON.stringify({
          t: 'meta', id,
          name: file.name, size: file.size,
          mime: file.type || 'application/octet-stream',
        }));
        let offset = 0;
        const meter = createSpeedometer();
        let lastUi = 0;
        while (offset < file.size) {
          while (dc.bufferedAmount > MAX_BUFFERED) await sleep(60);
          if (dc.readyState !== 'open') throw new Error('canal cerrado');
          const buf = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
          dc.send(buf);
          offset += buf.byteLength;
          row.set((offset / file.size) * 100, false);
          meter.push(buf.byteLength);
          const nowUi = Date.now();
          if (nowUi - lastUi > 500) {
            lastUi = nowUi;
            row.label('Enviando: ' + file.name + speedSuffix(meter, offset, file.size));
          }
        }
        dc.send(JSON.stringify({ t: 'done', id }));
        // El temporizador arranca AQUÍ, después de terminar de enviar:
        // si arrancara antes, un archivo grande lo vencería a mitad
        // de la transferencia y mostraría "sin confirmación" por error.
        const ackP = acks.wait(id);
        row.anim('st-confirming');
        row.label('Esperando confirmación: ' + file.name);
        const confirmed = await ackP;
        if (confirmed) {
          row.label('Recibido: ' + file.name);
          row.set(100, true);
        } else {
          // Los bytes se enviaron, pero el receptor no confirmó.
          row.label('Enviado sin confirmación: ' + file.name + ' ⚠');
          row.set(100, false);
        }
      }
      dc.send(JSON.stringify({ t: 'bye' }));
      await sleep(500);
    } catch (err) {
      console.error('Envío:', err);
      acks.clear();
      row.set(0, false, true);
    } finally {
      try { dc.close(); } catch (e) {}
      try { pc.close(); } catch (e) {}
      pcs.delete(key);
      sendMeta.delete(key);
    }
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  // La oferta anuncia qué archivos vienen, para mostrarlos antes de aceptar.
  const fileList = files.map((f) => ({ name: f.name, size: f.size }));
  send({ type: 'signal', to: peerId, payload: { kind: 'offer', session, name: myName, files: fileList, sdp: pc.localDescription } });
}

/* ---------------- recepción ---------------- */

/* -------- modo puente: los archivos viajan por tu propia PC --------
 * Se usa automáticamente cuando la red bloquea la conexión directa
 * (firewalls, WiFi que no deja hablar a los equipos entre sí, etc.).
 * Todo sigue dentro de tu red: el servidor solo reenvía los trozos.
 */

function sendViaRelay(peerId, files, reuseRow) {
  const session = 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const key = peerId + '|' + session;
  const row = reuseRow || addTransferRow('Esperando que el otro equipo acepte…');
  row.label(reuseRow
    ? 'Conexión directa no disponible — enviando por tu PC…'
    : 'Enviando por tu PC…');
  row.set(0, false);
  const st = { peerId, session, files, row, acks: createAckTracker(ACK_TIMEOUT_MS), timer: null };
  relaySends.set(key, st);
  st.timer = setTimeout(() => {
    if (relaySends.has(key)) {
      relaySends.delete(key);
      row.label('Sin respuesta del otro equipo');
      row.set(0, false, true);
    }
  }, ANSWER_TIMEOUT_MS);
  const fileList = files.map((f) => ({ name: f.name, size: f.size }));
  send({ type: 'signal', to: peerId, payload: { kind: 'relay-offer', session, files: fileList } });
}

async function streamRelay(st, key) {
  const { files, row, acks, peerId, session } = st;
  row.anim('st-sending');
  try {
    for (const file of files) {
      if (!relaySends.has(key)) throw new Error('cancelado');
      const id = 'f' + (++seq) + Date.now().toString(36);
      row.label('Enviando por tu PC: ' + file.name);
      send({ type: 'signal', to: peerId, payload: {
        kind: 'relay-meta', session, id,
        // OJO: no usar "name" — el servidor lo sobrescribe con el nombre
        // del equipo al sellar la identidad. El archivo va en "fileName".
        fileName: file.name, size: file.size,
        mime: file.type || 'application/octet-stream',
      }});
      let offset = 0;
      const meter = createSpeedometer();
      let lastUi = 0;
      while (offset < file.size) {
        while (ws.bufferedAmount > MAX_BUFFERED_WS && relaySends.has(key)) await sleep(100);
        if (!relaySends.has(key)) throw new Error('cancelado');
        const buf = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
        send({ type: 'signal', to: peerId, payload: {
          kind: 'relay-chunk', session, id, data: b64encodeBytes(new Uint8Array(buf)),
        }});
        offset += buf.byteLength;
        row.set((offset / file.size) * 100, false);
        meter.push(buf.byteLength);
        const nowUi = Date.now();
        if (nowUi - lastUi > 500) {
          lastUi = nowUi;
          row.label('Enviando por tu PC: ' + file.name + speedSuffix(meter, offset, file.size));
        }
      }
      send({ type: 'signal', to: peerId, payload: { kind: 'relay-done', session, id } });
      // El temporizador arranca después de terminar de enviar, igual que en directo.
      const ackP = acks.wait(id);
      row.anim('st-confirming');
      row.label('Esperando confirmación: ' + file.name);
      const confirmed = await ackP;
      if (confirmed) {
        row.label('Recibido: ' + file.name);
        row.set(100, true);
      } else {
        row.label('Enviado sin confirmación: ' + file.name + ' ⚠');
        row.set(100, false);
      }
    }
  } catch (err) {
    console.error('Envío por puente:', err);
    st.acks.clear();
    row.set(0, false, true);
  } finally {
    if (st.timer) clearTimeout(st.timer);
    relaySends.delete(key);
  }
}

function acceptRelayOffer(o) {
  const key = o.from + '|' + (o.session || '');
  // Lo autorizado al aceptar: al recibir se verifica nombre + tamaño.
  const rs = {
    peerId: o.from, session: o.session,
    authorized: sanitizeFileList(o.files), fileIndex: 0, incoming: null, watchdog: null,
  };
  relayRecvs.set(key, rs);
  armRelayWatchdog(rs, key);
  send({ type: 'signal', to: o.from, payload: { kind: 'relay-answer', session: o.session } });
}

function armRelayWatchdog(rs, key) {
  // Si el emisor se desconecta a mitad del puente (sin mandar done),
  // no quedarse esperando para siempre.
  if (rs.watchdog) clearTimeout(rs.watchdog);
  rs.watchdog = setTimeout(() => {
    if (rs.incoming) {
      rs.incoming.row.label('Transferencia interrumpida por el emisor');
      rs.incoming.row.set(rs.incoming.size ? (rs.incoming.received / rs.incoming.size) * 100 : 0, false, true);
      rs.incoming.chunks = [];
      rs.incoming = null;
    }
    relayRecvs.delete(key);
  }, 60000);
}

function clearRelayWatchdog(rs) {
  if (rs && rs.watchdog) { clearTimeout(rs.watchdog); rs.watchdog = null; }
}

function abortRelay(rs, key, why) {
  clearRelayWatchdog(rs);
  if (rs.incoming) {
    rs.incoming.row.label(why);
    rs.incoming.row.set(0, false, true);
    rs.incoming.chunks = [];
    rs.incoming = null;
  }
  relayRecvs.delete(key);
}

function handleRelaySignal(from, key, p) {
  if (p.kind === 'relay-offer') {
    // Si había un intento directo a medias con este equipo, ya no hace falta.
    const at = acceptTimers.get(from);
    if (at) { clearTimeout(at); acceptTimers.delete(from); }
    const files = sanitizeFileList(p.files);
    offerQueue.push({
      from,
      name: typeof p.name === 'string' ? p.name.slice(0, 40) : 'Un equipo',
      deviceId: typeof p.deviceId === 'string' ? p.deviceId : '?',
      files,
      session: p.session,
      viaRelay: true,
    });
    pumpOfferQueue();
    return;
  }
  if (p.kind === 'relay-reject') {
    const st = relaySends.get(key);
    if (st) {
      if (st.timer) clearTimeout(st.timer);
      relaySends.delete(key);
      st.row.label('El otro equipo rechazó la transferencia');
      st.row.set(0, false, true);
    }
    return;
  }
  if (p.kind === 'relay-answer') {
    const st = relaySends.get(key);
    if (!st) return;
    if (st.timer) { clearTimeout(st.timer); st.timer = null; }
    streamRelay(st, key);
    return;
  }
  if (p.kind === 'relay-ack') {
    const st = relaySends.get(key);
    if (st && typeof p.id === 'string') st.acks.got(p.id);
    return;
  }
  const rs = relayRecvs.get(key);
  if (!rs) return;
  if (p.kind === 'relay-meta') {
    // Igual que en directo: un meta con otro archivo pendiente bloquea la sesión.
    if (rs.incoming) {
      abortRelay(rs, key, 'Bloqueado: el emisor anunció un archivo nuevo sin terminar el anterior');
      return;
    }
    const size = Number(p.size) || 0;
    // El nombre del archivo viaja en "fileName": "name" lo pisa el servidor
    // con el nombre del equipo al sellar la identidad.
    const name = String(p.fileName || 'archivo').slice(0, 200);
    const v = verifyFileMeta(rs.authorized, { name, size }, rs.fileIndex);
    if (!v.ok) {
      abortRelay(rs, key, 'Bloqueado: el emisor cambió los archivos aprobados (' + v.reason + ')');
      return;
    }
    rs.fileIndex++;
    if (size > MAX_FILE_SIZE) {
      const row = addTransferRow('Rechazado automáticamente (pesa ' + fmtSize(size) +
        ', máximo ' + fmtSize(MAX_FILE_SIZE) + '): ' + name);
      row.set(0, false, true);
      clearRelayWatchdog(rs);
      relayRecvs.delete(key);
      return;
    }
    rs.incoming = {
      id: p.id, name, size,
      mime: String(p.mime || 'application/octet-stream').slice(0, 100),
      received: 0, chunks: [],
      meter: createSpeedometer(), lastUi: 0,
      row: addTransferRow('Recibiendo por tu PC: ' + name),
    };
    rs.incoming.row.dir('recv');
    rs.incoming.row.anim('st-sending');
    armRelayWatchdog(rs, key);
  } else if (p.kind === 'relay-chunk') {
    const inc = rs.incoming;
    if (!inc || p.id !== inc.id) return;
    // 64 KB en base64 ≈ 87 KB de texto; más de eso es inválido.
    if (typeof p.data !== 'string' || p.data.length > 120000) return;
    let bytes;
    try { bytes = b64decodeToBytes(p.data); } catch (e) {
      abortRelay(rs, key, 'Transferencia abortada: datos inválidos del emisor');
      return;
    }
    inc.chunks.push(bytes);
    inc.received += bytes.byteLength;
    if (inc.received > inc.size || inc.received > MAX_FILE_SIZE) {
      abortRelay(rs, key, 'Transferencia abortada: datos inválidos del emisor');
      return;
    }
    inc.row.set(inc.size ? (inc.received / inc.size) * 100 : 0, false);
    inc.meter.push(bytes.byteLength);
    const nowRx = Date.now();
    if (nowRx - inc.lastUi > 500) {
      inc.lastUi = nowRx;
      inc.row.label('Recibiendo por tu PC: ' + inc.name +
        speedSuffix(inc.meter, inc.received, inc.size));
    }
    armRelayWatchdog(rs, key);
  } else if (p.kind === 'relay-done') {
    const inc = rs.incoming;
    if (!inc || p.id !== inc.id) return;
    clearRelayWatchdog(rs);
    if (inc.received !== inc.size) {
      inc.row.label('Transferencia incompleta: llegaron ' + fmtSize(inc.received) +
        ' de ' + fmtSize(inc.size) + ' — archivo descartado');
      inc.row.set(0, false, true);
    } else {
      try { send({ type: 'signal', to: from, payload: { kind: 'relay-ack', session: rs.session, id: p.id } }); } catch (e) {}
      finishIncoming(inc);
    }
    rs.incoming = null;
  }
}

function setupReceiver(key, pc, dc) {
  dc.binaryType = 'arraybuffer';
  let incoming = null;
  // Lo que el receptor autorizó al aceptar (nombre+tamaño de cada archivo).
  const authorized = authorizedFiles.get(key) || [];
  let fileIndex = 0;

  const abort = (why) => {
    if (incoming) incoming.row.set(0, false, true);
    if (incoming) incoming.row.label(why);
    try { pc.close(); } catch (e) {}
    pcs.delete(key);
    authorizedFiles.delete(key);
    incoming = null;
  };

  dc.onmessage = (ev) => {
    if (typeof ev.data === 'string') {
      let m;
      try { m = JSON.parse(ev.data); } catch { return; }
      if (!m || typeof m !== 'object') return;
      if (m.t === 'meta') {
        // Un emisor bien comportado termina cada archivo (done+ack) antes de
        // anunciar el siguiente: si llega un meta con otro pendiente, se
        // bloquea la sesión en vez de reemplazar el estado en silencio.
        if (incoming) {
          abort('Bloqueado: el emisor anunció un archivo nuevo sin terminar el anterior');
          return;
        }
        const size = Number(m.size) || 0;
        const name = String(m.name || 'archivo').slice(0, 200);
        // El emisor no puede cambiar los archivos después de tu aprobación:
        // cada meta debe coincidir con lo autorizado (nombre + tamaño).
        const v = verifyFileMeta(authorized, { name, size }, fileIndex);
        if (!v.ok) {
          abort('Bloqueado: el emisor cambió los archivos aprobados (' + v.reason + ')');
          return;
        }
        fileIndex++;
        if (size > MAX_FILE_SIZE) {
          const row = addTransferRow('Rechazado automáticamente (pesa ' + fmtSize(size) +
            ', máximo ' + fmtSize(MAX_FILE_SIZE) + '): ' + name);
          row.set(0, false, true);
          try { pc.close(); } catch (e) {}
          pcs.delete(key);
          return;
        }
        incoming = {
          id: m.id, name, size,
          mime: String(m.mime || 'application/octet-stream').slice(0, 100),
          received: 0, chunks: [],
          meter: createSpeedometer(), lastUi: 0,
          row: addTransferRow('Recibiendo: ' + name),
        };
        incoming.row.dir('recv');
        incoming.row.anim('st-sending');
      } else if (m.t === 'done' && incoming && m.id === incoming.id) {
        // No dar por buena la transferencia hasta comprobar que llegaron
        // todos los bytes anunciados.
        if (incoming.received !== incoming.size) {
          incoming.row.label('Transferencia incompleta: llegaron ' + fmtSize(incoming.received) +
            ' de ' + fmtSize(incoming.size) + ' — archivo descartado');
          incoming.row.set(0, false, true);
        } else {
          // Confirmar al emisor: bytes verificados y descarga iniciada.
          // (El navegador no puede saber si el SO terminó de guardarlo.)
          try { dc.send(JSON.stringify({ t: 'ack', id: m.id })); } catch (e) {}
          finishIncoming(incoming);
        }
        incoming = null;
      } else if (m.t === 'bye') {
        try { pc.close(); } catch (e) {}
        pcs.delete(key);
        authorizedFiles.delete(key);
      }
      return;
    }
    if (!incoming) return;
    incoming.chunks.push(ev.data);
    incoming.received += ev.data.byteLength;
    // El emisor no debería mandar más de lo anunciado: si lo hace, se aborta.
    if (incoming.received > incoming.size || incoming.received > MAX_FILE_SIZE) {
      abort('Transferencia abortada: datos inválidos del emisor');
      return;
    }
    incoming.row.set(incoming.size ? (incoming.received / incoming.size) * 100 : 0, false);
    incoming.meter.push(ev.data.byteLength);
    const nowRx = Date.now();
    if (nowRx - incoming.lastUi > 500) {
      incoming.lastUi = nowRx;
      incoming.row.label('Recibiendo: ' + incoming.name +
        speedSuffix(incoming.meter, incoming.received, incoming.size));
    }
  };

  dc.onerror = () => abort('Error en la transferencia');

  // Si el emisor se desconecta a mitad de camino (sin mandar "done"),
  // marcar la transferencia como interrumpida y liberar la memoria.
  dc.onclose = () => {
    if (incoming) {
      incoming.row.label('Transferencia interrumpida por el emisor');
      incoming.row.set(
        incoming.size ? (incoming.received / incoming.size) * 100 : 0, false, true);
      incoming.chunks = [];
      incoming = null;
    }
    pcs.delete(key);
    authorizedFiles.delete(key);
  };
}

function finishIncoming(inc) {
  inc.row.set(100, true);
  const blob = new Blob(inc.chunks, { type: inc.mime });
  inc.chunks.length = 0; // liberar memoria: el Blob ya tiene su copia
  const rid = 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const url = addReceivedItem(inc.name, inc.size, blob, rid);

  // Descarga automática…
  const a = document.createElement('a');
  a.href = url;
  a.download = inc.name;
  document.body.appendChild(a);
  a.click();
  a.remove();

  // …y copia persistente en este navegador (sobrevive al refresh).
  idbRecv.put({ id: rid, name: inc.name, size: inc.size, mime: inc.mime, blob, date: Date.now() })
    .catch((e) => console.warn('Recibidos persistente:', e));
}

function fmtSize(b) {
  if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB';
  if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
  if (b >= 1024) return Math.round(b / 1024) + ' KB';
  return b + ' B';
}

// En iOS la pantalla se puede bloquear a mitad de una transferencia:
// pedimos que se mantenga encendida mientras la página está visible.
if ('wakeLock' in navigator) {
  const lock = async () => {
    if (document.visibilityState === 'visible') {
      try { await navigator.wakeLock.request('screen'); } catch (e) {}
    }
  };
  document.addEventListener('visibilitychange', lock);
  lock();
}

connect();
})();

/* Pruebas de seguridad de Pásalo (v1.0.5).
 *
 * Uso:
 *   npm ci --omit=dev
 *   node tests/test-server.js
 *
 * El archivo levanta el servidor solo en un puerto de prueba,
 * ejecuta la suite y lo apaga. No toca tu instalación normal.
 */
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');
const http = require('http');
const WebSocket = require('../node_modules/ws');

const ROOT = path.join(__dirname, '..');
const PORT = 3137;
const ORIGIN = 'http://127.0.0.1:' + PORT;
const URL = 'ws://127.0.0.1:' + PORT;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let pass = 0, fail = 0;
const ok = (name, cond) => {
  cond ? pass++ : fail++;
  console.log((cond ? 'PASS' : 'FAIL') + ' | ' + name);
};

const wsOpen = (ws) => new Promise((res, rej) => {
  ws.on('open', res);
  ws.on('error', rej);
  setTimeout(() => rej(new Error('no open')), 5000);
});
const collect = (ws) => {
  const m = [];
  ws.on('message', (d) => { try { m.push(JSON.parse(d)); } catch {} });
  return m;
};
const httpGet = (path, headers) => httpGetPort(PORT, path, headers);
const httpGetPort = (port, path, headers) => new Promise((res, rej) => {
  const req = http.get({ host: '127.0.0.1', port, path, headers }, (r) => {
    let b = '';
    r.on('data', (c) => { b += c; });
    r.on('end', () => res({ status: r.statusCode, body: b }));
  });
  req.on('error', rej);
  req.setTimeout(4000, () => req.destroy(new Error('timeout')));
});
const waitFor = async (fn, what) => {
  for (let i = 0; i < 50; i++) {
    try { if (await fn()) return; } catch {}
    await sleep(200);
  }
  throw new Error('no listo: ' + what);
};

async function runTests() {
  const W = (headers) => new WebSocket(URL, { headers: Object.assign({ Origin: ORIGIN }, headers) });

  // 1. Sin vincular: no recibe roster
  const A = W(); const mA = collect(A); await wsOpen(A);
  await sleep(400);
  ok('sin vincular no recibe roster', !mA.some((m) => m.type === 'roster'));

  // 2. Código incorrecto -> pair_error
  A.send(JSON.stringify({ type: 'pair', code: '000000' }));
  await sleep(300);
  ok('código malo -> pair_error', mA.some((m) => m.type === 'pair_error'));
  A.close();

  // 3. Vinculación correcta
  const D = W(); const mD = collect(D); await wsOpen(D);
  D.send(JSON.stringify({ type: 'pair', code: '123456' }));
  await sleep(400);
  const pairedD = mD.find((m) => m.type === 'paired');
  ok('código bueno -> paired + token + deviceId',
    !!pairedD && typeof pairedD.token === 'string' && typeof pairedD.deviceId === 'string');
  const tokenD = pairedD.token, didD = pairedD.deviceId;
  D.send(JSON.stringify({ type: 'rename', name: 'iPhone' }));
  await sleep(300);

  // 4. El código rotó: segundo equipo usa el nuevo
  const code2 = global.__lastCode;
  ok('el código rotó tras vincular', code2 && code2 !== '123456');
  const E = W(); const mE = collect(E); await wsOpen(E);
  E.send(JSON.stringify({ type: 'pair', code: code2 }));
  await sleep(400);
  ok('E se vincula con el código nuevo', mE.some((m) => m.type === 'paired'));
  E.send(JSON.stringify({ type: 'rename', name: 'PC' }));
  await sleep(400);
  const rosterE = mE.filter((m) => m.type === 'roster').pop();
  ok('roster con 2 + deviceIds',
    rosterE && rosterE.clients.length === 2 &&
    rosterE.clients.every((c) => typeof c.deviceId === 'string'));

  // 5. El servidor sella la identidad: nombre falso del emisor no pasa
  const idD = mD.find((m) => m.type === 'welcome').id;
  const idE = rosterE.clients.find((c) => c.id !== idD).id;
  const sigP = new Promise((res) => {
    const h = (d) => {
      const m = JSON.parse(d);
      if (m.type === 'signal') { E.off('message', h); res(m); }
    };
    E.on('message', h);
  });
  D.send(JSON.stringify({
    type: 'signal', to: idE,
    payload: { kind: 'offer', session: 'sx', sdp: {}, name: 'iPhone FALSO', deviceId: 'ffff' },
  }));
  const sig = await sigP;
  ok('identidad sellada por el servidor',
    sig.payload.name === 'iPhone' && sig.payload.deviceId === didD);

  // 6. No vinculado no puede señalizar
  const A2 = W(); collect(A2); await wsOpen(A2);
  let leaked = false;
  const h2 = (d) => { if (JSON.parse(d).type === 'signal') leaked = true; };
  E.on('message', h2);
  A2.send(JSON.stringify({ type: 'signal', to: idE, payload: { kind: 'offer', session: 'evil', sdp: {} } }));
  await sleep(400);
  E.off('message', h2);
  ok('no vinculado no puede señalizar', !leaked);
  A2.close();

  // 7. Origin inválido / ausente -> rechazado
  const evil = new WebSocket(URL, { headers: { Origin: 'http://evil.com' } });
  const evilCode = await new Promise((res) => {
    evil.on('close', (c) => res(c));
    setTimeout(() => res('no-cerrado'), 3000);
  });
  ok('Origin evil.com rechazado (1008)', evilCode === 1008);
  const noOrigin = new WebSocket(URL);
  const noOc = await new Promise((res) => {
    noOrigin.on('close', (c) => res(c));
    setTimeout(() => res('no-cerrado'), 3000);
  });
  ok('sin Origin rechazado (1008)', noOc === 1008);

  // 8. Host arbitrario bloqueado (anti DNS-rebinding)
  const evilHost = await httpGet('/pairing-code', { Host: 'evil.com' });
  ok('Host evil.com -> 403', evilHost.status === 403);
  const okHost = await httpGet('/pairing-code', { Host: '127.0.0.1:3137' });
  ok('Host 127.0.0.1 -> 200 con código',
    okHost.status === 200 && JSON.parse(okHost.body).code.length === 6);

  // 9. Timeout para no vinculados
  const G = W(); collect(G); await wsOpen(G);
  const gCode = await new Promise((res) => {
    G.on('close', (c) => res(c));
    setTimeout(() => res('no-cerrado'), 6000);
  });
  ok('sin vincular se cierra solo (1008)', gCode === 1008);

  // 10. Token: una conexión a la vez + reconexión
  const D2 = W(); const mD2 = collect(D2); await wsOpen(D2);
  const dClosed = new Promise((res) => { D.on('close', (c) => res(c)); setTimeout(() => res('sigue-abierto'), 3000); });
  D2.send(JSON.stringify({ type: 'hello', token: tokenD, name: 'iPhone' }));
  ok('token en 2 conexiones: la anterior se cierra', (await dClosed) === 1008);
  await sleep(300);
  ok('D2 vinculado con el mismo deviceId',
    mD2.some((m) => m.type === 'paired' && m.deviceId === didD));

  // 11. Desvincular revoca el token
  const pairedE = mE.find((m) => m.type === 'paired');
  E.send(JSON.stringify({ type: 'unpair' }));
  await sleep(400);
  ok('unpair -> auth_required', mE.some((m) => m.type === 'auth_required'));
  const F = W(); const mF = collect(F); await wsOpen(F);
  F.send(JSON.stringify({ type: 'hello', token: pairedE.token }));
  await sleep(300);
  ok('token revocado ya no sirve', mF.some((m) => m.type === 'auth_required'));

  // 12. Malformados no tumban
  const A3 = W(); collect(A3); await wsOpen(A3);
  for (const evilMsg of ['null', '[]', '"x"', '{"type":"pair"}', '{"type":"signal","to":"x","payload":null}']) {
    A3.send(evilMsg);
  }
  await sleep(500);
  ok('servidor vivo tras malformados', A3.readyState === WebSocket.OPEN);

  // 13. ÚLTIMO: fuerza bruta por IP (bloquea 127.0.0.1 — nada más después)
  const brutes = [];
  for (let i = 0; i < 12; i++) {
    const s = W(); collect(s); await wsOpen(s);
    brutes.push(s);
    s.send(JSON.stringify({ type: 'pair', code: '000000' }));
  }
  await sleep(1200);
  ok('fuerza bruta: el 12º es cortado',
    brutes[11].readyState === WebSocket.CLOSED);
  ok('fuerza bruta: el 1º sigue abierto (límite es por IP, no global)',
    brutes[0].readyState === WebSocket.OPEN);
  for (const s of brutes) try { s.close(); } catch {}
  A3.close(); D2.close(); E.close(); F.close();
}

/* ── Servidor 2: pruebas de los fixes de v1.0.5 ──
 * Ventanas cortas (2s / 9s) para probar el bloqueo sin esperar 10 min.
 */
const PORT2 = 3138;
const ORIGIN2 = 'http://127.0.0.1:' + PORT2;
const URL2 = 'ws://127.0.0.1:' + PORT2;

async function runTests2() {
  const W2 = (headers) => new WebSocket(URL2, { headers: Object.assign({ Origin: ORIGIN2 }, headers) });
  const getCode = async () => (await httpGetPort(PORT2, '/pairing-code')).body.trim();
  const pair2 = async (name, deviceId) => {
    const code = await getCode();
    const ws = W2(); const m = collect(ws); await wsOpen(ws);
    ws.send(JSON.stringify({ type: 'pair', code, name, deviceId }));
    await sleep(500);
    const p = m.find((x) => x.type === 'paired');
    return { ws, m, paired: p, token: p && p.token, deviceId: p && p.deviceId };
  };

  // 21. El bloqueo sobrevive a la ventana (el bug que encontró la auditoría)
  const b1 = W2(); const closeB1 = [];
  b1.on('close', (c) => closeB1.push(c));
  await wsOpen(b1);
  for (let i = 0; i < 11; i++) {
    b1.send(JSON.stringify({ type: 'pair', code: '000000', name: 'X', deviceId: 'bx' }));
    await sleep(40);
  }
  await sleep(400);
  ok('bloqueo: el 11º intento es cortado (1008)', closeB1.includes(1008));
  await sleep(2600); // la ventana (2s) ya venció, el bloqueo (9s) sigue
  const b2 = W2(); const closeB2 = [];
  b2.on('close', (c) => closeB2.push(c));
  await wsOpen(b2);
  b2.send(JSON.stringify({ type: 'pair', code: '000000', name: 'X', deviceId: 'bx' }));
  await sleep(500);
  ok('bloqueo: tras vencer la ventana SIGUE bloqueado', closeB2.includes(1008));
  await sleep(7000); // ~10.5s desde el bloqueo > 9s de castigo
  const b3 = W2(); const m3 = collect(b3);
  await wsOpen(b3);
  b3.send(JSON.stringify({ type: 'pair', code: '000000', name: 'X', deviceId: 'bx' }));
  await sleep(500);
  ok('bloqueo: vencido el castigo responde pair_error (no cortado)',
    m3.some((m) => m.type === 'pair_error') && b3.readyState === WebSocket.OPEN);
  b3.close();

  // 22. Origin estricto: IP válida pero distinta al Host real -> fuera
  const evil2 = new WebSocket(URL2, { headers: { Origin: 'http://192.168.1.99:' + PORT2 } });
  const evil2Code = await new Promise((res) => evil2.on('close', (c) => res(c)));
  ok('Origin de otra IP (mismo puerto) rechazado (1008)', evil2Code === 1008);

  // 23. Tope de conexiones por IP (5): la 6ª se corta
  const A = await pair2('PC', 'dev-a');
  ok('A vinculado en servidor 2', !!A.paired);
  const extra = [];
  for (let i = 0; i < 5; i++) { const s = W2(); await wsOpen(s); extra.push(s); }
  await sleep(400);
  const closed1013 = extra.filter((s) => s.readyState === WebSocket.CLOSED).length;
  ok('la 6ª conexión concurrente se corta (1013)', closed1013 === 1);
  for (const s of extra) try { s.close(); } catch {};
  await sleep(400);

  // 24. Revocar el acceso de OTRO equipo (iPhone perdido)
  const B = await pair2('iPhone', 'dev-b');
  ok('B vinculado en servidor 2', !!B.paired);
  const closeB = [];
  B.ws.on('close', (c) => closeB.push(c));
  A.ws.send(JSON.stringify({ type: 'revoke', deviceId: B.deviceId }));
  await sleep(500);
  ok('revocar cierra el socket del otro equipo', closeB.length > 0);
  const Br = W2(); const mBr = collect(Br); await wsOpen(Br);
  Br.send(JSON.stringify({ type: 'hello', token: B.token }));
  await sleep(400);
  ok('token revocado -> auth_required', mBr.some((m) => m.type === 'auth_required'));
  Br.close();
  A.ws.send(JSON.stringify({ type: 'revoke', deviceId: A.deviceId }));
  await sleep(400);
  ok('auto-revocación se ignora (sigues vinculado)', A.ws.readyState === WebSocket.OPEN);

  // 25. La oferta anuncia nombre+tamaño de los archivos, con identidad sellada
  const C = await pair2('iPhone2', 'dev-c');
  ok('C vinculado en servidor 2', !!C.paired);
  const rosterA = A.m.filter((m) => m.type === 'roster').pop();
  const idC = rosterA.clients.find((c) => c.deviceId === C.deviceId).id;
  const sigP = new Promise((res) => {
    const h = (d) => {
      const m = JSON.parse(d);
      if (m.type === 'signal') { C.ws.off('message', h); res(m); }
    };
    C.ws.on('message', h);
  });
  A.ws.send(JSON.stringify({ type: 'signal', to: idC,
    payload: { kind: 'offer', session: 's9', name: 'FALSO', deviceId: 'ffff',
      sdp: { type: 'offer', sdp: 'x' },
      files: [{ name: 'foto.jpg', size: 12345 }, { name: 'video.mp4', size: 987654321 }] } }));
  const sig = await sigP;
  ok('la oferta trae la lista de archivos con nombre y tamaño',
    Array.isArray(sig.payload.files) && sig.payload.files.length === 2 &&
    sig.payload.files[0].name === 'foto.jpg' && sig.payload.files[0].size === 12345 &&
    sig.payload.files[1].name === 'video.mp4' && sig.payload.files[1].size === 987654321);
  ok('la identidad sigue sellada aunque la oferta traiga files',
    sig.payload.name === 'PC' && sig.payload.deviceId === A.deviceId);

  // 26. Admin: la PC (localhost) sí es admin
  const A2 = await pair2('PC-admin', 'dev-a2');
  ok('localhost es admin (paired.admin true)', !!A2.paired && A2.paired.admin === true);

  // 27. Panel de vinculados: incluye desconectados y se puede revocar
  const getLinked = (ws) => new Promise((res) => {
    const h = (d) => {
      const x = JSON.parse(d);
      if (x.type === 'linked') { ws.off('message', h); res(x); }
    };
    ws.on('message', h);
    ws.send(JSON.stringify({ type: 'linked' }));
  });
  const B2 = await pair2('iPhone-perdido', 'dev-b2');
  let lk = await getLinked(A2.ws);
  const b2in = lk.devices.find((d) => d.deviceId === B2.deviceId);
  ok('linked lista al vinculado conectado', !!b2in && b2in.connected === true);
  B2.ws.close(); // se "pierde": apagado, fuera de casa
  await sleep(600);
  lk = await getLinked(A2.ws);
  const b2dev = lk.devices.find((d) => d.deviceId === B2.deviceId);
  ok('linked muestra al desconectado (connected:false)', !!b2dev && b2dev.connected === false);
  A2.ws.send(JSON.stringify({ type: 'revoke', deviceId: B2.deviceId }));
  await sleep(400);
  const Br2 = W2(); const mBr2 = collect(Br2); await wsOpen(Br2);
  Br2.send(JSON.stringify({ type: 'hello', token: B2.token }));
  await sleep(400);
  ok('revocar desconectado invalida su token', mBr2.some((m) => m.type === 'auth_required'));
  Br2.close();

  // 28. Origin: el puerto se compara con el Host real (Docker 8002:3000)
  const dock = new WebSocket(URL2, { headers: { Origin: 'http://127.0.0.1:9999', Host: '127.0.0.1:9999' } });
  await wsOpen(dock);
  ok('Docker: Origin y Host con puerto externo coinciden -> pasa', dock.readyState === WebSocket.OPEN);
  dock.close();
  const dockBad = new WebSocket(URL2, { headers: { Origin: 'http://127.0.0.1:' + PORT2, Host: '127.0.0.1:9999' } });
  const dockBadCode = await new Promise((res) => dockBad.on('close', (c) => res(c)));
  ok('Docker: Origin con puerto distinto al Host -> rechazado (1008)', dockBadCode === 1008);

  A2.ws.close();
  A.ws.close(); C.ws.close();
}

async function main() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT),
      PASALO_FIXED_CODE: '123456',
      PAIR_TIMEOUT_MS: '2500',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens.json'),
    }),
  });
  let out = '';
  child.stdout.on('data', (d) => { out += d.toString(); });
  child.stderr.on('data', (d) => { out += d.toString(); });
  // exponer el último código para la prueba de rotación
  const watcher = setInterval(() => {
    const all = [...out.matchAll(/CÓDIGO DE VINCULACIÓN: (\d{3}) (\d{3})/g)];
    if (all.length) {
      const last = all[all.length - 1];
      global.__lastCode = last[1] + last[2];
    }
  }, 200);
  try {
    await waitFor(async () => (await httpGet('/healthz')).status === 200, 'healthz');
    await runTests();
  } finally {
    clearInterval(watcher);
    child.kill();
  }
}

async function main2() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT2),
      PASALO_FIXED_CODE: '123456',
      PAIR_WINDOW_MS: '2000',
      PAIR_BLOCK_MS: '9000',
      PAIR_TIMEOUT_MS: '2500',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens2.json'),
    }),
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  try {
    await waitFor(async () => {
      try { return (await httpGetPort(PORT2, '/healthz')).status === 200; }
      catch { return false; }
    }, 'healthz2');
    await runTests2();
  } finally {
    child.kill();
  }
}

/* ── Servidor 3: sin admin (ADMIN_IPS no incluye localhost) ── */
const PORT3 = 3140;
const ORIGIN3 = 'http://127.0.0.1:' + PORT3;
const URL3 = 'ws://127.0.0.1:' + PORT3;

async function runTests3() {
  // 29-33. proto.js: funciones puras, sin servidor
  const proto = require(path.join(ROOT, 'public', 'proto.js'));
  ok('proto: coincide exacto -> ok',
    proto.verifyFileMeta([{ name: 'foto.jpg', size: 123 }], { name: 'foto.jpg', size: 123 }, 0).ok === true);
  ok('proto: emisor cambia el nombre -> bloqueado',
    proto.verifyFileMeta([{ name: 'foto.jpg', size: 123 }], { name: 'programa.exe', size: 123 }, 0).ok === false);
  ok('proto: emisor cambia el tamaño -> bloqueado',
    proto.verifyFileMeta([{ name: 'foto.jpg', size: 123 }], { name: 'foto.jpg', size: 999 }, 0).ok === false);
  ok('proto: emisor manda archivo de más -> bloqueado',
    proto.verifyFileMeta([{ name: 'foto.jpg', size: 123 }], { name: 'otro.jpg', size: 1 }, 1).ok === false);
  ok('proto: sanitizeFileList filtra entradas inválidas',
    JSON.stringify(proto.sanitizeFileList([{ name: 'a', size: 1 }, null, { name: 'b', size: -2 }])) ===
    JSON.stringify([{ name: 'a', size: 1 }]));

  // 34-38. proto.js: buffer de candidatos ICE (fix v1.0.9)
  {
    const b = proto.createIceBuffer(3);
    b.add('k1', { c: 1 });
    b.add('k1', { c: 2 });
    b.add('k2', { c: 3 });
    const got = b.take('k1');
    ok('iceBuffer: guarda y entrega en orden FIFO',
      got.length === 2 && got[0].c === 1 && got[1].c === 2);
    ok('iceBuffer: take vacía la entrada',
      b.take('k1').length === 0 && b.size() === 1);
    b.add('k3', { c: 1 }); b.add('k3', { c: 2 }); b.add('k3', { c: 3 }); b.add('k3', { c: 4 });
    ok('iceBuffer: respeta el tope por clave', b.take('k3').length === 3);
    b.drop('k2');
    ok('iceBuffer: drop elimina la entrada', b.size() === 0);
    const bb = proto.createIceBuffer();
    for (let i = 0; i < 60; i++) bb.add('s' + i, { c: i });
    ok('iceBuffer: purga entradas viejas si hay demasiadas claves', bb.size() <= 50);
  }

  // 39-41. Sin admin: el revoke se ignora
  const W3 = (headers) => new WebSocket(URL3, { headers: Object.assign({ Origin: ORIGIN3 }, headers) });
  const getCode3 = async () => (await httpGetPort(PORT3, '/pairing-code')).body.trim();
  const pair3 = async (name, deviceId) => {
    const code = await getCode3();
    const ws = W3(); const m = collect(ws); await wsOpen(ws);
    ws.send(JSON.stringify({ type: 'pair', code, name, deviceId }));
    await sleep(500);
    const p = m.find((x) => x.type === 'paired');
    return { ws, m, paired: p, token: p && p.token, deviceId: p && p.deviceId };
  };
  const A3 = await pair3('iPhoneX', 'dev-a3');
  ok('no-admin: paired.admin es false', !!A3.paired && A3.paired.admin === false);
  const B3 = await pair3('iPhoneY', 'dev-b3');
  const closeB3 = [];
  B3.ws.on('close', (c) => closeB3.push(c));
  A3.ws.send(JSON.stringify({ type: 'revoke', deviceId: B3.deviceId }));
  await sleep(500);
  ok('no-admin: el revoke se ignora (el otro sigue conectado)',
    closeB3.length === 0 && B3.ws.readyState === WebSocket.OPEN);
  const B3b = W3(); const mB3b = collect(B3b); await wsOpen(B3b);
  B3b.send(JSON.stringify({ type: 'hello', token: B3.token }));
  await sleep(400);
  ok('no-admin: el token del otro sigue válido', mB3b.some((m) => m.type === 'paired'));
  A3.ws.close(); B3.ws.close(); B3b.close();
}

async function main3() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT3),
      PASALO_FIXED_CODE: '123456',
      PASALO_ADMIN_IPS: '10.20.30.40',
      PAIR_TIMEOUT_MS: '2500',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens3.json'),
    }),
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  try {
    await waitFor(async () => {
      try { return (await httpGetPort(PORT3, '/healthz')).status === 200; }
      catch { return false; }
    }, 'healthz3');
    await runTests3();
  } finally {
    child.kill();
  }
}

/* ── Servidor 4: idempotencia del cleanup (7ª auditoría, punto 1) ── */
const PORT4 = 3142;
const ORIGIN4 = 'http://127.0.0.1:' + PORT4;

async function runTests4() {
  const net = require('net');
  const crypto = require('crypto');
  const proto = require(path.join(ROOT, 'public', 'proto.js'));
  const W4 = (headers) => new WebSocket('ws://127.0.0.1:' + PORT4, { headers: Object.assign({ Origin: ORIGIN4 }, headers) });

  // 37-39. createAckTracker: unitario
  {
    const t = proto.createAckTracker(500);
    const p = t.wait('a1');
    t.got('a1');
    ok('ack a tiempo resuelve true', (await p) === true);
  }
  {
    const t = proto.createAckTracker(120);
    ok('sin ack resuelve false al vencer', (await t.wait('a2')) === false);
  }
  {
    const t = proto.createAckTracker(5000);
    const p = t.wait('a3');
    t.clear();
    ok('clear() libera los pendientes en false', (await p) === false && t.pendingCount() === 0);
  }

  // 40. El temporizador ACK arranca DESPUÉS de enviar done (bug de v1.007:
  // arrancaba antes de transferir y un archivo grande lo vencía a mitad).
  {
    const src = fs.readFileSync(path.join(ROOT, 'public', 'app.js'), 'utf8');
    const doneIdx = src.indexOf(`dc.send(JSON.stringify({ t: 'done', id }));`);
    const waitIdx = src.indexOf('const ackP = acks.wait(id);');
    const between = doneIdx !== -1 && waitIdx !== -1 ? src.slice(doneIdx, waitIdx) : 'await';
    ok('el temporizador ACK arranca después de enviar done (sin await entre medio)',
      doneIdx !== -1 && waitIdx !== -1 && doneIdx < waitIdx && !/await/.test(between));
  }

  // 41. Conexión cruda: handshake manual + frame con UTF-8 inválido.
  // El servidor emite 'error' y 'close' para el mismo socket: el cleanup
  // debe descontar UNA sola vez del contador por IP.
  const rawSocket = () => new Promise((res, rej) => {
    const sock = net.connect(PORT4, '127.0.0.1');
    const key = crypto.randomBytes(16).toString('base64');
    sock.once('connect', () => {
      sock.write(
        'GET / HTTP/1.1\r\n' +
        'Host: 127.0.0.1:' + PORT4 + '\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        'Sec-WebSocket-Key: ' + key + '\r\n' +
        'Sec-WebSocket-Version: 13\r\n' +
        'Origin: ' + ORIGIN4 + '\r\n\r\n');
    });
    let buf = Buffer.alloc(0);
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      if (buf.indexOf('\r\n\r\n') !== -1) res(sock);
    });
    sock.on('error', rej);
    setTimeout(() => rej(new Error('sin handshake')), 4000);
  });
  const badTextFrame = () => {
    const payload = Buffer.from([0xff, 0xfe, 0xfd]);
    const mask = crypto.randomBytes(4);
    const out = Buffer.alloc(6 + payload.length);
    out[0] = 0x81;
    out[1] = 0x80 | payload.length;
    mask.copy(out, 2);
    for (let i = 0; i < payload.length; i++) out[6 + i] = payload[i] ^ mask[i % 4];
    return out;
  };

  const A = W4(); await wsOpen(A);          // 1 real
  const raw = await rawSocket();
  raw.write(badTextFrame());               // 'error' + 'close' en el servidor
  await sleep(800);
  const rest = [];
  for (let i = 0; i < 4; i++) { const s = W4(); await wsOpen(s); rest.push(s); } // 5 reales
  await sleep(300);
  const sixth = W4(); const closeSixth = [];
  sixth.on('close', (c) => closeSixth.push(c));
  await wsOpen(sixth).catch(() => {});
  await sleep(600);
  // Sin el guard: el doble cleanup deja el contador en 4 y la 6ª pasa (bug).
  // Con el guard: el contador está en 5 y la 6ª se corta con 1013.
  ok('doble evento no descuenta dos veces: la 6ª conexión se corta (1013)',
    closeSixth.includes(1013));
  for (const s of [A, ...rest]) try { s.close(); } catch {}
  try { sixth.close(); } catch {}
  try { raw.destroy(); } catch {}
}

async function main4() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT4),
      PASALO_FIXED_CODE: '123456',
      PAIR_TIMEOUT_MS: '15000',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens4.json'),
    }),
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  try {
    await waitFor(async () => {
      try { return (await httpGetPort(PORT4, '/healthz')).status === 200; }
      catch { return false; }
    }, 'healthz4');
    await runTests4();
  } finally {
    child.kill();
  }
}

main()
  .then(() => main2())
  .then(() => main3())
  .then(() => main4())
  .then(() => main5())
  .then(() => runTests6())
  .then(() => runTests7())
  .then(() => main6())
  .then(() => {
    console.log('\n' + pass + ' passed, ' + fail + ' failed');
    process.exit(fail ? 1 : 0);
  })
  .catch((e) => { console.error('FAIL:', e.message); process.exit(1); });

const PORT5 = 3144;
const ORIGIN5 = 'http://127.0.0.1:' + PORT5;

async function runTests5() {
  const proto = require(path.join(ROOT, 'public', 'proto.js'));
  const W5 = (headers) => new WebSocket('ws://127.0.0.1:' + PORT5, { headers: Object.assign({ Origin: ORIGIN5 }, headers) });

  // 42-43. proto.js: base64 del modo puente
  {
    const a = new Uint8Array(100000);
    for (let i = 0; i < a.length; i++) a[i] = (i * 7) % 256;
    const d = proto.b64decodeToBytes(proto.b64encodeBytes(a));
    ok('puente: base64 roundtrip 100KB', d.length === a.length && d.every((v, i) => v === a[i]));
    ok('puente: base64 vacío', proto.b64decodeToBytes(proto.b64encodeBytes(new Uint8Array(0))).length === 0);
  }

  // 44-48. Servidor: el puente no dispara el límite de señalización,
  // los mensajes gigantes se descartan y la identidad va sellada.
  const getCode5 = async () => (await httpGetPort(PORT5, '/pairing-code')).body.trim();
  const pair5 = async (name) => {
    const code = await getCode5();
    const ws = W5(); const m = collect(ws); await wsOpen(ws);
    ws.send(JSON.stringify({ type: 'pair', code, name }));
    await waitFor(() => m.find((x) => x.type === 'paired'), 'paired5 ' + name);
    const welcome = m.find((x) => x.type === 'welcome');
    return { ws, m, id: welcome && welcome.id };
  };
  const A = await pair5('A');
  const B = await pair5('B');
  const closedA = [];
  A.ws.on('close', (c) => closedA.push(c));

  // 150 trozos seguidos: sin el bypass, el mensaje 121 cerraría la conexión.
  const chunkMsg = JSON.stringify({ type: 'signal', to: B.id, payload: { kind: 'relay-chunk', session: 'r1', id: 'f1', data: 'QUJD' } });
  for (let i = 0; i < 150; i++) A.ws.send(chunkMsg);
  await waitFor(() => B.m.filter((x) => x.type === 'signal' && x.payload && x.payload.kind === 'relay-chunk').length >= 150, 'relay-chunks');
  ok('puente: 150 trozos no disparan el límite de mensajes', closedA.length === 0 && A.ws.readyState === WebSocket.OPEN);
  ok('puente: los trozos llegan al otro equipo vinculado',
    B.m.filter((x) => x.type === 'signal' && x.payload && x.payload.kind === 'relay-chunk').length === 150);

  A.ws.send(JSON.stringify({ type: 'signal', to: B.id, payload: { kind: 'relay-offer', session: 'r1', files: [{ name: 'v.mp4', size: 10 }] } }));
  await waitFor(() => B.m.find((x) => x.type === 'signal' && x.payload && x.payload.kind === 'relay-offer'), 'relay-offer');
  const offer = B.m.find((x) => x.type === 'signal' && x.payload && x.payload.kind === 'relay-offer');
  ok('puente: la oferta llega con identidad sellada por el servidor',
    offer.from === A.id && offer.payload.name === 'A' &&
    typeof offer.payload.deviceId === 'string' && offer.payload.deviceId.length > 0);

  const before = B.m.length;
  const big = 'x'.repeat(600 * 1024);
  A.ws.send(JSON.stringify({ type: 'signal', to: B.id, payload: { kind: 'relay-chunk', session: 'r1', id: 'f1', data: big } }));
  await sleep(800);
  ok('puente: mensaje gigante (>512KB) se descarta', B.m.length === before);

  // 49-50. Cableado del cliente (estático)
  {
    const src = fs.readFileSync(path.join(ROOT, 'public', 'app.js'), 'utf8');
    ok('puente: el timeout ICE cae al puente automáticamente',
      src.includes('function sendViaRelay') && src.includes('sendViaRelay(meta.peerId, meta.files, meta.row)'));
    ok('puente: atajo al puente si el directo ya falló en esta red',
      src.includes("localStorage.getItem('pasaloNoP2P')") && src.includes('function streamRelay'));
  }
  {
    const srv = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
    ok('puente: el servidor exime el puente del límite con tope de tamaño',
      srv.includes('RELAY_MAX_MSG_BYTES'));
  }

  for (const s of [A, B]) try { s.ws.close(); } catch {}
}

async function main5() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT5),
      PASALO_FIXED_CODE: '123456',
      PAIR_TIMEOUT_MS: '15000',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens5.json'),
    }),
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  try {
    await waitFor(async () => {
      try { return (await httpGetPort(PORT5, '/healthz')).status === 200; }
      catch { return false; }
    }, 'healthz5');
    await runTests5();
  } finally {
    child.kill();
  }
}

async function runTests6() {
  // 51-52. Anti-caché: los assets llevan versión en la URL y el pie la muestra.
  const html = fs.readFileSync(path.join(ROOT, 'public', 'index.html'), 'utf8');
  ok('caché: app.js y proto.js llevan ?v= en index.html',
    html.includes('app.js?v=') && html.includes('proto.js?v=') && html.includes('style.css?v='));
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
  ok('caché: la versión visible del pie coincide con package.json',
    html.includes('v' + pkg.version) && typeof pkg.version === 'string');
}

async function runTests7() {
  // 53. Regresión v1.0.12: el servidor sella cada señal con
  // Object.assign({}, payload, {name: deviceName, deviceId}), así que el
  // nombre del archivo NO puede viajar en "name" (lo pisaría).
  const src = fs.readFileSync(path.join(ROOT, 'public', 'app.js'), 'utf8');
  ok('puente: relay-meta manda el archivo en fileName, no en name',
    src.includes('fileName: file.name'));
  ok('puente: el receptor lee el archivo de p.fileName',
    src.includes("String(p.fileName || 'archivo')"));
  // Simulación del sellado: el fileName sobrevive, "name" no.
  {
    const payload = { kind: 'relay-meta', session: 'r1', id: 'f1', fileName: 'video.mp4', size: 10 };
    const stamped = Object.assign({}, payload, { name: 'PC Windows', deviceId: 'abc123' });
    ok('puente: tras el sellado, fileName intacto y name es el equipo',
      stamped.fileName === 'video.mp4' && stamped.name === 'PC Windows');
  }
}

const PORT6 = 3146;
const ORIGIN6 = 'http://127.0.0.1:' + PORT6;

async function runTests8() {
  const proto = require(path.join(ROOT, 'public', 'proto.js'));

  // 54-58. proto.js: velocidad y ETA
  ok('v2: fmtSpeed formatea B/s, KB/s y MB/s',
    proto.fmtSpeed(500) === '500 B/s' &&
    proto.fmtSpeed(2048) === '2,0 KB/s' &&
    proto.fmtSpeed(5 * 1024 * 1024) === '5,0 MB/s');
  ok('v2: fmtSpeed con valores inválidos devuelve vacío',
    proto.fmtSpeed(0) === '' && proto.fmtSpeed(-1) === '' && proto.fmtSpeed(NaN) === '');
  ok('v2: fmtETA formatea segundos y minutos',
    proto.fmtETA(45) === '~45 s' && proto.fmtETA(125) === '~2:05 min');
  {
    const m = proto.createSpeedometer(60000);
    m.push(1000);
    await new Promise((r) => setTimeout(r, 120));
    m.push(1000);
    const sp = m.speed();
    ok('v2: speedometer mide bytes/segundo en ventana móvil', sp > 1000 && sp < 100000);
  }
  ok('v2: speedometer sin muestras da 0', proto.createSpeedometer().speed() === 0);

  // 59. Servidor: /lan-urls responde la lista de URLs
  {
    const r = await httpGetPort(PORT6, '/lan-urls');
    let d = null;
    try { d = JSON.parse(r.body); } catch {}
    ok('v2: /lan-urls responde JSON con urls',
      r.status === 200 && d && Array.isArray(d.urls) &&
      d.urls.every((u) => typeof u === 'string' && u.startsWith('http://')));
  }

  // 60-62. Cableado del cliente (estático)
  {
    const src = fs.readFileSync(path.join(ROOT, 'public', 'app.js'), 'utf8');
    ok('v2: animación de transferencia (cajita) cableada',
      src.includes("row.anim('st-sending')") && src.includes("row.anim('st-confirming')") &&
      src.includes(".dir('recv')"));
    ok('v2: Recibidos persistentes (IndexedDB) cableados',
      src.includes('idbRecv') && src.includes('loadPersistedReceived') &&
      src.includes('addReceivedItem'));
    ok('v2: QR cableado (fetch lan-urls + qrcode)',
      src.includes("fetch('lan-urls')") && src.includes('qrcode(0,'));
  }
  {
    const html = fs.readFileSync(path.join(ROOT, 'public', 'index.html'), 'utf8');
    const app = fs.readFileSync(path.join(ROOT, 'public', 'app.js'), 'utf8');
    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
    ok('v2: index.html carga qrcode.min.js y muestra la versión del package.json',
      html.includes('qrcode.min.js?v=') && html.includes('v' + pkg.version));
    ok('v2.0.1: QR con selector de dirección (LAN + Wi-Fi)',
      app.includes("className = 'qr-url'") && app.includes('draw(u)') &&
      html.includes('Si un código no abre, toca la otra dirección'));
  }
  {
    const css = fs.readFileSync(path.join(ROOT, 'public', 'style.css'), 'utf8');
    ok('v2: CSS con animación de la cajita y respeto a reduced-motion',
      css.includes('@keyframes pack-travel') && css.includes('prefers-reduced-motion'));
  }
}

async function main6() {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: Object.assign({}, process.env, {
      PORT: String(PORT6),
      PASALO_FIXED_CODE: '123456',
      PAIR_TIMEOUT_MS: '15000',
      PASALO_TOKENS_FILE: path.join(os.tmpdir(), 'pasalo-test-tokens6.json'),
    }),
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  try {
    await waitFor(async () => {
      try { return (await httpGetPort(PORT6, '/healthz')).status === 200; }
      catch { return false; }
    }, 'healthz6');
    await runTests8();
  } finally {
    child.kill();
  }
}

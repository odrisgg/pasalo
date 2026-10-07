/* Pásalo — funciones puras del protocolo (sin DOM ni red).
 *
 * Se cargan en el navegador antes de app.js y se prueban en Node con:
 *   node -e "const p = require('./public/proto.js'); ..."
 *
 * Nada aquí toca la red ni el DOM: solo valida datos.
 */

// Un archivo anunciado: nombre (texto, máx 200) + tamaño (bytes, número).
function sanitizeFileEntry(f) {
  if (!f || typeof f !== 'object') return null;
  if (typeof f.name !== 'string' || typeof f.size !== 'number') return null;
  if (!Number.isFinite(f.size) || f.size < 0) return null;
  const name = f.name.slice(0, 200);
  if (!name) return null;
  return { name, size: f.size };
}

// Lista anunciada en la oferta: arreglo de hasta 50 entradas válidas.
function sanitizeFileList(arr) {
  if (!Array.isArray(arr)) return [];
  const out = [];
  for (const f of arr) {
    const e = sanitizeFileEntry(f);
    if (e) out.push(e);
    if (out.length >= 50) break;
  }
  return out;
}

// Verifica que el archivo que el emisor dice que va a mandar (meta) sea
// EXACTAMENTE el que el receptor autorizó al aceptar la oferta.
// authorized: lista saneada guardada al aceptar. index: posición que toca.
function verifyFileMeta(authorized, meta, index) {
  if (!Array.isArray(authorized)) return { ok: false, reason: 'sin lista autorizada' };
  const exp = authorized[index];
  if (!exp) return { ok: false, reason: 'archivo no autorizado (de más)' };
  if (!meta || typeof meta.name !== 'string' || typeof meta.size !== 'number') {
    return { ok: false, reason: 'metadatos inválidos' };
  }
  if (meta.name !== exp.name) return { ok: false, reason: 'nombre distinto al autorizado' };
  if (meta.size !== exp.size) return { ok: false, reason: 'tamaño distinto al autorizado' };
  return { ok: true };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { sanitizeFileEntry, sanitizeFileList, verifyFileMeta, createAckTracker, createIceBuffer, b64encodeBytes, b64decodeToBytes, fmtSpeed, fmtETA, createSpeedometer };
}

/* Confirmación de entrega (ACK) del receptor.
 *
 * El emisor muestra "Listo" solo cuando el receptor confirma que verificó
 * todos los bytes. Por cada archivo: wait(id) al mandar "done",
 * got(id) al recibir {t:'ack', id}. Si vence el tiempo, resuelve false
 * (enviado sin confirmación: los bytes salieron, pero nadie lo confirmó).
 */
function createAckTracker(timeoutMs) {
  const pending = new Map();
  const settle = (id, v) => {
    const e = pending.get(id);
    if (!e) return;
    clearTimeout(e.timer);
    pending.delete(id);
    e.resolve(v);
  };
  return {
    wait(id) {
      return new Promise((resolve) => {
        const timer = setTimeout(() => settle(id, false), timeoutMs);
        pending.set(id, { resolve, timer });
      });
    },
    got(id) { settle(id, true); },
    clear() { for (const id of [...pending.keys()]) settle(id, false); },
    pendingCount() { return pending.size; },
  };
}

/* Buffer de candidatos ICE.
 *
 * Los candidatos pueden llegar antes de que exista la conexión (el receptor
 * aún no tocó "Aceptar") o antes de la descripción remota. Sin este buffer
 * se perderían y la conexión directa nunca se establecería.
 */
function createIceBuffer(maxPerKey) {
  const max = maxPerKey || 100;
  const map = new Map();
  const sweep = () => {
    while (map.size > 50) map.delete(map.keys().next().value);
  };
  return {
    add(key, candidate) {
      if (!map.has(key)) { map.set(key, []); sweep(); }
      const arr = map.get(key);
      if (arr.length < max) arr.push(candidate);
    },
    // Saca y devuelve los guardados (en orden), vaciando la entrada.
    take(key) {
      const arr = map.get(key) || [];
      map.delete(key);
      return arr;
    },
    drop(key) { map.delete(key); },
    size() { return map.size; },
  };
}

/* Base64 para el modo puente (los trozos viajan como texto en el WebSocket).
 * La codificación se hace por bloques para no reventar la pila con
 * String.fromCharCode.apply en archivos grandes.
 */
function b64encodeBytes(u8) {
  let s = '';
  for (let i = 0; i < u8.length; i += 0x8000) {
    s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000));
  }
  return btoa(s);
}
function b64decodeToBytes(b64) {
  const s = atob(b64);
  const u8 = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) u8[i] = s.charCodeAt(i);
  return u8;
}

/* Velocidad y tiempo estimado (para mostrar mientras se transfiere). */
function fmtSpeed(bytesPerSec) {
  if (!isFinite(bytesPerSec) || bytesPerSec <= 0) return '';
  if (bytesPerSec < 1024) return Math.round(bytesPerSec) + ' B/s';
  if (bytesPerSec < 1024 * 1024) return (bytesPerSec / 1024).toFixed(1).replace('.', ',') + ' KB/s';
  return (bytesPerSec / 1024 / 1024).toFixed(1).replace('.', ',') + ' MB/s';
}
function fmtETA(seconds) {
  if (!isFinite(seconds) || seconds < 0) return '';
  seconds = Math.round(seconds);
  if (seconds < 1) return 'menos de 1 s';
  if (seconds < 60) return '~' + seconds + ' s';
  const m = Math.floor(seconds / 60);
  const s = String(seconds % 60).padStart(2, '0');
  return '~' + m + ':' + s + ' min';
}
// Ventana móvil de ~3 s para una velocidad estable.
function createSpeedometer(windowMs) {
  const win = windowMs || 3000;
  let samples = [];
  return {
    push(bytes) {
      const now = Date.now();
      samples.push({ t: now, bytes });
      const cut = now - win;
      while (samples.length && samples[0].t < cut) samples.shift();
    },
    speed() {
      if (samples.length < 2) return 0;
      const dt = (samples[samples.length - 1].t - samples[0].t) / 1000;
      if (dt <= 0) return 0;
      return samples.reduce((a, s) => a + s.bytes, 0) / dt;
    },
  };
}

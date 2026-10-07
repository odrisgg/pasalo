# Pásalo

> Send files between your PC and your phone over your own Wi-Fi. No cloud, no accounts, no sketchy apps. Up to 1 GB per file.

**¿Hablas español?** [Lee la versión en español](#-español).

![MIT License](https://img.shields.io/badge/license-MIT-green) ![Node](https://img.shields.io/badge/node-%3E%3D18-green) [![Tests](https://github.com/odrisgg/pasalo/actions/workflows/ci.yml/badge.svg)](https://github.com/odrisgg/pasalo/actions/workflows/ci.yml)

## What is it?

Pásalo is a tiny local-first file transfer app. Open it on your PC, scan a QR code with your phone (same Wi-Fi), and send files in either direction — straight from one device to the other.

- **Direct transfer** with WebRTC when your network allows it.
- **Bridge mode**: if the direct connection fails (firewalls, antivirus, tricky Wi-Fi), files travel through your own PC over WebSocket — automatically, nothing to configure. The server only relays chunks; it never stores them.
- **QR pairing** + 6-digit single-use code. No accounts, no cloud, no third-party services.
- **Live speed & ETA**, animated transfer, persistent inbox (survives page refresh).
- **Up to 1 GB per file**, no compression, no watermarks. The file arrives identical.

## Quick start (Windows)

1. Download the latest `.zip` from [Releases](../../releases) and extract it (e.g. `C:\pasalo`).
2. Double-click **`iniciar-pasalo.bat`** — it starts the server and opens the page in your browser.
3. Scan the QR with your phone's camera (same Wi-Fi), enter the pairing code, done.

Requirements: [Node.js](https://nodejs.org) 18+ (LTS installer).

## How it works

```
Phone ──WebRTC P2P (direct)──▶ PC
   ╲──WebSocket bridge (fallback)──▶ PC (relays, never stores)
```

- `server.js` is a small Node.js signaling + static server. It only runs on your LAN — never expose port 3000 to the internet.
- Devices pair with a rotating 6-digit code; tokens persist in `tokens.json`.
- Transfers use chunked WebRTC data channels with ACKs and resume-friendly protocol (`public/proto.js`).
- If P2P fails, chunks are relayed through your own PC via WebSocket (`modo puente`).

## Security model (honest version)

- **LAN-only by design.** Everything stays inside your network. Do not forward port 3000 on your router.
- **Pairing codes** are single-use and rotate; auth tokens never travel in the URL.
- **The server never stores your files** — in bridge mode it forwards chunks and forgets them.
- 8 AI-assisted security review rounds, 83 automated tests: `node tests/test-server.js`.
- Plain HTTP on purpose: this is a trusted-home-network tool. HTTPS is on the roadmap for when a domain is available.

## Project structure

```
server.js            Node.js signaling + static server (LAN only)
iniciar-pasalo.bat   Windows launcher (finds Node, opens the browser)
public/
  index.html         the app
  app.js             client logic (WebRTC, bridge, QR, inbox)
  promo.html         landing page (ES/EN, light/dark)
  cajin.png          Cajín, the mascot
tests/
  test-server.js     83 automated tests (security + protocol + wiring)
```

## Run the tests

```sh
npm ci
npm test
```

## Roadmap

- [ ] HTTPS when a domain is available
- [ ] GitHub Pages promo
- [ ] Optional English UI in the app

---

## 🇪🇸 Español

**Pásalo** envía archivos entre tu PC y tu celular por tu propio Wi-Fi. Sin nube, sin cuentas, sin apps raras. Hasta 1 GB por archivo.

1. Descarga el `.zip` del último [Release](../../releases) y extráelo (ej. `C:\pasalo`).
2. Doble clic a **`iniciar-pasalo.bat`** — arranca el servidor y abre la página en tu navegador.
3. Escanea el QR con la cámara del celular (mismo Wi-Fi), pon el código de vinculación y listo.

**Seguridad honesta:** solo funciona en tu red local (no abras el puerto 3000 en el router); el servidor nunca guarda tus archivos; códigos de un solo uso; 8 rondas de revisión de seguridad asistidas por IA y 83 pruebas automatizadas.

## License

MIT — see [LICENSE](LICENSE). Read it, audit it, improve it.

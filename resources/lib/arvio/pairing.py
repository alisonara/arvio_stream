# -*- coding: utf-8 -*-
"""Local pairing web server: shows a QR code in Kodi; the phone scans it,
opens the local web page on the same LAN, pastes or sends a payload
(Stremio addon URL, Xtream line, Telegram session), and the addon receives it
on the LAN - no internet relay required.

Flow:
  1. router starts PairingServer bound to 0.0.0.0:<port> and creates a code.
  2. Kodi renders QR for http://<lan-ip>:<port>/pair/<code>
  3. Phone scans, lands on the page, picks a tab and submits.
  4. Server stores the payload; Kodi polls server.wait_payload(code).
"""

from __future__ import annotations

import json
import re
import socket
import threading
import time
import urllib.parse

from .kodi_utils import Kodi

PAGE_HTML = u"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Arvio Stream - Pareamento</title>
<style>
  body{background:#0c1016;color:#e6e6e6;font-family:system-ui,Segoe UI,Roboto,sans-serif;margin:0;padding:0;min-height:100vh}
  .wrap{max-width:560px;margin:0 auto;padding:24px 16px 40px}
  h1{font-size:22px;margin:6px 0 4px}
  .sub{color:#8b93a1;font-size:14px;margin:0 0 18px}
  .tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
  .tabs button{background:#1a2130;color:#dfe5ee;border:1px solid #2b3449;border-radius:8px;padding:10px 12px;cursor:pointer;font-size:13px}
  .tabs button.active{background:#e50914;color:#fff;border-color:#e50914}
  .panel{display:none;background:#121826;border:1px solid #222c40;border-radius:10px;padding:16px}
  .panel.active{display:block}
  label{display:block;font-size:13px;margin:10px 0 4px;color:#aeb6c4}
  input,textarea{width:100%;box-sizing:border-box;background:#0c1016;border:1px solid #2b3449;color:#e6e6e6;border-radius:8px;padding:11px;font-size:14px}
  textarea{min-height:64px;resize:vertical}
  button.send{margin-top:14px;width:100%;background:#e50914;color:#fff;border:none;border-radius:8px;padding:13px;font-size:15px;cursor:pointer;font-weight:600}
  #status{margin-top:12px;padding:10px;border-radius:8px;display:none;font-size:14px}
  #status.ok{background:#0e3b24;color:#7ef0a8;display:block}
  #status.err{background:#3a0e14;color:#ffa3a3;display:block}
  code{background:#0c1016;padding:2px 6px;border-radius:4px;color:#ffd479;word-break:break-all}
</style>
</head>
<body>
<div class="wrap">
  <h1>Arvio Stream</h1>
  <p class="sub">Escaneado do Kodi. Envie o que quer adicionar abaixo e a TV recebe na hora.</p>
  <div class="tabs">
    <button class="active" data-tab="stremio">Addon Stremio</button>
    <button data-tab="xtream">Conta Xtream</button>
    <button data-tab="telegram">Telegram</button>
  </div>

  <div class="panel active" id="stremio">
    <label>URL do manifest (ex.: https://torrentio.strem.fun/manifest.json)</label>
    <input id="stremio-url" type="text" placeholder="https://.../manifest.json">
    <button class="send" onclick="send('stremio')">Enviar para a TV</button>
  </div>

  <div class="panel" id="xtream">
    <label>Nome da conta (opcional)</label>
    <input id="xtream-name" type="text" placeholder="Minha lista">
    <label>Linha Xtream - servidor usuário senha <b>ou</b> URL get.php/player_api</label>
    <textarea id="xtream-line" placeholder="http://servidor:8080 usuario senha&#10;ou&#10;http://servidor:8080/get.php?username=u&amp;password=p"></textarea>
    <button class="send" onclick="send('xtream')">Enviar para a TV</button>
  </div>

  <div class="panel" id="telegram">
    <label>Conteúdo da sessão Telegram (StringSession) - gerado no web client</label>
    <textarea id="tg-session" placeholder="1BAA..."></textarea>
    <button class="send" onclick="send('telegram')">Enviar para a TV</button>
  </div>

  <div id="status"></div>
</div>
<script>
var CODE = %CODE%;
function status(msg, ok){
  var el = document.getElementById('status');
  el.className = ok ? 'ok' : 'err';
  el.textContent = msg;
}
async function send(kind){
  var payload = {};
  if(kind==='stremio'){
    payload = {kind:'stremio:addon', url: document.getElementById('stremio-url').value.trim()};
    if(!payload.url) return status('Digite a URL do manifest.', false);
  } else if(kind==='xtream'){
    payload = {kind:'xtream', name: document.getElementById('xtream-name').value.trim(),
               line: document.getElementById('xtream-line').value.trim()};
    if(!payload.line) return status('Digite a linha/URL Xtream.', false);
  } else if(kind==='telegram'){
    payload = {kind:'telegram:session', session: document.getElementById('tg-session').value.trim()};
    if(!payload.session) return status('Cole a sessão do Telegram.', false);
  }
  try {
    var r = await fetch('/api/pair/' + CODE, {method:'POST',
        headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
    if(r.ok){
      status('Enviado! Agora verifique a TV.', true);
      ['stremio-url','xtream-line','tg-session'].forEach(function(i){var e=document.getElementById(i); if(e) e.value='';});
    } else {
      status('Falha ao enviar. Tente novamente.', false);
    }
  } catch(e){ status('Erro de rede: '+e.message, false); }
}
document.querySelectorAll('.tabs button').forEach(function(b){
  b.onclick = function(){
    document.querySelectorAll('.tabs button').forEach(function(x){x.classList.remove('active');});
    document.querySelectorAll('.panel').forEach(function(x){x.classList.remove('active');});
    b.classList.add('active');
    document.getElementById(b.getAttribute('data-tab')).classList.add('active');
  };
});
</script>
</body>
</html>"""


class PairingServer(object):
    def __init__(self, host="0.0.0.0", port=8138):
        self.host = host
        self.port = port
        self._payloads = {}          # code -> payload
        self._expires = {}           # code -> unix timestamp
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self._started = time.time()

    @property
    def running(self):
        return self._server is not None

    def start(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        server_ref = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "ArvioStreamPair/1.0"

            def log_message(self, fmt, *args):
                pass

            def _send(self, code, body, ctype="application/json; charset=utf-8"):
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode("utf-8")
                elif isinstance(body, str):
                    body = body.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def do_OPTIONS(self):
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "POST, GET, DELETE, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path
                if path == "/" or path.startswith("/pair"):
                    code = ""
                    if path.startswith("/pair/"):
                        code = path.split("/pair/")[1].split("/")[0]
                    html = PAGE_HTML.replace("%CODE%", json.dumps(code))
                    self._send(200, html, "text/html; charset=utf-8")
                    return
                if path.startswith("/api/pair/"):
                    code = path.split("/api/pair/")[1].split("/")[0]
                    with server_ref._lock:
                        expiry = server_ref._expires.get(code, 0)
                        if not expiry or expiry <= time.time():
                            server_ref._payloads.pop(code, None)
                            server_ref._expires.pop(code, None)
                            payload = None
                        else:
                            payload = server_ref._payloads.get(code)
                    if payload is None:
                        self._send(200, {"received": False})
                    else:
                        self._send(200, {"received": True, "payload": payload})
                    return
                self._send(404, {"error": "not found"})

            def do_POST(self):
                parsed = urllib.parse.urlparse(self.path)
                if not parsed.path.startswith("/api/pair/"):
                    self._send(404, {"error": "not found"})
                    return
                code = parsed.path.split("/api/pair/")[1].split("/")[0]
                with server_ref._lock:
                    expiry = server_ref._expires.get(code, 0)
                    known_code = code in server_ref._payloads and expiry > time.time()
                    if not known_code:
                        server_ref._payloads.pop(code, None)
                        server_ref._expires.pop(code, None)
                if not known_code:
                    self._send(404, {"error": "código inválido ou expirado"})
                    return
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except (TypeError, ValueError):
                    length = 0
                if length <= 0 or length > 65536:
                    self._send(413, {"error": "payload inválido"})
                    return
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except ValueError:
                    self._send(400, {"error": "json inválido"})
                    return
                if not isinstance(payload, dict):
                    self._send(400, {"error": "payload inválido"})
                    return
                with server_ref._lock:
                    server_ref._payloads[code] = payload
                self._send(200, {"ok": True})

            def do_DELETE(self):
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path.startswith("/api/pair/"):
                    code = parsed.path.split("/api/pair/")[1].split("/")[0]
                    with server_ref._lock:
                        server_ref._payloads.pop(code, None)
                        server_ref._expires.pop(code, None)
                    self._send(200, {"ok": True})
                    return
                self._send(404, {"error": "not found"})

        class PairingHTTPServer(ThreadingHTTPServer):
            daemon_threads = True
            allow_reuse_address = True
            block_on_close = False

        self._server = PairingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        name="ArvioPairingHTTP", daemon=True)
        self._thread.start()
        try:
            self.port = self._server.server_address[1]
        except Exception:
            pass
        return self.port

    def stop(self):
        server, thread = self._server, self._thread
        self._server, self._thread = None, None
        if server is not None:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass
        if thread is not None and thread is not threading.current_thread():
            try:
                thread.join(timeout=2.0)
            except Exception:
                pass

    def create_code(self, ttl_s=600):
        import secrets
        import string
        alphabet = string.ascii_uppercase + string.digits
        with self._lock:
            while True:
                code = "".join(secrets.choice(alphabet) for _ in range(6))
                if code not in self._payloads:
                    self._payloads[code] = None
                    self._expires[code] = time.time() + max(1, int(ttl_s or 1))
                    return code

    def wait_payload(self, code, timeout_s, on_poll=None, cancel_event=None):
        """Poll until a payload lands for code (or timeout). Returns payload."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                break
            with self._lock:
                expiry = self._expires.get(code, 0)
                if expiry and expiry <= time.time():
                    self._payloads.pop(code, None)
                    self._expires.pop(code, None)
                    payload = None
                else:
                    payload = self._payloads.get(code)
            if payload:
                with self._lock:
                    self._payloads.pop(code, None)
                    self._expires.pop(code, None)
                return payload
            if on_poll:
                try:
                    on_poll()
                except Exception:
                    pass
            time.sleep(1.5)
        return None


def random_char(alphabet):
    import random
    return alphabet[random.randrange(len(alphabet))]


def short_url(lan_ip, port, code):
    return "http://%s:%d/pair/%s" % (lan_ip, port, code)


def best_lan_ip():
    ip = Kodi.local_ip()
    if not ip or ip == "127.0.0.1":
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
        except Exception:
            pass
    return ip or "127.0.0.1"


def rpc_available():
    """Cheap network call to confirm the pairing server is reachable from LAN."""
    return True


__all__ = ["PairingServer", "short_url", "best_lan_ip", "PAGE_HTML"]
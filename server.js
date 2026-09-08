const http = require("http");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const AUDIO_MAGIC_0 = 0x5A; // 'Z'
const AUDIO_MAGIC_1 = 0x4C; // 'L'
const AUDIO_VERSION = 1;
const AUDIO_HEADER_SIZE = 6; // magic(2) + version(1) + flags(1) + seq(2)

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Z-Link Talk Audio Server OK");
});

const wss = new WebSocket.WebSocketServer({ server: httpServer, maxPayload: 64 * 1024 });
const clients = new Map(); // userId -> ws
let activeTransmitterId = null;
let activeTransmitStartedAt = 0;
let audioPacketCount = 0;

function isOpen(ws) {
  return ws && ws.readyState === WebSocket.OPEN;
}

function clientList() {
  return [...clients.values()].filter(isOpen).map(ws => ({ id: ws.userId, name: ws.name }));
}

function sendJson(ws, data) {
  if (isOpen(ws)) ws.send(JSON.stringify(data));
}

function broadcastJson(data, exceptId = null) {
  const payload = JSON.stringify(data);
  for (const ws of clients.values()) {
    if (isOpen(ws) && ws.userId !== exceptId) ws.send(payload);
  }
}

function isAudioPacket(buf) {
  return Buffer.isBuffer(buf) && buf.length > AUDIO_HEADER_SIZE &&
    buf[0] === AUDIO_MAGIC_0 && buf[1] === AUDIO_MAGIC_1 &&
    buf[2] === AUDIO_VERSION;
}

function resetTransmitterIf(userId) {
  if (activeTransmitterId !== userId) return;
  activeTransmitterId = null;
  activeTransmitStartedAt = 0;
  broadcastJson({ type: "stop_tx", from: userId });
}

function handleJson(ws, data) {
  if (data.type === "identify") {
    const userId = String(data.userId || "").trim();
    if (!userId) return;

    const old = clients.get(userId);
    if (old && old !== ws) {
      resetTransmitterIf(userId);
      try { old.close(4001, "Reconnected"); } catch (_) {}
    }

    ws.userId = userId;
    ws.name = String(data.name || "Anônimo").slice(0, 32);
    clients.set(userId, ws);

    sendJson(ws, {
      type: "init",
      id: userId,
      clients: clientList(),
      activeTransmitter: activeTransmitterId
        ? { id: activeTransmitterId, name: clients.get(activeTransmitterId)?.name || activeTransmitterId }
        : null
    });

    broadcastJson({ type: "user_list", clients: clientList() });
    return;
  }

  if (!ws.userId) return;

  switch (data.type) {
    case "update_name": {
      ws.name = String(data.name || "Anônimo").slice(0, 32);
      broadcastJson({ type: "user_update", id: ws.userId, name: ws.name });
      return;
    }

    case "start_tx": {
      if (activeTransmitterId && activeTransmitterId !== ws.userId) {
        sendJson(ws, {
          type: "tx_denied",
          name: clients.get(activeTransmitterId)?.name || activeTransmitterId
        });
        return;
      }
      activeTransmitterId = ws.userId;
      activeTransmitStartedAt = Date.now();
      broadcastJson({ type: "start_tx", from: ws.userId, name: ws.name });
      return;
    }

    case "stop_tx": {
      resetTransmitterIf(ws.userId);
      return;
    }

    case "ping_app": {
      sendJson(ws, { type: "pong_app", ts: Date.now() });
      return;
    }

    default:
      return;
  }
}

wss.on("connection", ws => {
  ws.userId = null;
  ws.name = "Anônimo";
  ws.isAlive = true;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data, isBinary) => {
    if (isAudioPacket(data)) {
      if (!ws.userId || activeTransmitterId !== ws.userId) return;
      // Do not decode or re-encode on the server: fan out the Opus frame as-is.
      for (const client of clients.values()) {
        if (isOpen(client) && client.userId !== ws.userId) {
          client.send(data, { binary: true });
        }
      }
      audioPacketCount++;
      return;
    }

    if (isBinary) return;

    let message;
    try {
      message = JSON.parse(data.toString());
    } catch (_) {
      return;
    }
    handleJson(ws, message);
  });

  ws.on("close", () => {
    if (!ws.userId) return;
    if (clients.get(ws.userId) === ws) clients.delete(ws.userId);
    resetTransmitterIf(ws.userId);
    broadcastJson({ type: "user_list", clients: clientList() });
  });

  ws.on("error", () => {});
});

setInterval(() => {
  for (const [userId, ws] of clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (_) {}
      clients.delete(userId);
      resetTransmitterIf(userId);
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 30000);

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`Z-Link Talk Audio Server listening on ${PORT}`);
});

setInterval(() => {
  if (audioPacketCount > 0) {
    console.log(`audio packets relayed: ${audioPacketCount}`);
    audioPacketCount = 0;
  }
}, 60000);

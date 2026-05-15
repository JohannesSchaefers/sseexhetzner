/*

const clients = new Set<ReadableStreamDefaultController<Uint8Array>>();
const encoder = new TextEncoder();

// Keep-alive ping every 15 seconds
setInterval(() => {
  const ping = encoder.encode(": ping\n\n");
  let active = 0;

  for (const client of clients) {
    try {
      client.enqueue(ping);
      active++;
    } catch {
      clients.delete(client);
    }
  }
  if (active > 0) console.log(`Sent keep-alive ping to ${active} client(s)`);
}, 15000);

function broadcast(message: string) {
  const data = `data: ${message}\n\n`;
  const chunk = encoder.encode(data);
  let delivered = 0;

  for (const client of clients) {
    try {
      client.enqueue(chunk);
      delivered++;
    } catch {
      clients.delete(client);
    }
  }
  console.log(`Broadcasted "${message}" to ${delivered} client(s)`);
}

function triggerEmail(value: string) {
  if (value.includes("geschlossen")) {
    console.log("[EMAIL] Schalter wurde geschlossen");
  } else if (value.includes("geöffnet")) {
    console.log("[EMAIL] Schalter wurde geöffnet");
  }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);

  if (url.pathname === "/events") {
    return new Response(
      new ReadableStream({
        start(controller) {
          clients.add(controller);
          console.log("Client connected. Total:", clients.size);

          req.signal.addEventListener("abort", () => {
            clients.delete(controller);
            console.log("Client disconnected. Remaining:", clients.size);
          });
        },
      }),
      {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "connection": "keep-alive",
        },
      }
    );
  }

  if (url.pathname === "/post" && req.method === "POST") {
    const text = await req.text();
    const value = text.trim();

    console.log("Received from ESP32:", value);

    broadcast(value);
    triggerEmail(value);

    return new Response("OK\n", { status: 200 });
  }

  if (url.pathname === "/" || url.pathname === "") {
    return new Response(`
<!doctype html>
<html>
<head><meta charset="utf-8"></head>
<body>
  <h1>Deno SSE Receiver (with Keep-Alive)</h1>
  <pre id="log" style="background:#f4f4f4; padding:12px; height:80vh; overflow:auto; font-family:monospace; white-space:pre-wrap;"></pre>

  <script>
    const log = document.getElementById("log");

    function connect() {
      const es = new EventSource("/events");

      es.onopen = () => log.textContent += "[✅ Connected to server]\\n";

      es.onmessage = (e) => {
        if (e.data) {
          log.textContent += e.data + "\\n";
        }
        log.scrollTop = log.scrollHeight;
      };

      es.onerror = () => {
        es.close();
        log.textContent += "[🔄 Reconnecting...]\\n";
        setTimeout(connect, 2000);
      };
    }

    connect();
  </script>
</body>
</html>`, {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }

  return new Response("Not found", { status: 404 });
});
*/

import { Hono } from 'hono'
import { stream, streamText, streamSSE } from 'hono/streaming'   // ← richtig

const app = new Hono();

const clients = new Set<(chunk: string) => void>();

// Keep-Alive Ping alle 15 Sekunden
setInterval(() => {
  const ping = ": ping\n\n";
  let active = 0;

  for (const send of clients) {
    try {
      send(ping);
      active++;
    } catch {
      clients.delete(send);
    }
  }
  if (active > 0) console.log(`[Keep-alive] ${active} Clients`);
}, 15000);

// Broadcast Funktion
function broadcast(value: string) {
  const payload = JSON.stringify({
    value: value.trim(),
    timestamp: new Date().toISOString(),
  });

  const chunk = `data: ${payload}\n\n`;

  let delivered = 0;
  for (const send of clients) {
    try {
      send(chunk);
      delivered++;
    } catch {
      clients.delete(send);
    }
  }
  console.log(`[Broadcast] "${value}" → ${delivered} Client(s)`);
}

// SSE Endpoint
app.get("/events", (c) => {
  return streamSSE(c, async (stream) => {
    const sender = (chunk: string) => stream.write(chunk);
    clients.add(sender);

    console.log(`[+] Client verbunden | Gesamt: ${clients.size}`);

    c.req.raw.signal.addEventListener("abort", () => {
      clients.delete(sender);
      console.log(`[-] Client getrennt | Verbleibend: ${clients.size}`);
    });

    // Willkommensnachricht
    await stream.writeSSE({
      data: JSON.stringify({ 
        value: "✅ Verbunden", 
        timestamp: new Date().toISOString() 
      })
    });

    await stream.sleep(999999999);
  });
});

// POST von Schalter / ESP32
app.post("/post", async (c) => {
  try {
    const value = (await c.req.text()).trim();

    if (!value) return c.text("Empty", 400);

    console.log(`[POST] Empfangen: ${value}`);
    broadcast(value);

    return c.text("OK", 200);
  } catch (err) {
    console.error("POST Fehler:", err);
    return c.text("Error", 500);
  }
});

// Einfache Test-Seite
app.get("/", (c) => {
  return c.html(`
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <title>Schalter Monitor</title>
</head>
<body>
  <h1>Schalter Monitor (SSE)</h1>
  <pre id="log" style="background:#111;color:#0f0;padding:15px;height:90vh;overflow:auto;font-family:monospace;"></pre>

  <script>
    const log = document.getElementById('log');

    function connect() {
      const es = new EventSource("/events");

      es.onmessage = (e) => {
        if (!e.data) return;
        try {
          const data = JSON.parse(e.data);
          log.textContent += \`[\${data.timestamp.slice(11,19)}] \${data.value}\\n\`;
        } catch {
          log.textContent += e.data + "\\n";
        }
        log.scrollTop = log.scrollHeight;
      };

      es.onerror = () => {
        log.textContent += "[🔄 Reconnecting...]\\n";
        setTimeout(connect, 2500);
      };
    }

    connect();
  </script>
</body>
</html>`);
});

Deno.serve(app.fetch);
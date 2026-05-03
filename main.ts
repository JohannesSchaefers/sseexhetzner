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
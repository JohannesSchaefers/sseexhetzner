import { Hono } from "hono";
import { streamSSE } from "hono/streaming";

const app = new Hono();

type Client = (data: string) => Promise<void>;

const clients = new Set<Client>();

// Keep-alive every 15 seconds
setInterval(async () => {
  if (clients.size === 0) return;

  let active = 0;

  await Promise.allSettled(
    [...clients].map(async (send) => {
      try {
        await send("__ping__");
        active++;
      } catch {
        clients.delete(send);
      }
    })
  );

  console.log(`[Keep-alive] ${active} client(s)`);
}, 15000);

async function broadcast(value: string) {
  const payload = JSON.stringify({
    value: value.trim(),
    timestamp: new Date().toISOString(),
  });

  let delivered = 0;

  await Promise.allSettled(
    [...clients].map(async (send) => {
      try {
        await send(payload);
        delivered++;
      } catch {
        clients.delete(send);
      }
    })
  );

  console.log(`[Broadcast] "${value}" → ${delivered} client(s)`);
}

// SSE endpoint
app.get("/events", (c) => {
  return streamSSE(c, async (s) => {
    const client: Client = async (data: string) => {
      await s.writeSSE({ data });
    };

    clients.add(client);

    console.log(`[+] Client connected | Total: ${clients.size}`);

    const cleanup = () => {
      clients.delete(client);
      console.log(`[-] Client disconnected | Remaining: ${clients.size}`);
    };

    c.req.raw.signal.addEventListener("abort", cleanup, { once: true });

    try {
      await s.writeSSE({
        data: JSON.stringify({
          value: "✅ Verbunden",
          timestamp: new Date().toISOString(),
        }),
      });

      await new Promise<void>((resolve) => {
        c.req.raw.signal.addEventListener("abort", resolve, {
          once: true,
        });
      });
    } finally {
      cleanup();
    }
  });
});

// ESP32 POST endpoint
app.post("/post", async (c) => {
  try {
    const value = (await c.req.text()).trim();

    if (!value) {
      return c.text("Empty", 400);
    }

    console.log(`[POST] ${value}`);

    await broadcast(value);

    return c.text("OK", 200);
  } catch (err) {
    console.error("[POST ERROR]", err);
    return c.text("Error", 500);
  }
});

// Test page
app.get("/", (c) => {
  return c.html(`
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Schalter Monitor</title>
<style>
body {
  font-family: system-ui, sans-serif;
  margin: 20px;
}
#log {
  background: #111;
  color: #0f0;
  padding: 15px;
  height: 80vh;
  overflow: auto;
  font-family: monospace;
  white-space: pre-wrap;
}
</style>
</head>
<body>

<h1>Schalter Monitor 5</h1>

<pre id="log"></pre>

<script>
const log = document.getElementById("log");

const append = (text) => {
  log.textContent += text + "\\n";
  log.scrollTop = log.scrollHeight;
};

const es = new EventSource("/events");

es.onopen = () => {
  append("[✅ Connected]");
};

es.onmessage = (e) => {
  if (!e.data) return;

  if (e.data === "__ping__") {
    return;
  }

  try {
    const data = JSON.parse(e.data);

    append(
      "[" +
      data.timestamp.slice(11, 19) +
      "] " +
      data.value
    );
  } catch {
    append(e.data);
  }
};

es.onerror = () => {
  append("[⚠️ Connection lost - EventSource will reconnect automatically]");
};
</script>

</body>
</html>
`);
});

Deno.serve(app.fetch);
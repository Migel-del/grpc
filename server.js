import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import fetch from "node-fetch";
import dotenv from "dotenv";
import express from "express";

dotenv.config();

const PORT = process.env.PORT || 5566;
const BACKEND_URL = process.env.BACKEND_URL;
if (!BACKEND_URL) {
  console.error("❌ BACKEND_URL not set in .env");
  process.exit(1);
}

// ====== gRPC DEFINITIONS ======
const PROTO_PATH = "./service.proto";
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition).marznode;

// ====== UTILS ======
async function forwardToBackend(method, body) {
  const url = `${BACKEND_URL}/${method}`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json().catch(() => ({}));
  } catch (err) {
    console.error(`[${method}] backend error:`, err.message);
    throw err;
  }
}

// ====== gRPC IMPLEMENTATION ======
const impl = {
  async RepopulateUsers(call, callback) {
    try {
      const result = await forwardToBackend("RepopulateUsers", call.request);
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  async FetchUsersStats(call, callback) {
    try {
      const result = await forwardToBackend("FetchUsersStats", {});
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  async FetchBackends(call, callback) {
    try {
      const result = await forwardToBackend("FetchBackends", {});
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  async RestartBackend(call, callback) {
    try {
      const result = await forwardToBackend("RestartBackend", call.request);
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  async FetchBackendConfig(call, callback) {
    try {
      const result = await forwardToBackend("FetchBackendConfig", call.request);
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  async GetBackendStats(call, callback) {
    try {
      const result = await forwardToBackend("GetBackendStats", call.request);
      callback(null, result);
    } catch (e) {
      callback(e);
    }
  },

  StreamBackendLogs(call) {
    forwardToBackend("StreamBackendLogs", call.request)
      .then((lines) => {
        if (Array.isArray(lines)) {
          for (const line of lines)
            call.write({ line: line.line || JSON.stringify(line) });
        }
        call.end();
      })
      .catch((err) => {
        call.emit("error", err);
        call.end();
      });
  },

  // --- Поток SyncUsers (заглушка для панели)
  SyncUsers(stream) {
    stream.on("data", (data) => {
      console.log("[SyncUsers] got user:", data.user?.username);
    });
    stream.on("end", () => stream.end());
  },
};

// ====== gRPC SERVER ======
const server = new grpc.Server();
server.addService(proto.MarzService.service, impl);
server.bindAsync(
  `0.0.0.0:${PORT}`,
  grpc.ServerCredentials.createInsecure(),
  () => {
    server.start();
    console.log(`✅ MarzProxy gRPC server running on port ${PORT}`);
    console.log(`→ Forwarding to backend: ${BACKEND_URL}`);
  }
);

// ====== EXPRESS HEALTHCHECK ======
import express from "express";
const app = express();
app.get("/", (_, res) => res.send("OK"));
app.get("/health", (_, res) => res.send("healthy"));

const httpPort = process.env.PORT || 8080;
app.listen(httpPort, () =>
  console.log(`HTTP healthcheck running on :${httpPort}`)
);

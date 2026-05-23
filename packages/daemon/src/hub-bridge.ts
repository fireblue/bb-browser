/**
 * Hub bridge — connects daemon directly to Pinix Hub via gRPC ProviderStream.
 *
 * Extracted from bin/bb-browser-provider.ts and adapted to call
 * dispatchRequest() directly instead of HTTP round-tripping to daemon.
 */

import { readFileSync, readdirSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { readFile as readFileAsync } from "node:fs/promises";
import { join, dirname, resolve, relative, extname } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHmac } from "node:crypto";
import { homedir } from "node:os";
import { statSync, readdirSync as readdirSyncNative, unlinkSync } from "node:fs";
import { create } from "@bufbuild/protobuf";
import { createClient, type CallOptions } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  HubService,
  type ProviderMessage,
  type HubMessage,
  type InvokeCommand,
  type DataCommand,
  ProviderMessageSchema,
  RegisterRequestSchema,
  ClipRegistrationSchema,
  InvokeResultSchema,
  DataResultSchema,
  DataEntrySchema,
  DataStatSchema,
  HeartbeatSchema,
  HubErrorSchema,
} from "@pinixai/hub-client";
// GetClipWebResultSchema is not re-exported from @pinixai/hub-client index;
// import directly from the gen file via relative path to node_modules.
import { GetClipWebResultSchema, type GetClipWebCommand } from "../../../node_modules/@pinixai/hub-client/src/gen/hub_pb.ts";

import { COMMANDS, commandToJsonSchema } from "@bb-browser/shared";
import { COMMAND_TIMEOUT } from "@bb-browser/shared";
import type { Request } from "@bb-browser/shared";
import { BB_BROWSER_ROOT, getInstanceId } from "@bb-browser/shared";

import { CdpConnection } from "./cdp-connection.js";
import { dispatchRequest } from "./command-dispatch.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LOG_PREFIX = "[hub-bridge]";
const INSTANCE_ID = getInstanceId();
const PROVIDER_NAME = INSTANCE_ID === "default" ? "bb-browser" : `bb-browser-${INSTANCE_ID}`;
const BROWSER_CLIP_ALIAS = INSTANCE_ID === "default" ? "browser" : `browser-${INSTANCE_ID}`;
const BROWSER_CLIP_PACKAGE = "browser";
const BROWSER_CLIP_DOMAIN = "\u6d4f\u89c8\u5668";
const RECONNECT_DELAY_MS = 5000;
const REGISTER_TIMEOUT_MS = 10000;
const HEARTBEAT_INTERVAL_MS = 15000;

const STREAMER_PORT = "0";
let streamerActualPort = 0;
const getStreamerApiBase = () => `http://127.0.0.1:${streamerActualPort}`;

const LOCAL_SITES_DIR = join(BB_BROWSER_ROOT, "sites");
const COMMUNITY_SITES_DIR = join(BB_BROWSER_ROOT, "bb-sites");

const PINIX_DATA_ROOT = join(process.env.PINIX_HOME || join(homedir(), ".pinix"), "data");

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// ---------------------------------------------------------------------------
// Package version
// ---------------------------------------------------------------------------

function readPackageVersion(): string {
  try {
    // Resolve relative to this file — works for both dev and built paths
    const currentDir = dirname(fileURLToPath(import.meta.url));
    const candidates = [
      resolve(currentDir, "../../../package.json"),   // dev: packages/daemon/src/
      resolve(currentDir, "../../package.json"),       // built: packages/daemon/dist/
      resolve(currentDir, "../package.json"),           // release bundle: dist/
    ];
    for (const p of candidates) {
      try {
        const pkg = JSON.parse(readFileSync(p, "utf-8")) as { version?: string };
        if (pkg.version) return pkg.version.trim();
      } catch {}
    }
    return "0.0.0";
  } catch {
    return "0.0.0";
  }
}

const CLIP_VERSION = readPackageVersion();

// ---------------------------------------------------------------------------
// Streamer (viewer) sidecar management
// ---------------------------------------------------------------------------

let streamerProcess: ChildProcess | null = null;

function findStreamerBinary(): string | null {
  const localPath = join(BB_BROWSER_ROOT, "bin", "bb-viewer");
  if (existsSync(localPath)) return localPath;
  return "bb-viewer";
}

function generateTurnCredentials(secret: string, ttlSeconds = 86400): { username: string; password: string } {
  const expiry = Math.floor(Date.now() / 1000) + ttlSeconds;
  const username = `${expiry}:bbviewer`;
  const password = createHmac("sha1", secret).update(username).digest("base64");
  return { username, password };
}

async function ensureStreamer(): Promise<void> {
  if (streamerProcess && !streamerProcess.killed && streamerActualPort > 0) {
    try {
      const resp = await fetch(`${getStreamerApiBase()}/health`, { signal: AbortSignal.timeout(2000) });
      if (resp.ok) return;
    } catch {}
    try { streamerProcess.kill(); } catch {}
    streamerProcess = null;
    streamerActualPort = 0;
  }

  const bin = findStreamerBinary();
  if (!bin) throw new Error("bb-viewer binary not found");

  const args = ["--api-only", "--port", STREAMER_PORT];

  const turnUrl = process.env.TURN_URL;
  const turnSecret = process.env.TURN_SECRET;
  if (turnUrl && turnSecret) {
    const { username, password } = generateTurnCredentials(turnSecret);
    args.push("--turn-url", turnUrl, "--turn-user", username, "--turn-cred", password);
  } else if (turnUrl) {
    args.push("--turn-url", turnUrl);
  }

  console.error(`${LOG_PREFIX} Spawning streamer: ${bin} ${args.join(" ")}`);
  const child = spawn(bin, args, { stdio: ["ignore", "pipe", "pipe"] });
  streamerProcess = child;

  child.stdout?.on("data", (d: Buffer) => {
    for (const line of d.toString().trim().split("\n")) {
      console.error(`${LOG_PREFIX} [streamer] ${line}`);
      if (streamerActualPort === 0) {
        const match = line.match(/(?:listening|port)\s*[:\s]*(\d+)/i);
        if (match) streamerActualPort = parseInt(match[1], 10);
      }
    }
  });
  child.stderr?.on("data", (d: Buffer) => {
    for (const line of d.toString().trim().split("\n")) {
      console.error(`${LOG_PREFIX} [streamer] ${line}`);
      if (streamerActualPort === 0) {
        const match = line.match(/(?:listening|port)\s*[:\s]*(\d+)/i);
        if (match) streamerActualPort = parseInt(match[1], 10);
      }
    }
  });
  child.on("exit", (code) => {
    console.error(`${LOG_PREFIX} [streamer] exited with code ${code}`);
    if (streamerProcess === child) { streamerProcess = null; streamerActualPort = 0; }
  });

  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 300));
    if (streamerActualPort <= 0) continue;
    try {
      const resp = await fetch(`${getStreamerApiBase()}/health`, { signal: AbortSignal.timeout(2000) });
      if (resp.ok) {
        console.error(`${LOG_PREFIX} Streamer ready at port ${streamerActualPort}`);
        return;
      }
    } catch {}
  }
  throw new Error("Streamer did not become healthy in 10s");
}

async function streamerCommand(path: string, body?: unknown): Promise<unknown> {
  const url = `${getStreamerApiBase()}${path}`;
  const opts: RequestInit = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
    signal: AbortSignal.timeout(15000),
  };
  const resp = await fetch(url, opts);
  const text = await resp.text();
  if (!resp.ok) throw new Error(`Streamer ${path} failed (${resp.status}): ${text}`);
  try { return JSON.parse(text); } catch { return { output: text }; }
}

export function stopStreamer(): void {
  if (streamerProcess && !streamerProcess.killed) {
    console.error(`${LOG_PREFIX} Stopping streamer`);
    try { streamerProcess.kill(); } catch {}
    streamerProcess = null;
  }
}

// ---------------------------------------------------------------------------
// Site adapter scanning
// ---------------------------------------------------------------------------

interface SiteAdapterMeta {
  name: string;
  description: string;
  domain: string;
  args: Record<string, { required?: boolean; description?: string }>;
}

interface PlatformClip {
  alias: string;
  domain: string;
  commands: { name: string; description: string; inputSchema: string }[];
}

function parseSiteMeta(filePath: string, sitesDir: string): SiteAdapterMeta | null {
  let content: string;
  try { content = readFileSync(filePath, "utf-8"); } catch { return null; }
  const defaultName = relative(sitesDir, filePath).replace(/\.js$/, "").replace(/\\/g, "/");
  const metaMatch = content.match(/\/\*\s*@meta\s*\n([\s\S]*?)\*\//);
  if (!metaMatch) return { name: defaultName, description: "", domain: "", args: {} };
  try {
    const m = JSON.parse(metaMatch[1]);
    return { name: m.name || defaultName, description: m.description || "", domain: m.domain || "", args: m.args || {} };
  } catch {
    return { name: defaultName, description: "", domain: "", args: {} };
  }
}

function scanSitesDir(dir: string): SiteAdapterMeta[] {
  if (!existsSync(dir)) return [];
  const results: SiteAdapterMeta[] = [];
  function walk(d: string) {
    let entries;
    try { entries = readdirSync(d, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      const p = join(d, e.name);
      if (e.isDirectory() && !e.name.startsWith(".")) walk(p);
      else if (e.isFile() && e.name.endsWith(".js")) {
        const m = parseSiteMeta(p, dir);
        if (m) results.push(m);
      }
    }
  }
  walk(dir);
  return results;
}

function metaArgsToJsonSchema(args: Record<string, { required?: boolean; description?: string }>): string {
  const properties: Record<string, unknown> = {};
  const required: string[] = [];
  for (const [name, def] of Object.entries(args)) {
    properties[name] = { type: "string", ...(def.description ? { description: def.description } : {}) };
    if (def.required) required.push(name);
  }
  return JSON.stringify({
    type: "object", properties,
    ...(required.length > 0 ? { required } : {}),
    additionalProperties: true,
  });
}

function buildPlatformClips(): PlatformClip[] {
  const community = scanSitesDir(COMMUNITY_SITES_DIR);
  const local = scanSitesDir(LOCAL_SITES_DIR);
  const byName = new Map<string, SiteAdapterMeta>();
  for (const s of community) byName.set(s.name, s);
  for (const s of local) byName.set(s.name, s);

  const groups = new Map<string, SiteAdapterMeta[]>();
  for (const adapter of byName.values()) {
    const slash = adapter.name.indexOf("/");
    if (slash <= 0) continue;
    const platform = adapter.name.substring(0, slash);
    const existing = groups.get(platform) || [];
    existing.push(adapter);
    groups.set(platform, existing);
  }

  const clips: PlatformClip[] = [];
  for (const [platform, adapters] of groups) {
    const firstDomain = adapters.find((a) => a.domain)?.domain || "";
    const commands = adapters.map((a) => {
      const cmdName = a.name.substring(platform.length + 1);
      return { name: cmdName, description: a.description, inputSchema: metaArgsToJsonSchema(a.args) };
    });
    clips.push({ alias: platform, domain: firstDomain, commands });
  }
  return clips;
}

// ---------------------------------------------------------------------------
// Build clip registrations
// ---------------------------------------------------------------------------

const BROWSER_COMMANDS = COMMANDS.filter((c) => c.group !== "site");

const STREAM_COMMANDS = [
  {
    name: "stream.start",
    description: "Start remote viewing: creates a WebRTC peer and returns offer SDP + ICE candidates",
    inputSchema: JSON.stringify({ type: "object", properties: {}, additionalProperties: true }),
  },
  {
    name: "stream.answer",
    description: "Complete WebRTC connection: accepts answer SDP + ICE candidates, starts video streaming",
    inputSchema: JSON.stringify({
      type: "object",
      properties: {
        answer_sdp: { type: "string", description: "Answer SDP from the remote peer" },
        candidates: {
          type: "array",
          items: {
            type: "object",
            properties: { candidate: { type: "string" }, sdpMLineIndex: { type: "number" } },
            required: ["candidate", "sdpMLineIndex"],
          },
          description: "ICE candidates from the remote peer",
        },
      },
      required: ["answer_sdp"],
      additionalProperties: true,
    }),
  },
  {
    name: "stream.close",
    description: "Stop remote viewing and close the WebRTC peer",
    inputSchema: JSON.stringify({ type: "object", properties: {}, additionalProperties: true }),
  },
  {
    name: "stream.switch",
    description: "Switch the streaming tab: tell the streamer to connect to a different tab's CDP WebSocket",
    inputSchema: JSON.stringify({
      type: "object",
      properties: {
        tab: { type: "string", description: "Short tab ID to switch streaming to" },
      },
      required: ["tab"],
      additionalProperties: true,
    }),
  },
];

const BROWSER_COMMAND_NAMES = [
  ...BROWSER_COMMANDS.map((c) => c.method),
  "stream.start",
  "stream.answer",
  "stream.close",
  "stream.switch",
];

function buildClipRegistrations() {
  const browserCommands = [
    ...BROWSER_COMMANDS.map((cmd) => ({
      name: cmd.method,
      description: cmd.description,
      input: commandToJsonSchema(cmd),
      output: JSON.stringify({ type: "object", additionalProperties: true }),
    })),
    ...STREAM_COMMANDS.map((cmd) => ({
      name: cmd.name,
      description: cmd.description,
      input: cmd.inputSchema,
      output: JSON.stringify({ type: "object", additionalProperties: true }),
    })),
  ];

  const platformClips = buildPlatformClips();

  const browserClip = create(ClipRegistrationSchema, {
    alias: BROWSER_CLIP_ALIAS,
    package: BROWSER_CLIP_PACKAGE,
    version: CLIP_VERSION,
    domain: BROWSER_CLIP_DOMAIN,
    commands: browserCommands,
    hasWeb: true,
    dependencies: [],
    tokenProtected: false,
  });

  const siteClips = platformClips.map((pc) =>
    create(ClipRegistrationSchema, {
      alias: pc.alias,
      package: `browser-site-${pc.alias}`,
      version: CLIP_VERSION,
      domain: pc.domain,
      commands: pc.commands.map((c) => ({
        name: c.name,
        description: c.description,
        input: c.inputSchema,
        output: JSON.stringify({ type: "object", additionalProperties: true }),
      })),
      hasWeb: false,
      dependencies: [BROWSER_CLIP_ALIAS],
      tokenProtected: false,
    }),
  );

  return { browserClip, siteClips, platformClips };
}

// ---------------------------------------------------------------------------
// Input/output encoding
// ---------------------------------------------------------------------------

type InputObject = Record<string, unknown>;

function decodeInput(data: Uint8Array | undefined): InputObject {
  if (!data || data.length === 0) return {};
  let raw = textDecoder.decode(data).trim();
  if (!raw) return {};
  raw = raw.replace(
    /"(?:[^"\\]|\\.)*"|\d{16,}/g,
    (m) => m.startsWith('"') ? m : `"${m}"`,
  );
  try { return JSON.parse(raw) as InputObject; }
  catch { throw new Error("Invoke input must be valid JSON"); }
}

function encodeOutput(value: unknown): Uint8Array {
  return textEncoder.encode(JSON.stringify(value ?? {}));
}

// ---------------------------------------------------------------------------
// Clip Data file I/O
// ---------------------------------------------------------------------------

function clipDataDir(clipName: string): string {
  return join(PINIX_DATA_ROOT, clipName);
}

function validateDataPath(p: string): void {
  if (!p && p !== "") return;
  if (p.startsWith("/") || p.startsWith("\\")) throw new Error("Absolute paths not allowed");
  const cleaned = p.replace(/\\/g, "/").split("/").filter(s => s !== ".");
  if (cleaned.some(s => s === "..")) throw new Error(`Path "${p}" escapes data directory`);
}

function guessMime(name: string): string {
  const ext = extname(name).toLowerCase();
  const map: Record<string, string> = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
    ".webp": "image/webp", ".svg": "image/svg+xml", ".json": "application/json",
    ".txt": "text/plain", ".html": "text/html", ".css": "text/css", ".js": "application/javascript",
    ".pdf": "application/pdf", ".mp4": "video/mp4", ".mp3": "audio/mpeg", ".wav": "audio/wav",
  };
  return map[ext] || "application/octet-stream";
}

async function handleDataCommand(cmd: DataCommand): Promise<Uint8Array> {
  const clipName = cmd.clipName?.trim() || "";
  const operation = cmd.operation?.trim().toLowerCase() || "";
  const dataPath = cmd.path?.trim() || "";

  if (!clipName) throw new Error("clip_name is required");
  if (!operation) throw new Error("operation is required");
  if (operation !== "list" && !dataPath) throw new Error("path is required");
  validateDataPath(dataPath);

  const dataDir = clipDataDir(clipName);
  const fullPath = dataPath ? join(dataDir, dataPath) : dataDir;

  switch (operation) {
    case "read": {
      const content = await readFileAsync(fullPath);
      return textEncoder.encode(JSON.stringify({ content: Buffer.from(content).toString("base64") }));
    }
    case "write": {
      mkdirSync(dirname(fullPath), { recursive: true });
      writeFileSync(fullPath, cmd.content);
      const uri = `pinix://${clipName}/${dataPath}`;
      return textEncoder.encode(JSON.stringify({ uri }));
    }
    case "list": {
      mkdirSync(fullPath, { recursive: true });
      const entries = readdirSyncNative(fullPath, { withFileTypes: true }).map(e => {
        const entryPath = dataPath ? `${dataPath}/${e.name}` : e.name;
        let size = 0;
        try { size = statSync(join(fullPath, e.name)).size; } catch {}
        return {
          name: e.name,
          path: `pinix://${clipName}/${entryPath}`,
          type: e.isDirectory() ? "directory" : "file",
          size,
          mime: e.isDirectory() ? "" : guessMime(e.name),
        };
      });
      return textEncoder.encode(JSON.stringify({ entries }));
    }
    case "delete": {
      unlinkSync(fullPath);
      return textEncoder.encode(JSON.stringify({ uri: `pinix://${clipName}/${dataPath}` }));
    }
    case "stat": {
      const info = statSync(fullPath);
      return textEncoder.encode(JSON.stringify({
        stat: { size: info.size, mime: guessMime(dataPath), modified: info.mtime.toISOString() },
      }));
    }
    default:
      throw new Error(`Unsupported data operation: ${operation}`);
  }
}

// ---------------------------------------------------------------------------
// AsyncMessageQueue
// ---------------------------------------------------------------------------

class AsyncMessageQueue<T> implements AsyncIterable<T>, AsyncIterator<T> {
  private readonly values: T[] = [];
  private readonly waiters: Array<{ resolve: (r: IteratorResult<T>) => void; reject: (e: unknown) => void }> = [];
  private closed = false;
  private failed: Error | null = null;

  push(value: T): void {
    if (this.closed) throw new Error("queue is closed");
    if (this.failed) throw this.failed;
    const w = this.waiters.shift();
    if (w) { w.resolve({ done: false, value }); return; }
    this.values.push(value);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    while (this.waiters.length) this.waiters.shift()!.resolve({ done: true, value: undefined as never });
  }

  fail(error: unknown): void {
    if (this.failed) return;
    this.failed = error instanceof Error ? error : new Error(String(error));
    while (this.waiters.length) this.waiters.shift()!.reject(this.failed);
  }

  next(): Promise<IteratorResult<T>> {
    if (this.values.length) return Promise.resolve({ done: false, value: this.values.shift()! });
    if (this.failed) return Promise.reject(this.failed);
    if (this.closed) return Promise.resolve({ done: true, value: undefined as never });
    return new Promise((resolve, reject) => { this.waiters.push({ resolve, reject }); });
  }

  return(): Promise<IteratorResult<T>> { this.close(); return Promise.resolve({ done: true, value: undefined as never }); }
  throw(e?: unknown): Promise<IteratorResult<T>> { this.fail(e ?? new Error("aborted")); return Promise.reject(this.failed); }
  [Symbol.asyncIterator](): AsyncIterator<T> { return this; }
}

// ---------------------------------------------------------------------------
// ProviderStream bridge
// ---------------------------------------------------------------------------

interface ProviderClient {
  providerStream(request: AsyncIterable<ProviderMessage>, options?: CallOptions): AsyncIterable<HubMessage>;
}

export interface HubBridgeOptions {
  hubUrl: string;
  hubToken?: string;
  cdp: CdpConnection;
  /** @deprecated No longer used by streamer; kept for backward compat. */
  cdpPort?: number;
}

export class HubBridge {
  private abortController: AbortController | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;
  private platformClipAliases = new Set<string>();

  private readonly hubUrl: string;
  private readonly hubToken: string | undefined;
  private readonly cdp: CdpConnection;

  constructor(options: HubBridgeOptions) {
    this.hubUrl = options.hubUrl;
    this.hubToken = options.hubToken;
    this.cdp = options.cdp;
  }

  start(): void { this.connect(); }

  stop(): void {
    this.stopped = true;
    this.clearReconnect();
    this.abortController?.abort();
    stopStreamer();
  }

  private connect(): void {
    if (this.stopped) return;
    this.runStream().catch((err) => {
      if (this.stopped) return;
      console.error(`${LOG_PREFIX} Stream error: ${err instanceof Error ? err.message : err}`);
      this.scheduleReconnect();
    });
  }

  private async runStream(): Promise<void> {
    console.error(`${LOG_PREFIX} Connecting to ${this.hubUrl}`);
    const transport = createGrpcTransport({ baseUrl: this.hubUrl, httpVersion: "2" });
    const client = createClient(HubService, transport) as unknown as ProviderClient;

    const ac = new AbortController();
    this.abortController = ac;

    const queue = new AsyncMessageQueue<ProviderMessage>();
    const heartbeat = setInterval(() => {
      if (ac.signal.aborted) return;
      try {
        queue.push(create(ProviderMessageSchema, {
          payload: { case: "ping", value: create(HeartbeatSchema, { sentAtUnixMs: BigInt(Date.now()) }) },
        }));
      } catch {}
    }, HEARTBEAT_INTERVAL_MS);

    let registerAccepted = false;
    const registerTimeout = setTimeout(() => {
      if (registerAccepted || ac.signal.aborted) return;
      ac.abort();
    }, REGISTER_TIMEOUT_MS);

    try {
      const callOpts = this.getCallOptions(ac.signal);
      const stream = client.providerStream(queue, callOpts);

      // Send register message (re-scan adapters on each reconnect)
      const { browserClip, siteClips, platformClips } = buildClipRegistrations();
      this.platformClipAliases = new Set(platformClips.map((p) => p.alias));
      queue.push(create(ProviderMessageSchema, {
        payload: {
          case: "register",
          value: create(RegisterRequestSchema, {
            providerName: PROVIDER_NAME,
            clips: [browserClip, ...siteClips],
          }),
        },
      }));

      for await (const msg of stream) {
        if (ac.signal.aborted && this.stopped) return;
        switch (msg.payload.case) {
          case "registerResponse": {
            clearTimeout(registerTimeout);
            if (!msg.payload.value.accepted) {
              throw new Error(msg.payload.value.message || "Registration rejected");
            }
            registerAccepted = true;
            this.clearReconnect();
            const totalCmds = BROWSER_COMMAND_NAMES.length + platformClips.reduce((n, p) => n + p.commands.length, 0);
            console.error(`${LOG_PREFIX} Registered ${1 + platformClips.length} clips (${totalCmds} commands) at ${this.hubUrl}`);
            break;
          }
          case "invokeCommand": {
            void this.handleInvoke(queue, msg.payload.value);
            break;
          }
          case "dataCommand": {
            void this.handleData(queue, msg.payload.value);
            break;
          }
          case "invokeInput": break;
          case "pong": break;
          case "getClipWebCommand": {
            void this.handleGetClipWeb(queue, msg.payload.value);
            break;
          }
          default: break;
        }
      }
      throw new Error("Provider stream closed");
    } catch (err) {
      if (this.stopped) return;
      throw err;
    } finally {
      clearInterval(heartbeat);
      clearTimeout(registerTimeout);
      queue.close();
      if (this.abortController === ac) this.abortController = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Invoke handling — calls dispatchRequest() directly (no HTTP round-trip)
  // ---------------------------------------------------------------------------

  private async handleInvoke(queue: AsyncMessageQueue<ProviderMessage>, inv: InvokeCommand): Promise<void> {
    const requestId = inv.requestId?.trim();
    if (!requestId) return;

    try {
      const clipName = inv.clipName?.trim() || "";
      const command = inv.command?.trim() || "";
      const input = decodeInput(inv.input);
      let result: unknown;

      // stream.* commands → streamer sidecar (Protocol 2: /command)
      // Streamer returns {result: {...}} — unwrap to pass the inner result to Hub.
      if (clipName === BROWSER_CLIP_ALIAS && command === "stream.start") {
        await ensureStreamer();
        const cdpUrl = await this.getCurrentTabCdpUrl();
        const resp = await streamerCommand("/command", {
          method: "connect",
          params: { cdpUrl },
        }) as any;
        result = resp?.result ?? resp;
      } else if (clipName === BROWSER_CLIP_ALIAS && command === "stream.answer") {
        const resp = await streamerCommand("/command", {
          method: "answer",
          params: input,
        }) as any;
        result = resp?.result ?? resp;
      } else if (clipName === BROWSER_CLIP_ALIAS && command === "stream.close") {
        const resp = await streamerCommand("/command", {
          method: "stop",
        }) as any;
        result = resp?.result ?? resp;
      } else if (clipName === BROWSER_CLIP_ALIAS && command === "stream.switch") {
        const tabRef = (input as Record<string, unknown>).tab;
        if (!tabRef) throw new Error("Missing tab parameter");
        const cdpUrl = await this.getTabCdpUrl(String(tabRef));
        const resp = await streamerCommand("/command", {
          method: "switch",
          params: { cdpUrl },
        }) as any;
        result = resp?.result ?? resp;
      } else if (clipName === BROWSER_CLIP_ALIAS) {
        // Browser commands — dispatch directly via CDP (no HTTP round-trip!)
        result = await this.executeBrowserCommand(command, input);
      } else if (this.platformClipAliases.has(clipName)) {
        // Site/platform commands — dispatch directly
        result = await this.executeSiteCommand(clipName, command, input);
      } else {
        throw new Error(`Unknown clip: ${clipName}`);
      }

      this.send(queue, requestId, encodeOutput(result), undefined);
    } catch (err) {
      this.send(queue, requestId, undefined, err);
    }
  }

  /**
   * Build the CDP WebSocket URL for the current (or first) page target.
   * Used by stream.start to tell the streamer which tab to connect to.
   */
  private async getCurrentTabCdpUrl(): Promise<string> {
    if (!this.cdp.connected) {
      await Promise.race([
        this.cdp.waitUntilReady(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("CDP connection timeout")), COMMAND_TIMEOUT),
        ),
      ]);
    }
    const targets = await this.cdp.getTargets();
    const current = this.cdp.currentTargetId
      ? targets.find((t) => t.id === this.cdp.currentTargetId && t.type === "page")
      : undefined;
    const page = current ?? targets.find((t) => t.type === "page");
    if (!page) throw new Error("No page target found for streamer");
    return `ws://${this.cdp.host}:${this.cdp.port}/devtools/page/${page.id}`;
  }

  /**
   * Resolve a short tab ID to a CDP WebSocket URL.
   * Used by stream.switch to tell the streamer which tab to connect to.
   */
  private async getTabCdpUrl(tabRef: string): Promise<string> {
    if (!this.cdp.connected) {
      await Promise.race([
        this.cdp.waitUntilReady(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("CDP connection timeout")), COMMAND_TIMEOUT),
        ),
      ]);
    }
    // Resolve short ID to full target ID
    const resolvedId = this.cdp.tabManager.resolveShortId(tabRef);
    const targetId = resolvedId || tabRef;
    const targets = await this.cdp.getTargets();
    const page = targets.find((t) => t.id === targetId && t.type === "page");
    if (!page) throw new Error(`Tab not found: ${tabRef}`);
    return `ws://${this.cdp.host}:${this.cdp.port}/devtools/page/${page.id}`;
  }

  /**
   * Execute a browser command by calling dispatchRequest() directly.
   * This is the key improvement over the standalone provider which
   * had to HTTP POST to the daemon.
   */
  private async executeBrowserCommand(cmdName: string, input: InputObject): Promise<unknown> {
    const cmd = BROWSER_COMMANDS.find((c) => c.method === cmdName);
    if (!cmd) throw new Error(`Unknown browser command: ${cmdName}`);

    // Wait for CDP to be ready
    if (!this.cdp.connected) {
      await Promise.race([
        this.cdp.waitUntilReady(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("CDP connection timeout")), COMMAND_TIMEOUT),
        ),
      ]);
    }

    const { tab, ...rest } = input;
    const request: Request = {
      method: cmd.method as Request["method"],
      ...rest,
      ...(tab !== undefined ? { tabId: tab } : {}),
    } as Request;

    const response = await dispatchRequest(this.cdp, request);
    if (response.error) throw new Error(response.error.message || "Command failed");
    return response.result ?? {};
  }

  /**
   * Execute a site command by calling dispatchRequest() with method "site_run".
   */
  private async executeSiteCommand(clipName: string, command: string, input: InputObject): Promise<unknown> {
    // Wait for CDP to be ready
    if (!this.cdp.connected) {
      await Promise.race([
        this.cdp.waitUntilReady(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("CDP connection timeout")), COMMAND_TIMEOUT),
        ),
      ]);
    }

    const { tab, ...siteArgs } = input;
    const request: Request = {
      method: "site_run" as Request["method"],
      siteName: `${clipName}/${command}`,
      siteArgs: Object.fromEntries(
        Object.entries(siteArgs)
          .filter(([, v]) => v !== undefined && v !== null && v !== "")
          .map(([k, v]) => [k, String(v)]),
      ),
      ...(tab !== undefined ? { tabId: String(tab) } : {}),
    } as Request;

    const response = await dispatchRequest(this.cdp, request);
    if (response.error) throw new Error(response.error.message || "Site command failed");
    return response.result ?? {};
  }

  // ---------------------------------------------------------------------------
  // Data handling
  // ---------------------------------------------------------------------------

  private async handleData(queue: AsyncMessageQueue<ProviderMessage>, cmd: DataCommand): Promise<void> {
    const requestId = cmd.requestId?.trim();
    if (!requestId) return;

    try {
      const output = await handleDataCommand(cmd);
      this.sendDataResult(queue, requestId, output, undefined);
    } catch (err) {
      this.sendDataResult(queue, requestId, undefined, err);
    }
  }

  // ---------------------------------------------------------------------------
  // GetClipWeb handling — serve files from web/ directory
  // ---------------------------------------------------------------------------

  private async handleGetClipWeb(queue: AsyncMessageQueue<ProviderMessage>, cmd: GetClipWebCommand): Promise<void> {
    const requestId = cmd.requestId?.trim();
    if (!requestId) return;

    try {
      let filePath = cmd.path?.trim() || "";
      if (!filePath || filePath === "/" || filePath === "index.html") filePath = "view.html";
      if (filePath.startsWith("/")) filePath = filePath.slice(1);

      if (filePath.includes("..") || filePath.startsWith("/")) {
        throw new Error("Invalid path");
      }

      // Resolve web/ directory relative to this file
      const currentDir = dirname(fileURLToPath(import.meta.url));
      // Dev: packages/daemon/src/ -> ../../../web
      // Built (package): packages/daemon/dist/ -> ../../../web
      // Release bundle: dist/ -> ../web
      const webDirCandidates = [
        resolve(currentDir, "../../../web"),
        resolve(currentDir, "../../web"),
        resolve(currentDir, "../web"),
      ];
      let webDir: string | null = null;
      for (const candidate of webDirCandidates) {
        if (existsSync(candidate)) {
          webDir = candidate;
          break;
        }
      }
      if (!webDir) throw new Error("web directory not found");

      const fullPath = join(webDir, filePath);
      const resolvedFull = resolve(fullPath);
      const resolvedWeb = resolve(webDir);
      if (!resolvedFull.startsWith(resolvedWeb)) {
        throw new Error("Invalid path");
      }

      const content = await readFileAsync(fullPath);

      const ext = extname(filePath).toLowerCase();
      const MIME_MAP: Record<string, string> = {
        ".html": "text/html; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".mjs": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".ttf": "font/ttf",
      };
      const contentType = MIME_MAP[ext] || "application/octet-stream";
      const etag = `"${content.length.toString(16)}"`;

      if (cmd.ifNoneMatch && cmd.ifNoneMatch === etag) {
        queue.push(create(ProviderMessageSchema, {
          payload: {
            case: "getClipWebResult",
            value: create(GetClipWebResultSchema, {
              requestId,
              notModified: true,
              etag,
              totalSize: BigInt(content.length),
            }),
          },
        }));
        return;
      }

      const offset = Number(cmd.offset || 0n);
      const length = Number(cmd.length || 0n);
      let slice: Uint8Array;
      if (length > 0) {
        slice = new Uint8Array(content.buffer, content.byteOffset + offset, Math.min(length, content.length - offset));
      } else if (offset > 0) {
        slice = new Uint8Array(content.buffer, content.byteOffset + offset);
      } else {
        slice = new Uint8Array(content);
      }

      queue.push(create(ProviderMessageSchema, {
        payload: {
          case: "getClipWebResult",
          value: create(GetClipWebResultSchema, {
            requestId,
            content: slice,
            contentType,
            etag,
            totalSize: BigInt(content.length),
          }),
        },
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const code = message === "Invalid path" ? "invalid_argument" : "not_found";
      queue.push(create(ProviderMessageSchema, {
        payload: {
          case: "getClipWebResult",
          value: create(GetClipWebResultSchema, {
            requestId,
            error: create(HubErrorSchema, { code, message }),
          }),
        },
      }));
    }
  }

  // ---------------------------------------------------------------------------
  // Result senders
  // ---------------------------------------------------------------------------

  private sendDataResult(queue: AsyncMessageQueue<ProviderMessage>, requestId: string, output: Uint8Array | undefined, error: unknown): void {
    try {
      const hubError = error
        ? create(HubErrorSchema, { code: "internal", message: error instanceof Error ? error.message : String(error) })
        : undefined;
      let parsed: Record<string, unknown> = {};
      if (output) {
        try { parsed = JSON.parse(textDecoder.decode(output)); } catch {}
      }
      queue.push(create(ProviderMessageSchema, {
        payload: {
          case: "dataResult",
          value: create(DataResultSchema, {
            requestId,
            content: parsed.content ? new Uint8Array(Buffer.from(parsed.content as string, "base64")) : undefined,
            uri: (parsed.uri as string) || "",
            entries: Array.isArray(parsed.entries)
              ? (parsed.entries as Array<Record<string, unknown>>).map(e => create(DataEntrySchema, {
                  name: (e.name as string) || "",
                  path: (e.path as string) || "",
                  type: (e.type as string) || "file",
                  size: BigInt((e.size as number) || 0),
                  mime: (e.mime as string) || "",
                }))
              : [],
            stat: parsed.stat
              ? create(DataStatSchema, {
                  size: BigInt(((parsed.stat as Record<string, unknown>).size as number) || 0),
                  mime: ((parsed.stat as Record<string, unknown>).mime as string) || "",
                  modified: ((parsed.stat as Record<string, unknown>).modified as string) || "",
                })
              : undefined,
            error: hubError,
          }),
        },
      }));
    } catch (e) {
      if (!this.stopped) console.error(`${LOG_PREFIX} Failed to send data result: ${e instanceof Error ? e.message : e}`);
    }
  }

  private send(queue: AsyncMessageQueue<ProviderMessage>, requestId: string, output: Uint8Array | undefined, error: unknown): void {
    try {
      const hubError = error
        ? create(HubErrorSchema, { code: "internal", message: error instanceof Error ? error.message : String(error) })
        : undefined;
      queue.push(create(ProviderMessageSchema, {
        payload: {
          case: "invokeResult",
          value: create(InvokeResultSchema, { requestId, output, error: hubError, done: true }),
        },
      }));
    } catch (e) {
      if (!this.stopped) console.error(`${LOG_PREFIX} Failed to send result: ${e instanceof Error ? e.message : e}`);
    }
  }

  private getCallOptions(signal: AbortSignal): CallOptions {
    const token = this.hubToken || process.env.PINIX_HUB_TOKEN?.trim() || process.env.PINIX_TOKEN?.trim();
    const opts: CallOptions = { signal, timeoutMs: 0 };
    if (token) opts.headers = { Authorization: `Bearer ${token}` };
    return opts;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.stopped) return;
    console.error(`${LOG_PREFIX} Reconnecting in ${RECONNECT_DELAY_MS}ms`);
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, RECONNECT_DELAY_MS);
  }

  private clearReconnect(): void {
    if (!this.reconnectTimer) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }
}

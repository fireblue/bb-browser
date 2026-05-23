/**
 * bb-browser MCP Server
 *
 * Exposes bb-browser commands as MCP tools via stdio transport.
 * Auto-starts the daemon if not running. Tracks session tabs for cleanup.
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  COMMANDS,
  COMMAND_TIMEOUT,
  DAEMON_DIR,
  type CommandDef,
  type DaemonInfo,
  type Request,
  type Response,
  readDaemonJson,
} from "@bb-browser/shared";
import { execFile, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve, join } from "node:path";
import { z } from "zod";

declare const __BB_BROWSER_VERSION__: string;

// ---------------------------------------------------------------------------
// Session tab tracking
// ---------------------------------------------------------------------------

const sessionOpenedTabs = new Set<string>();

function trackTab(tabId: unknown): void {
  if (typeof tabId === "string" && tabId) sessionOpenedTabs.add(tabId);
  else if (typeof tabId === "number" && Number.isFinite(tabId)) sessionOpenedTabs.add(String(tabId));
}

function untrackTab(tabId: unknown): void {
  if (typeof tabId === "string" && tabId) sessionOpenedTabs.delete(tabId);
  else if (typeof tabId === "number" && Number.isFinite(tabId)) sessionOpenedTabs.delete(String(tabId));
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

function getDaemonPath(): string {
  const dir = dirname(fileURLToPath(import.meta.url));
  const sameDir = resolve(dir, "daemon.js");
  if (existsSync(sameDir)) return sameDir;
  return resolve(dir, "../../daemon/dist/index.js");
}

function getCliPath(): string {
  const dir = dirname(fileURLToPath(import.meta.url));
  const sameDir = resolve(dir, "cli.js");
  if (existsSync(sameDir)) return sameDir;
  return resolve(dir, "../../cli/dist/index.js");
}

// ---------------------------------------------------------------------------
// Daemon lifecycle
// ---------------------------------------------------------------------------

let cachedInfo: DaemonInfo | null = null;

async function getDaemonInfo(): Promise<DaemonInfo | null> {
  if (cachedInfo) return cachedInfo;
  cachedInfo = await readDaemonJson();
  return cachedInfo;
}

async function isDaemonRunning(): Promise<boolean> {
  const info = await getDaemonInfo();
  if (!info) return false;
  try {
    const res = await fetch(`http://${info.host}:${info.port}/status`, {
      signal: AbortSignal.timeout(2000),
      headers: { Authorization: `Bearer ${info.token}` },
    });
    return res.ok;
  } catch { return false; }
}

async function ensureDaemon(): Promise<void> {
  if (await isDaemonRunning()) return;
  cachedInfo = null;

  // Try CLI daemon status first (triggers CDP discovery + Chrome launch)
  try {
    await new Promise<void>((res, rej) => {
      execFile(process.execPath, [getCliPath(), "daemon", "status", "--json"],
        { timeout: 15000 }, (err) => err ? rej(err) : res());
    });
    if (await isDaemonRunning()) return;
  } catch {}

  // Spawn daemon directly
  let cdpArgs: string[] = [];
  try {
    const portFile = join(DAEMON_DIR, "browser", "cdp-port");
    const port = (await readFile(portFile, "utf8")).trim();
    if (port) cdpArgs = ["--cdp-port", port];
  } catch {}

  const child = spawn(process.execPath, [getDaemonPath(), ...cdpArgs], {
    detached: true, stdio: "ignore", env: { ...process.env },
  });
  child.unref();

  for (let i = 0; i < 50; i++) {
    await new Promise(r => setTimeout(r, 200));
    cachedInfo = null;
    if (await isDaemonRunning()) return;
  }
}

// ---------------------------------------------------------------------------
// Command transport
// ---------------------------------------------------------------------------

async function sendCommand(request: Request): Promise<Response> {
  await ensureDaemon();
  const info = await getDaemonInfo();
  if (!info) return { error: { message: "No daemon.json found. Is the daemon running?" } };

  try {
    const res = await fetch(`http://${info.host}:${info.port}/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${info.token}` },
      body: JSON.stringify(request),
      signal: AbortSignal.timeout(COMMAND_TIMEOUT),
    });
    if (res.status === 503) {
      return { error: { message: "Chrome not connected", hint: "Make sure Chrome is running" } };
    }
    return (await res.json()) as Response;
  } catch {
    return { error: { message: "Daemon request failed" } };
  }
}

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

function errorResult(msg: string) {
  return { content: [{ type: "text" as const, text: `Error: ${msg}` }], isError: true };
}

function textResult(value: unknown) {
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  return { content: [{ type: "text" as const, text }] };
}

// ---------------------------------------------------------------------------
// Build MCP tool schema from CommandDef params
// ---------------------------------------------------------------------------

function buildZodShape(cmd: CommandDef): Record<string, z.ZodTypeAny> {
  const shape: Record<string, z.ZodTypeAny> = {};
  for (const [name, def] of Object.entries(cmd.params)) {
    let field: z.ZodTypeAny;
    if (def.type === "number") field = z.number().describe(def.description);
    else if (def.type === "boolean") field = z.boolean().describe(def.description);
    else field = z.string().describe(def.description);
    shape[name] = def.required ? field : field.optional();
  }
  if (!shape.tab) {
    shape.tab = z.string().optional().describe("Tab short ID to target");
  }
  return shape;
}

// ---------------------------------------------------------------------------
// Build Request from MCP tool args
// ---------------------------------------------------------------------------

function buildRequest(cmd: CommandDef, args: Record<string, unknown>): Request {
  const { tab, ...rest } = args;
  return {
    method: cmd.method,
    ...rest,
    ...(tab !== undefined ? { tabId: tab } : {}),
  } as Request;
}

// ---------------------------------------------------------------------------
// Special handlers for commands that need result processing
// ---------------------------------------------------------------------------

type ToolResult = { content: Array<{ type: "text"; text: string } | { type: "image"; data: string; mimeType: string }>; isError?: boolean };
type ToolHandler = (args: Record<string, unknown>) => Promise<ToolResult>;

function makeSpecialHandler(cmd: CommandDef): ToolHandler | null {
  switch (cmd.method) {
    case "snap":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        return textResult(resp.result?.snapshotData?.snapshot || "(empty)");
      };

    case "screenshot":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        const dataUrl = resp.result?.dataUrl;
        if (typeof dataUrl !== "string") return errorResult("Screenshot data missing");
        return {
          content: [{ type: "image" as const, data: dataUrl.replace(/^data:image\/png;base64,/, ""), mimeType: "image/png" }],
        };
      };

    case "eval":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        return textResult(resp.result?.result ?? null);
      };

    case "get":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        return textResult(resp.result?.value ?? "");
      };

    case "tab_list":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        return textResult(resp.result?.tabs || []);
      };

    case "open":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        if (args.tab === undefined) trackTab((resp.result as Record<string, unknown>)?.tabId);
        return textResult(resp.result || `Opened ${args.url}`);
      };

    case "tab_new":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        trackTab((resp.result as Record<string, unknown>)?.tabId);
        return textResult(resp.result || "Opened new tab");
      };

    case "close":
      return async (args) => {
        const method = args.tab === undefined ? "close" : "tab_close";
        const { tab, ...rest } = args;
        const req = { method, ...rest, ...(tab !== undefined ? { tabId: tab } : {}) } as Request;
        const resp = await sendCommand(req);
        if (resp.error) return errorResult(resp.error.message);
        untrackTab(args.tab);
        return textResult(resp.result || "Closed tab");
      };

    case "network":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        const nc = args.networkCommand as string | undefined;
        if (nc === "requests" || nc === undefined) {
          const data = resp.result as Record<string, unknown>;
          return textResult({ requests: data?.networkRequests || data?.requests || [], cursor: data?.cursor });
        }
        return textResult(resp.result || "Done");
      };

    case "console":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        const cc = args.consoleCommand as string | undefined;
        if (cc === "get" || cc === undefined) {
          const data = resp.result as Record<string, unknown>;
          return textResult({ messages: data?.consoleMessages || data?.messages || [], cursor: data?.cursor });
        }
        return textResult(resp.result || "Cleared");
      };

    case "errors":
      return async (args) => {
        const resp = await sendCommand(buildRequest(cmd, args));
        if (resp.error) return errorResult(resp.error.message);
        const ec = args.errorsCommand as string | undefined;
        if (ec === "get" || ec === undefined) {
          const data = resp.result as Record<string, unknown>;
          return textResult({ errors: data?.jsErrors || data?.errors || [], cursor: data?.cursor });
        }
        return textResult(resp.result || "Cleared");
      };

    default:
      return null;
  }
}

// ---------------------------------------------------------------------------
// Site CLI helpers
// ---------------------------------------------------------------------------

function runSiteCli(args: string[]): Promise<unknown> {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, [getCliPath(), "site", ...args], {
      encoding: "utf8", timeout: COMMAND_TIMEOUT, maxBuffer: 10 * 1024 * 1024,
    }, (error, stdout, stderr) => {
      const trimmed = stdout.trim();
      let parsed: unknown = null;
      try { parsed = JSON.parse(trimmed); } catch {}

      if (parsed && typeof parsed === "object" && "success" in (parsed as Record<string, unknown>) && (parsed as Record<string, unknown>).success === false) {
        const p = parsed as Record<string, unknown>;
        reject(new Error((p.error as string) || stderr.trim() || "Site command failed"));
        return;
      }
      if (error) {
        reject(new Error(stderr.trim() || stdout.trim() || "Site command failed"));
        return;
      }
      resolve(parsed ?? trimmed);
    });
  });
}

// ---------------------------------------------------------------------------
// MCP Server
// ---------------------------------------------------------------------------

const server = new McpServer(
  { name: "bb-browser", version: typeof __BB_BROWSER_VERSION__ !== "undefined" ? __BB_BROWSER_VERSION__ : "0.0.0" },
  { instructions: `bb-browser lets you control the user's real Chrome browser via CDP.

Key tools:
- browser_snap: Read page via accessibility tree (ref numbers for interaction)
- browser_click/fill/type: Interact with elements by @ref
- browser_eval: Run JavaScript in page context
- browser_network/console/errors: Observe browser events (supports since: "last_action")
- browser_screenshot: Visual capture
- browser_tab_list/tab_new: Multi-tab support (use tab parameter for concurrent ops)
- browser_close_all: Close tabs opened during this MCP session
- site_*: Pre-built adapters for 36+ platforms

Tab IDs are short hex strings (e.g. "c416"). Pass tab to any tool to target a specific tab.` },
);

// Register browser commands from unified COMMANDS registry
for (const cmd of COMMANDS) {
  if (cmd.group === "site") continue;

  const toolName = `browser_${cmd.method}`;
  const shape = buildZodShape(cmd);
  const special = makeSpecialHandler(cmd);

  if (special) {
    server.tool(toolName, cmd.description, shape, special);
  } else {
    server.tool(toolName, cmd.description, shape, async (args: Record<string, unknown>) => {
      const resp = await sendCommand(buildRequest(cmd, args));
      if (resp.error) return errorResult(resp.error.message);
      return textResult(resp.result || "Done");
    });
  }
}

// browser_close_all — session-scoped
server.tool("browser_close_all", "Close tabs opened during this MCP session", {}, async () => {
  const closed: string[] = [];
  const failed: Array<{ tabId: string; error: string }> = [];
  for (const tabId of Array.from(sessionOpenedTabs)) {
    const resp = await sendCommand({ method: "tab_close", tabId } as unknown as Request);
    sessionOpenedTabs.delete(tabId);
    if (resp.error && !/tab not found/i.test(resp.error.message)) {
      failed.push({ tabId, error: resp.error.message });
    } else {
      closed.push(tabId);
    }
  }
  return textResult({ closed, failed });
});

// Site tools — route through CLI
server.tool("site_list", "List installed site adapters", {}, async () => {
  try { return textResult(await runSiteCli(["list", "--json"])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("site_search", "Search site adapters", {
  query: z.string().describe("Search query"),
}, async ({ query }) => {
  try { return textResult(await runSiteCli(["search", query, "--json"])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("site_info", "Get adapter metadata", {
  name: z.string().describe("Adapter name, e.g. twitter/search"),
}, async ({ name }) => {
  try { return textResult(await runSiteCli(["info", name, "--json"])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("site_run", "Run a site adapter", {
  name: z.string().describe("Adapter name, e.g. twitter/search"),
  args: z.array(z.string()).optional().describe("Positional arguments"),
  namedArgs: z.record(z.string()).optional().describe("Named arguments as --key value"),
  tab: z.string().optional().describe("Tab short ID"),
}, async ({ name, args: posArgs, namedArgs, tab }) => {
  try {
    const cliArgs = ["run", name];
    for (const a of posArgs || []) cliArgs.push(a);
    for (const [k, v] of Object.entries(namedArgs || {})) cliArgs.push(`--${k}`, v);
    if (tab) cliArgs.push("--tab", tab);
    cliArgs.push("--json");
    const result = await runSiteCli(cliArgs);
    const unwrapped = result && typeof result === "object" && "data" in (result as Record<string, unknown>) ? (result as Record<string, unknown>).data : result;
    return textResult(unwrapped);
  } catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("site_update", "Update community adapter repository", {}, async () => {
  try { return textResult(await runSiteCli(["update", "--json"])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

// ---------------------------------------------------------------------------
// Instance management tools
// ---------------------------------------------------------------------------

function runInstanceCli(args: string[]): Promise<unknown> {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, [getCliPath(), "instance", ...args, "--json"], {
      encoding: "utf8", timeout: 15000,
    }, (error, stdout, stderr) => {
      const trimmed = stdout.trim();
      let parsed: unknown = null;
      try { parsed = JSON.parse(trimmed); } catch {}
      if (error) {
        reject(new Error(stderr.trim() || stdout.trim() || "Instance command failed"));
        return;
      }
      resolve(parsed ?? trimmed);
    });
  });
}

server.tool("instance_list", "List all browser instances (each has independent Chrome profile and login state)", {}, async () => {
  try { return textResult(await runInstanceCli(["list"])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("instance_stop", "Stop a running browser instance's daemon", {
  id: z.string().describe("Instance ID to stop"),
}, async ({ id }) => {
  try { return textResult(await runInstanceCli(["stop", id])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("instance_rename", "Rename a browser instance (stops daemon if running, moves Chrome profile)", {
  oldId: z.string().describe("Current instance ID"),
  newId: z.string().describe("New instance ID"),
}, async ({ oldId, newId }) => {
  try { return textResult(await runInstanceCli(["rename", oldId, newId])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

server.tool("instance_delete", "Delete a browser instance and its Chrome profile (irreversible)", {
  id: z.string().describe("Instance ID to delete"),
}, async ({ id }) => {
  try { return textResult(await runInstanceCli(["delete", id])); }
  catch (e) { return errorResult(e instanceof Error ? e.message : String(e)); }
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const transport = new StdioServerTransport();
server.connect(transport).catch((error) => {
  console.error(error);
  process.exit(1);
});

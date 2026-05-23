/**
 * instance 命令 — 管理浏览器实例（独立 Chrome profile + 登录态）
 *
 * 用法：
 *   bb-browser instance list                列出所有实例
 *   bb-browser instance stop <id>           停止实例的 daemon
 *   bb-browser instance rename <old> <new>  重命名实例
 *   bb-browser instance delete <id>         删除实例（含 Chrome profile）
 */

import { BB_BROWSER_ROOT, getInstanceId, getInstanceDir, isProcessAlive } from "@bb-browser/shared";
import { readdirSync, readFileSync, existsSync, renameSync, rmSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { request as httpRequest } from "node:http";

// ---------------------------------------------------------------------------
// Instance discovery
// ---------------------------------------------------------------------------

export interface InstanceInfo {
  id: string;
  running: boolean;
  pid?: number;
  port?: number;
  cdpPort?: number;
  hasProfile: boolean;
}

function probeInstance(id: string, dir: string): InstanceInfo {
  const info: InstanceInfo = { id, running: false, hasProfile: false };

  const daemonJson = join(dir, "daemon.json");
  try {
    const raw = JSON.parse(readFileSync(daemonJson, "utf8"));
    if (typeof raw.pid === "number" && isProcessAlive(raw.pid)) {
      info.running = true;
      info.pid = raw.pid;
      info.port = raw.port;
      info.cdpPort = raw.cdpPort;
    }
  } catch {}

  info.hasProfile = existsSync(join(dir, "browser", "user-data"));
  return info;
}

export function discoverInstances(): InstanceInfo[] {
  const instances: InstanceInfo[] = [];

  instances.push(probeInstance("default", BB_BROWSER_ROOT));

  const instancesDir = join(BB_BROWSER_ROOT, "instances");
  if (existsSync(instancesDir)) {
    try {
      for (const entry of readdirSync(instancesDir, { withFileTypes: true })) {
        if (entry.isDirectory() && !entry.name.startsWith(".")) {
          instances.push(probeInstance(entry.name, join(instancesDir, entry.name)));
        }
      }
    } catch {}
  }

  return instances.filter(i => i.running || i.hasProfile);
}

// ---------------------------------------------------------------------------
// Stop a running instance
// ---------------------------------------------------------------------------

async function stopInstance(id: string): Promise<boolean> {
  const dir = getInstanceDir(id);
  const daemonJson = join(dir, "daemon.json");

  let info: { pid: number; host: string; port: number; token: string };
  try {
    info = JSON.parse(readFileSync(daemonJson, "utf8"));
  } catch {
    return false;
  }

  if (!isProcessAlive(info.pid)) {
    try { unlinkSync(daemonJson); } catch {}
    return false;
  }

  // Graceful shutdown via HTTP
  try {
    await new Promise<void>((resolve) => {
      const req = httpRequest({
        hostname: info.host, port: info.port, path: "/shutdown", method: "POST",
        headers: { Authorization: `Bearer ${info.token}` }, timeout: 3000,
      }, (res) => { res.resume(); res.on("end", () => resolve()); });
      req.on("error", () => resolve());
      req.on("timeout", () => { req.destroy(); resolve(); });
      req.end();
    });
  } catch {}

  // Wait for exit
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (!isProcessAlive(info.pid)) return true;
    await new Promise(r => setTimeout(r, 200));
  }

  // Force kill
  try { process.kill(info.pid, "SIGKILL"); } catch {}
  try { unlinkSync(daemonJson); } catch {}
  return true;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

interface InstanceOptions {
  json?: boolean;
}

export async function instanceListCommand(options: InstanceOptions = {}): Promise<void> {
  const instances = discoverInstances();
  const current = getInstanceId();

  if (options.json) {
    console.log(JSON.stringify(instances, null, 2));
    return;
  }

  if (instances.length === 0) {
    console.log("No instances found");
    return;
  }

  console.log("Instances:\n");
  for (const inst of instances) {
    const marker = inst.id === current ? " (current)" : "";
    const status = inst.running
      ? `running (pid:${inst.pid} port:${inst.port} cdp:${inst.cdpPort})`
      : "stopped";
    const profile = inst.hasProfile ? "profile: yes" : "profile: no";
    console.log(`  ${inst.id}${marker}`);
    console.log(`    ${status}, ${profile}`);
  }
  console.log(`\nUse: bb-browser --instance <id> <command>`);
}

export async function instanceStopCommand(id: string, options: InstanceOptions = {}): Promise<void> {
  if (!id) {
    console.error("Usage: bb-browser instance stop <id>");
    process.exit(1);
  }

  const stopped = await stopInstance(id);
  if (options.json) {
    console.log(JSON.stringify({ id, stopped }));
  } else {
    console.log(stopped ? `Instance "${id}" stopped` : `Instance "${id}" was not running`);
  }
}

export async function instanceRenameCommand(oldId: string, newId: string, options: InstanceOptions = {}): Promise<void> {
  if (!oldId || !newId) {
    console.error("Usage: bb-browser instance rename <old-id> <new-id>");
    process.exit(1);
  }

  if (oldId === "default") {
    console.error('Cannot rename the "default" instance');
    process.exit(1);
  }

  if (newId === "default") {
    console.error('Cannot rename to "default" — it is reserved');
    process.exit(1);
  }

  const oldDir = getInstanceDir(oldId);
  const newDir = getInstanceDir(newId);

  if (!existsSync(oldDir)) {
    console.error(`Instance "${oldId}" not found`);
    process.exit(1);
  }

  if (existsSync(newDir)) {
    console.error(`Instance "${newId}" already exists`);
    process.exit(1);
  }

  // Stop daemon first if running
  const probe = probeInstance(oldId, oldDir);
  if (probe.running) {
    await stopInstance(oldId);
  }

  renameSync(oldDir, newDir);

  if (options.json) {
    console.log(JSON.stringify({ oldId, newId, renamed: true }));
  } else {
    console.log(`Renamed "${oldId}" → "${newId}"`);
  }
}

export async function instanceDeleteCommand(id: string, options: InstanceOptions = {}): Promise<void> {
  if (!id) {
    console.error("Usage: bb-browser instance delete <id>");
    process.exit(1);
  }

  if (id === "default") {
    console.error('Cannot delete the "default" instance. Use bb-browser daemon stop instead.');
    process.exit(1);
  }

  const dir = getInstanceDir(id);
  if (!existsSync(dir)) {
    console.error(`Instance "${id}" not found`);
    process.exit(1);
  }

  // Stop daemon first
  await stopInstance(id);

  rmSync(dir, { recursive: true, force: true });

  if (options.json) {
    console.log(JSON.stringify({ id, deleted: true }));
  } else {
    console.log(`Instance "${id}" deleted (Chrome profile and all data removed)`);
  }
}

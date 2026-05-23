import { ensureDaemon, getDaemonStatus, stopDaemon } from "../daemon-manager.js";
import { BB_BROWSER_ROOT, getInstanceId, readDaemonJson, isProcessAlive } from "@bb-browser/shared";
import { readdirSync, readFileSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";

export interface DaemonOptions {
  json?: boolean;
}

export async function statusCommand(
  options: DaemonOptions = {}
): Promise<void> {
  const status = await getDaemonStatus();

  if (!status) {
    if (options.json) {
      console.log(JSON.stringify({ running: false }));
    } else {
      console.log("Daemon not running");
      console.log("\n\u{1F4A1} 启动: bb-browser daemon start");
    }
    return;
  }

  if (options.json) {
    console.log(JSON.stringify(status, null, 2));
    return;
  }

  // Human-readable output
  console.log(`Daemon running: ${status.running ? "yes" : "no"}`);
  console.log(`CDP connected:  ${status.cdpConnected ? "yes" : "no"}`);
  console.log(`Uptime:         ${formatUptime(status.uptime as number)}`);
  console.log(`Global seq:     ${status.currentSeq ?? "N/A"}`);

  const tabs = status.tabs as Array<{
    shortId: string;
    targetId: string;
    networkRequests: number;
    consoleMessages: number;
    jsErrors: number;
    lastActionSeq: number;
  }> | undefined;

  if (tabs && tabs.length > 0) {
    console.log(`\nTabs (${tabs.length}):`);
    for (const tab of tabs) {
      const active = tab.targetId === status.currentTargetId ? " *" : "";
      console.log(
        `  ${tab.shortId}${active}  net:${tab.networkRequests} console:${tab.consoleMessages} err:${tab.jsErrors} seq:${tab.lastActionSeq}`
      );
    }
  } else {
    console.log("\nNo tabs");
  }

  if (status.cdpConnected === false) {
    console.log("\n⚠️ Chrome 未连接。运行 bb-browser daemon stop && bb-browser tab list 重新启动");
  } else {
    console.log("\n\u{1F4A1} 停止: bb-browser daemon stop");
  }
}

export async function startCommand(
  options: DaemonOptions = {}
): Promise<void> {
  await ensureDaemon();
  const status = await getDaemonStatus();
  if (options.json) {
    console.log(JSON.stringify(status, null, 2));
  } else {
    console.log("Daemon started");
    if (status) {
      console.log(`CDP connected:  ${status.cdpConnected ? "yes" : "no"}`);
      const tabs = status.tabs as Array<{ shortId: string }> | undefined;
      console.log(`Tabs:           ${tabs?.length ?? 0}`);
    }
  }
}

export async function shutdownCommand(
  options: DaemonOptions = {}
): Promise<void> {
  const ok = await stopDaemon();
  if (options.json) {
    console.log(JSON.stringify({ stopped: ok }));
  } else {
    console.log(ok ? "Daemon stopped" : "Daemon was not running");
  }
}

interface InstanceInfo {
  id: string;
  running: boolean;
  pid?: number;
  port?: number;
  cdpPort?: number;
  hasProfile: boolean;
}

function discoverInstances(): InstanceInfo[] {
  const instances: InstanceInfo[] = [];

  // Check "default" instance (root dir)
  instances.push(probeInstance("default", BB_BROWSER_ROOT));

  // Check instances/ subdirectory
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

function probeInstance(id: string, dir: string): InstanceInfo {
  const info: InstanceInfo = { id, running: false, hasProfile: false };

  // Check daemon.json
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

  // Check Chrome profile exists
  const profileDir = join(dir, "browser", "user-data");
  info.hasProfile = existsSync(profileDir);

  return info;
}

export async function instancesCommand(
  options: DaemonOptions = {}
): Promise<void> {
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
    const status = inst.running ? `running (pid:${inst.pid} port:${inst.port} cdp:${inst.cdpPort})` : "stopped";
    const profile = inst.hasProfile ? "profile: yes" : "profile: no";
    console.log(`  ${inst.id}${marker}`);
    console.log(`    ${status}, ${profile}`);
  }
  console.log(`\nUse: bb-browser --instance <id> <command>`);
}

function formatUptime(ms: number): string {
  if (!ms || ms <= 0) return "0s";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

import fs from "fs/promises";
import fsSync from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { AsyncLocalStorage } from "node:async_hooks";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const LOG_DIR = path.join(__dirname, "logs");
export const COMMAND_LOG_FILE = path.join(LOG_DIR, "actual-commands.log");
export const PID_LOG_FILE = path.join(LOG_DIR, `${process.pid}.log`);

export const authStorage = new AsyncLocalStorage();

export function getLocalTimestamp() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

export function formatLogEntry(msg, clientId = null) {
  const timestamp = getLocalTimestamp();
  const authTag = clientId ? `[${clientId}]` : "";
  const prefix = `[${timestamp}]${authTag}`;
  const text = typeof msg === "string" ? msg.replace(/\r\n/g, "\n").trimEnd() : String(msg);
  if (text.includes("\n")) {
    return `${prefix}\n${text}\n`;
  }
  return `${prefix} ${text}\n`;
}

export async function rotateLogIfNeeded(logPath, maxLines = 1000) {
  try {
    if (!fsSync.existsSync(logPath)) return;
    const content = await fs.readFile(logPath, "utf8");
    let lineCount = 0;
    for (let i = 0; i < content.length; i++) {
      if (content[i] === "\n") lineCount++;
    }
    if (lineCount >= maxLines) {
      const logDir = path.dirname(logPath);
      const logFile = path.basename(logPath);
      const files = await fs.readdir(logDir);
      let maxIdx = 0;
      for (const file of files) {
        if (file.startsWith(logFile + ".")) {
          const ext = file.substring(logFile.length + 1);
          const num = parseInt(ext, 10);
          if (!isNaN(num) && num > maxIdx) {
            maxIdx = num;
          }
        }
      }
      for (let i = maxIdx; i >= 1; i--) {
        const oldLog = `${logPath}.${i}`;
        const newLog = `${logPath}.${i + 1}`;
        if (fsSync.existsSync(oldLog)) {
          await fs.rename(oldLog, newLog);
        }
      }
      await fs.rename(logPath, `${logPath}.1`);
    }
  } catch (e) {
    console.error(`Error rotating log: ${e.message}`);
  }
}

async function appendToFile(filePath, content) {
  try {
    if (!fsSync.existsSync(LOG_DIR)) {
      await fs.mkdir(LOG_DIR, { recursive: true });
    }
    await rotateLogIfNeeded(filePath);
    await fs.appendFile(filePath, content);
  } catch (e) {
    console.error(`Failed to write to log file (${filePath}): ${e.message}`);
  }
}

/**
 * Log commands to logs/actual-commands.log (only commands, no timing/system logs)
 */
export async function logCommand(cmd, explicitClientId = null) {
  const store = authStorage.getStore();
  const clientId = explicitClientId !== null ? explicitClientId : (store?.clientId || null);
  const line = formatLogEntry(cmd, clientId);
  await appendToFile(COMMAND_LOG_FILE, line);
}

/**
 * Log all other system, error, and status messages to logs/[pid].log
 */
export async function logMessage(msg, explicitClientId = null) {
  const store = authStorage.getStore();
  const clientId = explicitClientId !== null ? explicitClientId : (store?.clientId || null);
  const line = formatLogEntry(msg, clientId);
  await appendToFile(PID_LOG_FILE, line);
}

#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { 
  CallToolRequestSchema, ListToolsRequestSchema,
  ListResourcesRequestSchema, ReadResourceRequestSchema,
  ListPromptsRequestSchema, GetPromptRequestSchema
} from "@modelcontextprotocol/sdk/types.js";
import { spawn } from "child_process";
import fs from "fs/promises";
import fsSync from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const LOG_DIR = path.join(os.homedir(), 'Projects', 'actual-report', 'logs');
const LOG_FILE = path.join(LOG_DIR, 'actual-commands.log');
const LOCK_DIR = path.join(LOG_DIR, '.actual.lock');
const LOCK_STALE_SECONDS = 3600;
const ACTUAL_RC_PATH = path.join(os.homedir(), '.actualrc.json');

const RETRYABLE_ERRORS = /SQLITE_BUSY|database is locked|SQLITE_IOERR|getSyncError/i;

// Ensure PATH includes the node version used in the wrapper
const extendedEnv = { ...process.env };
extendedEnv.PATH = `/usr/local/bin:${process.env.PATH || ''}`;

const server = new Server({
  name: "actual-cli-mcp",
  version: "1.1.0"
}, {
  capabilities: {
    tools: {},
    resources: {},
    prompts: {}
  }
});

// Helper: Logging
async function logMessage(msg) {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const line = `${timestamp} - ${msg}\n`;
  try {
    if (!fsSync.existsSync(LOG_DIR)) {
      await fs.mkdir(LOG_DIR, { recursive: true });
    }
    await fs.appendFile(LOG_FILE, line);
  } catch (e) {
    console.error(`Failed to write to log file: ${e.message}`);
  }
}

// Helper: Locking
async function acquireLock() {
  if (process.env._ACTUAL_NOLOCK === '1') return true;
  
  while (true) {
    try {
      if (!fsSync.existsSync(LOG_DIR)) {
        await fs.mkdir(LOG_DIR, { recursive: true });
      }
      await fs.mkdir(LOCK_DIR);
      // Success, lock acquired
      const info = `${process.pid}:${Math.floor(Date.now() / 1000)}`;
      await fs.writeFile(path.join(LOCK_DIR, 'info'), info);
      return true;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      
      // Lock exists, inspect it
      try {
        const infoStr = await fs.readFile(path.join(LOCK_DIR, 'info'), 'utf8');
        const [lockPid, lockTime] = infoStr.split(':');
        const age = Math.floor(Date.now() / 1000) - parseInt(lockTime, 10);
        
        if (age >= LOCK_STALE_SECONDS) {
          await logMessage(`Removing stale lock (PID ${lockPid}, age ${age}s)`);
          await fs.rm(LOCK_DIR, { recursive: true, force: true });
          continue;
        }
        
        // Check if process is dead
        let isDead = false;
        if (lockPid) {
          try {
            process.kill(parseInt(lockPid, 10), 0);
          } catch (e) {
            if (e.code === 'ESRCH') isDead = true;
          }
        }
        
        if (isDead) {
          await logMessage(`Removing orphaned lock (PID ${lockPid} no longer running)`);
          await fs.rm(LOCK_DIR, { recursive: true, force: true });
          continue;
        }
      } catch (e) {
        // info file might not exist yet due to brief race condition
      }
      
      // Wait before retrying
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function releaseLock() {
  try {
    await fs.rm(LOCK_DIR, { recursive: true, force: true });
  } catch (e) {
    // Ignore errors
  }
}

// Helper: Read ActualRC
async function readActualRc() {
  try {
    const data = await fs.readFile(ACTUAL_RC_PATH, 'utf8');
    return JSON.parse(data);
  } catch (e) {
    return {};
  }
}

// Helper: Cache Workaround
async function runCacheWorkaround(rc) {
  if (rc.encryptionPassword && rc.dataDir && rc.syncId) {
    const cacheState = path.join(rc.dataDir, '.actual-cli', rc.syncId, 'state.json');
    try {
      await fs.unlink(cacheState);
    } catch (e) {
      // Ignore if not exists
    }
  }
}

// Helper: Spawn Command
function spawnCommand(args) {
  return new Promise((resolve) => {
    const child = spawn('actual', args, { cwd: os.homedir(), env: extendedEnv });
    let stdout = '';
    let stderr = '';
    
    child.stdout.on('data', (data) => { stdout += data.toString(); });
    child.stderr.on('data', (data) => { stderr += data.toString(); });
    
    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
    
    child.on('error', (err) => {
      resolve({ code: -1, stdout: '', stderr: err.message });
    });
  });
}

// Helper: Reset budget cache
async function resetBudgetCache() {
  await logMessage('ACTUAL BUDGET CLI RESET');
  const rc = await readActualRc();
  if (!rc.dataDir) {
    throw new Error(`Error: dataDir not found in ${ACTUAL_RC_PATH}`);
  }
  
  // Find and remove budget directories
  try {
    const files = await fs.readdir(rc.dataDir, { withFileTypes: true });
    for (const file of files) {
      if (file.isDirectory()) {
        const budgetDir = path.join(rc.dataDir, file.name);
        try {
          const dbPath = path.join(budgetDir, 'db.sqlite');
          await fs.access(dbPath); // Check if db.sqlite exists
          await logMessage(`Removing budget cache directory: ${budgetDir}`);
          await fs.rm(budgetDir, { recursive: true, force: true });
        } catch (e) {
          // No db.sqlite or permission error, skip
        }
      }
    }
  } catch (e) {
    // Directory might not exist yet
  }
  
  if (rc.syncId) {
    await logMessage(`Downloading budget ${rc.syncId}...`);
    const args = ['budgets', 'download', rc.syncId];
    if (rc.encryptionPassword) {
      args.push('--encryption-password', rc.encryptionPassword);
    }
    await spawnCommand(args);
  }
  
  await logMessage('Refetching accounts...');
  await spawnCommand(['accounts', 'list']);
}

// Main execution block with retries
async function executeWithRetry(args) {
  await acquireLock();
  try {
    await logMessage(`actual ${args.join(' ')}`);
    const rc = await readActualRc();
    await runCacheWorkaround(rc);
    
    const maxRetries = parseInt(process.env.ACTUAL_MAX_RETRIES || '1', 10);
    let attempt = 0;
    
    while (true) {
      const { code, stdout, stderr } = await spawnCommand(args);
      
      if (code !== 0 && RETRYABLE_ERRORS.test(stderr)) {
        attempt++;
        if (attempt <= maxRetries) {
          await logMessage(`SQLite error detected, resetting and retrying (attempt ${attempt}/${maxRetries})...`);
          await resetBudgetCache();
          await new Promise(r => setTimeout(r, 2000));
          continue;
        } else {
          await logMessage(`SQLite error persisted after ${maxRetries} retry attempt(s), giving up`);
          return { code, stdout, stderr };
        }
      }
      
      return { code, stdout, stderr };
    }
  } finally {
    await releaseLock();
  }
}

const COMMAND_SCHEMAS = {
  accounts: {
    desc: "Manage accounts.",
    details: "Subcommands:\n" +
             "- list [--include-closed] [--format json|table|csv]\n" +
             "- create --name <name> [--offbudget] [--balance <cents>]\n" +
             "- update <id> [--name <name>] [--offbudget <bool>]\n" +
             "- close <id> [--transfer-account <id>] [--transfer-category <id>]\n" +
             "- reopen <id>\n" +
             "- delete <id>\n" +
             "- balance <id> [--cutoff <YYYY-MM-DD>]",
    rules: "Rules:\n- Accounts must be referenced by their UUID.\n- Use `balance` with an account ID to get its current balance."
  },
  budgets: {
    desc: "Manage budgets.",
    details: "Subcommands:\n" +
             "- list\n" +
             "- download <syncId> [--encryption-password <pw>]\n" +
             "- months\n" +
             "- month <YYYY-MM>\n" +
             "- set-amount --month <YYYY-MM> --category <id> --amount <cents>\n" +
             "- set-carryover --month <YYYY-MM> --category <id> --flag <bool>\n" +
             "- hold-next-month --month <YYYY-MM> --amount <cents>\n" +
             "- reset-hold --month <YYYY-MM>",
    rules: "Rules:\n- Budgets list does not require syncId.\n- `--month` must be formatted as YYYY-MM."
  },
  transactions: {
    desc: "Manage transactions.",
    details: "Subcommands:\n" +
             "- list --account <id> --start <YYYY-MM-DD> --end <YYYY-MM-DD>\n" +
             "- add --account <id> (--data <json> | --file <path>) [--learn-categories] [--run-transfers]\n" +
             "- import --account <id> (--data <json> | --file <path>) [--dry-run]\n" +
             "- update <id> (--data <json> | --file <path>)\n" +
             "- delete <id>",
    rules: "Rules:\n- list: ALL of `--account`, `--start`, and `--end` are absolutely required or the command will fail.\n- add/import: MUST provide `--account` and either `--data` (as JSON string) or `--file`.\n- update: Takes the transaction ID as a positional argument. The `--data` flag must contain fields to update."
  },
  categories: {
    desc: "Manage categories.",
    details: "Subcommands:\n" +
             "- list [--include-hidden]\n" +
             "- create --name <name> --group-id <id> [--is-income]\n" +
             "- update <id> [--name <name>] [--hidden <bool>]\n" +
             "- delete <id> [--transfer-to <id>]",
    rules: "Rules:\n- create MUST have both `--name` and `--group-id`.\n- update MUST have at least one flag to change."
  },
  'category-groups': {
    desc: "Manage category groups.",
    details: "Subcommands:\n" +
             "- list [--include-hidden]\n" +
             "- create --name <name> [--is-income]\n" +
             "- update <id> [--name <name>] [--hidden <bool>]\n" +
             "- delete <id> [--transfer-to <id>]",
    rules: "Rules:\n- list output will include categories nested inside the group if standard format is used."
  },
  payees: {
    desc: "Manage payees.",
    details: "Subcommands:\n" +
             "- list\n" +
             "- common\n" +
             "- create --name <name>\n" +
             "- update <id> --name <name>\n" +
             "- delete <id>\n" +
             "- merge --target <id> --ids <id1,id2,id3>",
    rules: "Rules:\n- merge requires `--target` (the final ID) and `--ids` (comma separated list to merge)."
  },
  tags: {
    desc: "Manage tags.",
    details: "Subcommands:\n" +
             "- list\n" +
             "- create --tag <name> [--color <color>] [--description <text>]\n" +
             "- update <id> [--tag <name>] [--color <color>] [--description <text>]\n" +
             "- delete <id>",
    rules: "Rules:\n- `--tag` is used for the name instead of `--name`."
  },
  rules: {
    desc: "Manage transaction rules.",
    details: "Subcommands:\n" +
             "- list\n" +
             "- payee-rules <payeeId>\n" +
             "- create (--data <json> | --file <path>)\n" +
             "- update (--data <json> | --file <path>)\n" +
             "- delete <id>",
    rules: "Rules:\n- update: CRITICAL NOTE: unlike other commands, `update` does NOT take a positional `<id>` argument. The `id` must be provided inside the JSON payload."
  },
  schedules: {
    desc: "Manage scheduled transactions.",
    details: "Subcommands:\n" +
             "- list\n" +
             "- create (--data <json> | --file <path>)\n" +
             "- update <id> (--data <json> | --file <path>) [--reset-next-date]\n" +
             "- delete <id>",
    rules: "Rules:\n- create and update use JSON via `--data` for configuration."
  },
  sync: {
    desc: "Sync the local cached budget with the server.",
    details: "Flags:\n" +
             "- (none) Syncs the local cached budget with the server\n" +
             "- --status Prints cache status\n" +
             "- --clear Deletes the local cache state file",
    rules: "Rules:\n- Call with no arguments to force a sync."
  },
  query: {
    desc: "Run ActualQL queries.",
    details: "Subcommands:\n" +
             "- tables\n" +
             "- fields <table>\n" +
             "- run [--table <table>] [--select <fields>] [--filter <json>] [--order-by <fields>] [--limit <n>] [--count] [--file <path>]",
    rules: "Rules:\n- Use `--filter` with ActualQL JSON syntax (e.g. `'{\"amount\": {\"$lt\": 0}}'`).\n- `--table` is usually required unless using `--file` or `--last`."
  },
  server: {
    desc: "Server utilities.",
    details: "Subcommands:\n" +
             "- version\n" +
             "- get-id --type <type> --name <name>\n" +
             "- bank-sync [--account <id>]",
    rules: "Rules:\n- `get-id` requires `--type` (e.g. 'accounts') and `--name`."
  }
};

const CLI_COMMANDS = Object.keys(COMMAND_SCHEMAS);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  const tools = Object.entries(COMMAND_SCHEMAS).map(([cmd, schema]) => ({
    name: `actual_${cmd.replace(/-/g, '_')}`,
    description: `Execute 'actual ${cmd}' command. ${schema.desc}\n\n${schema.details}\n\n${schema.rules}\n\nGlobal flags like --format json, --verbose are also supported. Amounts are in integer cents (e.g. 5000 = $50.00).`,
    inputSchema: {
      type: "object",
      properties: {
        args: {
          type: "array",
          items: { type: "string" },
          description: `The list of arguments/flags to pass to 'actual ${cmd}'. E.g., ["list", "--format", "json"] or ["create", "--name", "My Account"]`
        }
      },
      required: ["args"]
    }
  }));

  // Add the generic actual_execute tool back just in case
  tools.push({
    name: "actual_execute",
    description: "Execute a generic command using the Actual Budget CLI. E.g., args: ['help'].",
    inputSchema: {
      type: "object",
      properties: {
        args: {
          type: "array",
          items: { type: "string" },
          description: "The list of arguments to pass to the CLI. Do not include the 'actual' executable name."
        }
      },
      required: ["args"]
    }
  });

  return { tools };
});

// Expose SKILL.md as an MCP Resource
server.setRequestHandler(ListResourcesRequestSchema, async () => {
  return {
    resources: [
      {
        uri: "actual://skill-docs",
        name: "Actual Budget Skill Documentation",
        description: "Comprehensive guide, CLI reference, and best practices for interacting with the Actual Budget API.",
        mimeType: "text/markdown"
      }
    ]
  };
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const uri = request.params.uri;
  if (uri === "actual://skill-docs") {
    const skillPath = '~/.gemini/antigravity/skills/actual/SKILL.md';
    try {
      const content = await fs.readFile(skillPath, 'utf8');
      return {
        contents: [
          {
            uri: "actual://skill-docs",
            mimeType: "text/markdown",
            text: content
          }
        ]
      };
    } catch (e) {
      throw new Error(`Failed to read SKILL.md: ${e.message}`);
    }
  }
  throw new Error(`Unknown resource: ${uri}`);
});

// Expose Prompts to instruct agents
server.setRequestHandler(ListPromptsRequestSchema, async () => {
  return {
    prompts: [
      {
        name: "actual_budget_guidelines",
        description: "Get instructions and best practices for interacting with Actual Budget.",
      }
    ]
  };
});

server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  if (request.params.name === "actual_budget_guidelines") {
    return {
      description: "Actual Budget Integration Guidelines",
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text: "Please read the Actual Budget Skill Documentation resource at 'actual://skill-docs' to understand how to correctly query, format, and interact with the actual budget tools. Pay special attention to the advanced learnings, ActualQL querying, and transaction linking best practices."
          }
        }
      ]
    };
  }
  throw new Error(`Unknown prompt: ${request.params.name}`);
});

// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  
  let cliArgs = args.args || [];
  
  // If a specific tool is called, prepend the command name
  if (name.startsWith('actual_') && name !== 'actual_execute') {
    const cmd = name.replace('actual_', '').replace(/_/g, '-');
    if (CLI_COMMANDS.includes(cmd)) {
      cliArgs = [cmd, ...cliArgs];
    } else {
      throw new Error(`Unknown tool: ${name}`);
    }
  } else if (name !== 'actual_execute') {
    throw new Error(`Unknown tool: ${name}`);
  }
  
  const { code, stdout, stderr } = await executeWithRetry(cliArgs);
  
  const textParts = [];
  if (stdout) {
    textParts.push(stdout);
  }
  if (stderr) {
    textParts.push(`Standard Error:\n${stderr}`);
  }
  if (code !== 0) {
    textParts.push(`Process exited with code ${code}`);
  }
  
  return {
    content: [
      {
        type: "text",
        text: textParts.join('\n\n') || "Command executed successfully with no output."
      }
    ],
    isError: code !== 0
  };
});

async function run() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Actual CLI MCP server running on stdio");
}

run().catch(console.error);

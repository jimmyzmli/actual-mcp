import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const serverPath = path.resolve(__dirname, 'index.js');
const server = spawn('node', [serverPath]);

let currentTest = null;
let resolveCurrent = null;

server.stdout.on('data', (data) => {
  const lines = data.toString().split('\n').filter(l => l.trim());
  for (const line of lines) {
    try {
      const res = JSON.parse(line);
      if (res.id && resolveCurrent) {
        resolveCurrent(res);
      }
    } catch (e) {
      // Not JSON or partial, ignore
    }
  }
});

server.stderr.on('data', (data) => {
  console.error(`Log: ${data.toString()}`);
});

async function runTest(id, name, args) {
  console.log(`\n--- Testing ${name} with args ${JSON.stringify(args)} ---`);
  const request = {
    jsonrpc: '2.0',
    id,
    method: 'tools/call',
    params: {
      name,
      arguments: { args }
    }
  };

  return new Promise((resolve) => {
    resolveCurrent = resolve;
    server.stdin.write(JSON.stringify(request) + '\n');
  }).then(res => {
    if (res.result && res.result.isError) {
      console.log(`ERROR:`);
      console.log(res.result.content[0].text);
    } else {
      console.log(`SUCCESS. Output preview:`);
      const text = res.result.content[0].text;
      console.log(text.substring(0, 200) + (text.length > 200 ? '...' : ''));
    }
  });
}

async function runAll() {
  await runTest(1, 'actual_accounts', ['list']);
  await runTest(2, 'actual_budgets', ['list']);
  await runTest(3, 'actual_categories', ['list']);
  await runTest(4, 'actual_transactions', ['list']); // should fail without args
  await runTest(5, 'actual_query', ['tables']);
  server.kill();
}

runAll();

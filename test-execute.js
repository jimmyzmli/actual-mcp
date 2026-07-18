import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const serverPath = path.resolve(__dirname, 'index.js');
const server = spawn('node', [serverPath]);

server.stdout.on('data', (data) => {
  console.log(`Received: ${data.toString()}`);
  server.kill();
});

server.stderr.on('data', (data) => {
  console.error(`Log: ${data.toString()}`);
});

const request = {
  jsonrpc: '2.0',
  id: 2,
  method: 'tools/call',
  params: {
    name: 'actual_accounts',
    arguments: {
      args: ['list']
    }
  }
};

server.stdin.write(JSON.stringify(request) + '\n');

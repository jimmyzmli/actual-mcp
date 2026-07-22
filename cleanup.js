import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';

console.log('Searching for zombie actual-mcp processes (PPID = 1)...');

exec('pgrep -P 1 -f "actual-mcp/index.js"', (error, stdout, stderr) => {
  if (error) {
    if (error.code === 1) {
      console.log('No zombie processes found.');
      return;
    }
    console.error(`Error searching for processes: ${error.message}`);
    return;
  }

  const pids = stdout.trim().split('\n').filter(Boolean);
  
  if (pids.length === 0) {
    console.log('No zombie processes found.');
    return;
  }

  console.log(`Found ${pids.length} zombie process(es): ${pids.join(', ')}`);

  let killed = 0;
  for (const pidStr of pids) {
    const pid = parseInt(pidStr, 10);
    if (!isNaN(pid)) {
      try {
        process.kill(pid, 'SIGKILL');
        console.log(`Killed PID ${pid}`);
        killed++;
        
        const dataDir = path.join(os.homedir(), '.actual-data', String(pid));
        if (fs.existsSync(dataDir)) {
          fs.rmSync(dataDir, { recursive: true, force: true });
          console.log(`Removed data directory for PID ${pid}`);
        }
      } catch (err) {
        console.error(`Failed to kill PID ${pid}: ${err.message}`);
      }
    }
  }

  console.log(`Cleanup complete. Killed ${killed} process(es).`);
});

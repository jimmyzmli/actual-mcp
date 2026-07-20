import api from '@actual-app/api';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';

const ACTUAL_RC_PATH = path.join(os.homedir(), '.actualrc.json');
const rcConfig = JSON.parse(fs.readFileSync(ACTUAL_RC_PATH, 'utf8'));

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.log('Usage: node restore.js [options] <id1> <id2> ...');
    console.log('Options:');
    console.log('  --payee      Restore payee (description) field');
    console.log('  --notes      Restore notes field');
    console.log('  --category   Restore category field');
    console.log('  --amount     Restore amount field');
    console.log('  --file <db>  Path to SQLite backup file (default: backup.sqlite)');
    console.log('\nIf no field flags are provided, all fields are restored by default.');
    process.exit(1);
  }

  let dbFile = 'backup.sqlite';
  let restorePayee = false;
  let restoreNotes = false;
  let restoreCategory = false;
  let restoreAmount = false;
  
  const ids = [];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--file') {
      dbFile = args[++i];
    } else if (arg === '--payee') {
      restorePayee = true;
    } else if (arg === '--notes') {
      restoreNotes = true;
    } else if (arg === '--category') {
      restoreCategory = true;
    } else if (arg === '--amount') {
      restoreAmount = true;
    } else if (!arg.startsWith('--')) {
      ids.push(arg);
    }
  }

  if (ids.length === 0) {
    console.log('Error: No transaction IDs provided.');
    process.exit(1);
  }

  // If no specific fields requested, restore all
  const restoreAll = !(restorePayee || restoreNotes || restoreCategory || restoreAmount);

  console.log(`Looking up ${ids.length} transaction(s) in ${dbFile}...`);
  
  const idsList = ids.map(id => `'${id}'`).join(', ');
  const query = `SELECT * FROM transactions WHERE id IN (${idsList});`;
  let output;
  try {
    output = execSync(`sqlite3 -json "${dbFile}" "${query}"`, { encoding: 'utf8' });
  } catch (e) {
    console.error(`Error querying sqlite3: ${e.message}`);
    process.exit(1);
  }
  
  let rows = [];
  try {
    if (output && output.trim()) {
      rows = JSON.parse(output);
    }
  } catch (e) {
    console.error('Failed to parse sqlite output:', e);
    process.exit(1);
  }

  // Initialize Actual API
  console.log('Connecting to Actual server...');
  const initOpts = {
    serverURL: rcConfig.serverUrl,
    dataDir: rcConfig.dataDir || path.join(os.homedir(), '.actual-data'),
  };
  if (rcConfig.password) {
    initOpts.password = rcConfig.password;
  }
  await api.init(initOpts);
  await api.downloadBudget(rcConfig.syncId, { password: rcConfig.encryptionPassword });

  let restoredCount = 0;
  for (const row of rows) {
    const apiTx = {};
    if (restoreAll) {
      if (row.acct) apiTx.account = row.acct;
      if (row.amount != null) apiTx.amount = row.amount;
      if (row.category) apiTx.category = row.category;
      if (row.description) apiTx.payee = row.description;
      apiTx.notes = row.notes || '';
      if (row.date) {
        const d = String(row.date);
        if (d.length === 8) {
          apiTx.date = `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
        }
      }
      if (row.cleared != null) apiTx.cleared = Boolean(row.cleared);
    } else {
      if (restoreAmount && row.amount != null) apiTx.amount = row.amount;
      if (restoreCategory && row.category) apiTx.category = row.category;
      if (restorePayee && row.description) apiTx.payee = row.description;
      if (restoreNotes) apiTx.notes = row.notes || '';
    }
    
    if (Object.keys(apiTx).length > 0) {
      console.log(`Restoring transaction ${row.id}`);
      await api.updateTransaction(row.id, apiTx);
      restoredCount++;
    } else {
      console.log(`Nothing to restore for transaction ${row.id}`);
    }
  }
  
  const foundIds = new Set(rows.map(r => r.id));
  for (const id of ids) {
    if (!foundIds.has(id)) {
      console.log(`Skipping ${id}: not found in backup`);
    }
  }
  
  console.log(`Successfully restored ${restoredCount} transaction(s).`);
  await api.shutdown();
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});

import api from '@actual-app/api';
import fs from 'fs';
import path from 'path';
import os from 'os';

const ACTUAL_RC_PATH = path.join(os.homedir(), '.actualrc.json');
const rcConfig = JSON.parse(fs.readFileSync(ACTUAL_RC_PATH, 'utf8'));

async function main() {
  const args = process.argv.slice(2);
  const ids = [];
  let fullMode = false;

  for (const arg of args) {
    if (arg === '--full') {
      fullMode = true;
    } else if (!arg.startsWith('-')) {
      ids.push(arg);
    }
  }
  
  if (ids.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.log('Usage: node query.js [--full] <id1> [id2] ...');
    process.exit(1);
  }

  const initOpts = {
    serverURL: rcConfig.serverUrl,
    dataDir: rcConfig.dataDir || path.join(os.homedir(), '.actual-data'),
  };
  if (rcConfig.password) {
    initOpts.password = rcConfig.password;
  }

  await api.init(initOpts);
  await api.downloadBudget(rcConfig.syncId, { password: rcConfig.encryptionPassword });

  try {
    const allTransactions = [];
    
    for (const id of ids) {
      const qObj = api.q('transactions').filter({ id }).select('*');
      
      let result;
      if (typeof api.runQuery === 'function') {
        result = await api.runQuery(qObj);
      } else if (typeof api.aqlQuery === 'function') {
        result = await api.aqlQuery(qObj);
      } else {
        throw new Error('Neither runQuery nor aqlQuery found on API');
      }
      
      if (result && result.data && result.data.length > 0) {
        allTransactions.push(...result.data);
      } else if (Array.isArray(result) && result.length > 0) {
        allTransactions.push(...result);
      }
    }
    
    if (allTransactions.length > 0) {
      const tableData = {};
      
      if (fullMode) {
        allTransactions.forEach(t => {
          const { id, ...rest } = t;
          tableData[id] = rest;
        });
      } else {
        allTransactions.forEach(t => {
          tableData[t.id] = {
            date: t.date,
            account: t.account,
            payee: t.payee,
            notes: t.notes,
            category: t.category,
            amount: t.amount
          };
        });
      }
      
      console.table(tableData);
    } else {
      console.log('No results found for provided IDs');
    }
  } catch (err) {
    console.error('Error executing query:', err);
  } finally {
    await api.shutdown();
  }
}

main().catch(err => {
  console.error('Fatal Error:', err);
  process.exit(1);
});

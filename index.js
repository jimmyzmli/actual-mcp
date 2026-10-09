import net from 'node:net';
import dns from 'node:dns';

// Disable Happy Eyeballs / network family autoselection and prefer IPv4.
// Prevents connection timeouts when remote hosts advertise AAAA records (e.g. Pikapod)
// but local routing/ISP cannot reach the IPv6 destination.
if (typeof net.setDefaultAutoSelectFamily === 'function') {
  net.setDefaultAutoSelectFamily(false);
}
if (typeof dns.setDefaultResultOrder === 'function') {
  dns.setDefaultResultOrder('ipv4first');
}

const verbose = process.argv.includes('-v') || process.argv.includes('--verbose');

// Strip the verbose flag so it doesn't leak into MCP transport parsing
process.argv = process.argv.filter(a => a !== '-v' && a !== '--verbose');

if (verbose) {
  // Redirect to stderr so debug messages are visible but stdout stays clean
  const stderrWrite = (...args) => console.error(...args);
  console.log = stderrWrite;
  console.info = stderrWrite;
  console.warn = stderrWrite;
  console.debug = stderrWrite;
} else {
  console.log = function() {};
  console.info = function() {};
  console.warn = function() {};
  console.debug = function() {};
}

import('./server.js');

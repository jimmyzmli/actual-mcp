// wrapper.js — MCP stdout must be clean JSON protocol.
// -v / --verbose: redirect console.log/info/warn/debug to stderr
// otherwise: suppress them entirely

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

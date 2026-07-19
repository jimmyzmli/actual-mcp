// wrapper.js
const originalConsoleLog = console.log;
console.log = function(...args) {
    console.error(...args);
};

import('./index.js');

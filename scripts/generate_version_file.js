const fs = require('fs');
const packageFile = require('../package.json');

fs.writeFileSync(`${process.cwd()}/dist/version.js`, `module.exports = '${packageFile.version}'`);

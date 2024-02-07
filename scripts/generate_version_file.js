const fs = require('fs');
const packageFile = require('../package.json');

fs.writeFileSync(`${process.cwd()}/src/version.ts`, `module.exports = '${packageFile.version}'`);

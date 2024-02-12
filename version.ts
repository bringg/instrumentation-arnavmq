import * as pkginfo from 'pkginfo';

export default pkginfo.find(module).version as string;

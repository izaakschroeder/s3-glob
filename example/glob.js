#!/usr/bin/env node

import yargs from 'yargs';
import S3GlobStream from '..';

const argv = yargs.argv;

/* eslint no-console: 0 */
console.log(argv._);

const stream = S3GlobStream(argv._);

stream.on('readable', function readable() {
  let entry = this.read();
  while (entry) {
    console.log(entry.Key);
    entry = this.read();
  }
}).on('error', function error(err) {
  console.log('Error:', err);
  process.exit(1);
});

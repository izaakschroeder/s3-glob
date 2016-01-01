#!/usr/bin/env node

import yargs from 'yargs';
import S3GlobStream from '..';

const argv = yargs.argv;

console.log(argv._);

const stream = S3GlobStream(argv._);

stream.on('readable', function readable() {
  let entry;
  while ((entry = this.read()) !== null) {
    console.log(entry.Key);
  }
}).on('error', function error(err) {
  console.log('Error:', err);
  process.exit(1);
});

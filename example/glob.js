#!/usr/bin/env node

'use strict';

var path = require('path'),
	S3GlobStream = require(path.join(__dirname, '..')),
	argv = require('yargs').argv;

console.log(argv._);

var stream = S3GlobStream(argv._);

stream.on('readable', function readable() {
	var entry;
	while (null !== (entry = this.read())) {
		console.log(entry.Key);
	}
}).on('error', function error(err) {
	console.log('Error:', err);
	process.exit(1);
});

#!/usr/bin/env node

'use strict';

var path = require('path'),
	S3 = require('aws-sdk').S3,
	S3GlobStream = require(path.join(__dirname, '..')),
	argv = require('yargs').argv;

var s3 = new S3();

var stream = S3GlobStream(s3, { Bucket: argv.bucket }, argv._);

stream.on('readable', function readable() {
	var entry;
	while (null !== (entry = this.read())) {
		console.log(entry.Key);
	}
}).on('error', function error(err) {
	console.log('Error:', err);
	process.exit(1);
});

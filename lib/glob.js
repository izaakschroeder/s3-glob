
'use strict';

var _ = require('lodash'),
	Stream = require('stream'),
	S3 = require('aws-sdk').S3,
	Minimatch = require('minimatch').Minimatch,
	util = require('util');

/**
 * @constructor
 * @param {AWS.S3} s3 S3 instance.
 * @param {Object} awsOptions Parameters passed straight to `listObjects`.
 * @param {Array|String} globs List of glob patterns to match.
 * @param {Object} streamOptions Options passed to Stream.Readable.
 */
function GlobStream(s3, awsOptions, globs, streamOptions) {
	// Allow not using `new`
	if (this instanceof GlobStream === false) {
		return new GlobStream(s3, awsOptions, globs, streamOptions);
	}

	// Type check
	if (s3 instanceof S3 === false) {
		throw new TypeError();
	}

	// Must provide a bucket.
	if (!_.has(awsOptions, 'Bucket')) {
		throw new TypeError();
	}

	// Handle the single value case
	if (_.isString(globs)) {
		globs = [ globs ];
	}

	// Type check
	if (!_.isArray(globs)) {
		throw new TypeError();
	}

	// Create the readable with some sensible defaults.
	Stream.Readable.call(this, _.assign({
		highWaterMark: 200
	}, streamOptions, { objectMode: true }));

	this.s3 = s3;
	this.awsOptions = awsOptions;

	// Create the globs
	this.globs = _.map(globs, Minimatch);

	// Must have at least 1 non-negative glob
	if (_.every(this.globs, 'negate')) {
		throw new TypeError();
	}

	this.states = _.map(GlobStream.prefixes(globs), function buildState(prefix) {
		return {
			Prefix: prefix,
			Marker: null
		};
	});
}
util.inherits(GlobStream, Stream.Readable);

/**
 * @param {Array} set Single match set from a Minimatch object.
 * @returns {String} Single prefix corresponding to the set.
 */
GlobStream.prefix = function prefix(set) {
	return _.chain(set)
		.first(_.isString)
		.join('/')
		.value();
};

/**
 * @param {Array} globs List of Minimatch glob items.
 * @returns {Array} List of prefixes.
 */
GlobStream.prefixes = function prefixes(globs) {
	return _.chain(globs)
		.reject('negate')
		.pluck('set')
		.flatten(true)
		.map(GlobStream.prefix)
		.value();
};

GlobStream.prototype.match = function match(entry) {
	return _.every(this.globs, function checkGlob(glob) {
		return glob.match(entry.Key);
	});
};

/**
 * @param {Number} size The number of S3 keys to read
 * @returns {Undefined} Nothing.
 * @see Stream.Readable._read
 */
GlobStream.prototype._read = function _read(size) {
	var self = this,
		state = _.last(self.states);

	// We have to use a kind of primitive lock since _read is called
	// constantly when pushing data.
	if (self._reading) {
		return;
	}

	// If there are no more states left then we're done.
	if (this.states.length === 0) {
		this.push(null);
		return;
	}

	// No work needs doing if we're not reading anything.
	if (size <= 0) {
		return;
	}

	self._reading = true;

	// Fetch the objects making sure to respect the desired
	// highWaterMark (passed in through size here).
	self.s3.listObjects(_.assign({ }, self.awsOptions, state, {
		MaxKeys: size
	}), function listObjectsDone(err, result) {

		// Pass thru errors
		if (err) {
			return self.emit('error', err);
		}

		// Pump all the matching results out to the stream
		_.chain(result.Contents)
			.filter(self.match, self)
			.each(self.push, self);

		// Unlock for more reads
		self._reading = false;

		// Upload local state if necessary
		if (result.IsTruncated) {
			state.Marker = result.NextMarker;
		} else {
			self.states.pop();
		}

		self._read(size - result.Contents.length);
	});
};

module.exports = GlobStream;


'use strict';

var _ = require('lodash'),
	AWS = require('aws-sdk'),
	Stream = require('stream'),
	Minimatch = require('minimatch').Minimatch,
	url = require('s3-url'),
	util = require('util');

/**
 * @constructor
 * @param {Array|String|Object} globs List of glob patterns to match.
 * @param {Object} options Options.
 * @param {Boolean} options.format Output object format.
 * @param {Object} options.s3 S3 instance to use.
 *
 * Note that the globs are either S3 URLs of the form s3://bucket/key/*...
 * or negation patterns of the form !not/pattern. Globbing is done in a similar
 * manner to file-glob, where anything matching ANY of the positive patterns is
 * returned (only once) but nothing matching ANY of the negative patterns is.
 */
function GlobStream(globs, options) {

	// Allow not using `new`
	if (this instanceof GlobStream === false) {
		return new GlobStream(globs, options);
	}

	options = _.assign({
		highWaterMark: 200,
		format: 'object',
		unique: true,
		s3: new AWS.S3()
	}, options);

	// Duck typing
	if (!options.s3 || !_.isFunction(options.s3.listObjects)) {
		throw new TypeError('Bad S3.');
	}

	// Handle the single value case
	if (_.isString(globs) || _.isPlainObject(globs)) {
		globs = [ globs ];
	}

	// Type check
	if (!_.isArray(globs)) {
		throw new TypeError();
	}

	if (!_.contains(GlobStream.formats, options.format)) {
		throw new TypeError();
	}

	// Create the readable with some sensible defaults.
	Stream.Readable.call(this, {
		objectMode: true,
		highWaterMark: options.highWaterMark
	});

	this.s3 = options.s3;
	this.awsOptions = options.awsOptions;
	this.unique = options.unique;
	this.format = options.format;


	var parts = _.groupBy(globs, function group(glob) {
		if (_.isString(glob) && glob.charAt(0) === '!') {
			return 'filter';
		} else {
			return 'search';
		}
	});

	this.filters = _.map(parts.filter, Minimatch);

	this.states = _.chain(parts.search)
		.map(this.normalize, this)
		.map(function state(awsOptions) {
			var match = Minimatch(awsOptions.Key);
			return _.map(GlobStream.prefixes(match), function entry(prefix) {
				return {
					match: match,
					awsOptions: awsOptions,
					prefix: prefix,
					marker: null
				};
			});
		})
		.flatten()
		.value();

	this.processed = { };

	// Must have at least 1 non-negative glob
	if (this.states.length === 0) {
		throw new TypeError('Must have at least 1 non-negative glob.');
	}
}
util.inherits(GlobStream, Stream.Readable);

GlobStream.formats = [
	'object',
	'query'
];

GlobStream.allowedOptions = [
	'Bucket'
];

/**
 * Create and validate AWS parameters from a globbish object.
 * @param {String|Object} glob Thing to suck parameters out of.
 * @returns {Object} AWS parameters.
 */
GlobStream.prototype.normalize = function normalize(glob) {
	var res = _.assign(
		{ },
		this.awsOptions,
		_.isString(glob) ? url.urlToOptions(glob) : glob
	);

	if (_.isEmpty(res.Key)) {
		throw new TypeError();
	} else if (_.isEmpty(res.Bucket)) {
		throw new TypeError();
	} else {
		return res;
	}
};

/**
 * Determine if the entry matches the current search. All negative filters
 * of the stream must be matched and the local entry's filter must be matched.
 * @param {Object} search One of the entries in this.states.
 * @param {Object} entry The AWS object gotten from the AWS SDK.
 * @returns {Boolean} True if we should add this to the stream.
 */
GlobStream.prototype.match = function match(search, entry) {
	return _.every(this.filters, function check(filter) {
		return filter.match(entry.Key);
	}) && search.match.match(entry.Key);
};

/**
 * Return a unique key for S3 objects. This is used to check and see if the
 * entry has already been processed.
 * @param {Object} search One of the entries in this.states.
 * @param {Object} entry The AWS object gotten from the AWS SDK.
 * @returns {String} A unique key for `entry`.
 */
GlobStream.prototype.key = function key(search, entry) {
	return entry.Bucket + '/' + entry.Key;
};

/**
 * Check if an S3 object has been processed or not.
 * @param {Object} search One of the entries in this.states.
 * @param {Object} entry The AWS object gotten from the AWS SDK.
 * @returns {Boolean} True if processed, false otherwise.
 */
GlobStream.prototype.hasProcessed = function hasProcessed(search, entry) {
	return this.processed[this.key(search, entry)];
};

/**
 * Process an AWS object retrived from S3.listObjects.
 * @param {Object} search One of the entries in this.states.
 * @param {Object} entry The AWS object gotten from the AWS SDK.
 * @returns {void}
 */
GlobStream.prototype.process = function process(search, entry) {
	if (this.unique && this.hasProcessed(search, entry)) {
		return;
	}
	this.processed[this.key(search, entry)] = true;
	if (this.match(search, entry)) {
		switch (this.format) {
		case 'query':
			this.push(_.assign({ }, search.awsOptions, { Key: entry.Key }));
			break;
		case 'object':
			this.push(entry);
			break;
		default:
			this.emit('error', 'UNKNOWN_FORMAT');
		}
	}
};

/**
 * @param {Array} set Single match set from a Minimatch object.
 * @returns {String} Single prefix corresponding to the set.
 */
GlobStream.prefix = function prefix(set) {
	return _.chain(set)
		.takeWhile(_.isString)
		.join('/')
		.value();
};

/**
* @param {Object} match Single Minimatch object.
* @returns {Array} Array of prefixes corresponding to the Minimatch object.
*/
GlobStream.prefixes = function prefix(match) {
	return _.map(match.set, GlobStream.prefix);
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

	var request = _.defaults({ }, {
		Prefix: state.prefix,
		Marker: state.marker,
		MaxKeys: size
	}, _.pick(state.awsOptions, GlobStream.allowedOptions));

	// Fetch the objects making sure to respect the desired
	// highWaterMark (passed in through size here).
	self.s3.listObjects(request, function listObjectsDone(err, result) {

		// Pass thru errors
		if (err) {
			return self.emit('error', err);
		}

		// Pump all the matching results out to the stream
		_.forEach(result.Contents, function process(entry) {
			entry.Bucket = request.Bucket;
			self.process(state, entry);
		});

		// Unlock for more reads
		self._reading = false;

		// Upload local state if necessary; if the results are truncated then
		// more results are available for the current state, so just update the
		// marker; otherwise the state is done and we can pop it off the list
		// of states to process.
		if (result.IsTruncated) {
			// The NextMarker is only provided when Delimiter is set, otherwise
			// the Key of the last element is to be used instead (as per the
			// AWS documentation).
			state.marker = result.NextMarker || _.last(result.Contents).Key;
		} else {
			self.states.pop();
		}

		self._read(size - result.Contents.length);
	});
};

module.exports = GlobStream;

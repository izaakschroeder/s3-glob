
'use strict';

var _ = require('lodash'),
	AWS = require('aws-sdk'),
	Minimatch = require('minimatch').Minimatch,
	GlobStream = require('glob');

describe('GlobStream', function() {
	describe('#constructor', function() {

		beforeEach(function() {
			this.sandbox = sinon.sandbox.create();
			this.s3 = sinon.createStubInstance(AWS.S3);
			this.sandbox.stub(GlobStream.prototype, '_read');
		});

		afterEach(function() {
			this.sandbox.restore();
		});

		it('should throw an error when not given an S3 object', function() {
			expect(_.partial(GlobStream)).to.throw(TypeError);
		});

		it('should throw an error when not given a bucket parameter', function() {
			expect(_.partial(GlobStream, this.s3, { }, [ '*' ])).to.throw(TypeError);
		});

		it('should throw an error when given no globs', function() {
			expect(_.partial(GlobStream, this.s3, { Bucket: 'foo' }, [ ])).to.throw(TypeError);
		});

		it('should throw an error when given invalid globs', function() {
			expect(_.partial(GlobStream, this.s3, { Bucket: 'foo' }, false)).to.throw(TypeError);
			expect(_.partial(GlobStream, this.s3, { Bucket: 'foo' }, 5)).to.throw(TypeError);
			expect(_.partial(GlobStream, this.s3, { Bucket: 'foo' }, [ 10 ])).to.throw(TypeError);
		});

		it('should throw an error when given only negative globs', function() {
			expect(_.partial(GlobStream, this.s3, { Bucket: 'foo' }, [ '!test' ])).to.throw(TypeError);
		});

		it('should always put the stream into object mode', function() {
			var stream = GlobStream(this.s3, { Bucket: 'foo' }, [ '*' ], { objectMode: false });
			expect(stream._readableState).to.have.property('objectMode', true);
		});

		it('should create minimatch objects from a singular glob', function() {
			var stream = GlobStream(this.s3, { Bucket: 'foo' }, '*');
			expect(stream.globs).to.have.length(1);
			expect(stream.globs[0]).to.be.an.instanceof(Minimatch);
		});
	});

	describe('prefix', function() {
		it('should create a valid single prefix from a minimatch set', function() {
			var set = Minimatch('a/b/c').set[0];
			expect(GlobStream.prefix(set)).to.equal('a/b/c');
		});
		it('should create a valid single prefix from a complex minimatch set', function() {
			var set = Minimatch('a/b/foo*.gz').set[0];
			expect(GlobStream.prefix(set)).to.equal('a/b');
		});
	});

	describe('prefixes', function() {
		it('should ignore any negative minimatch items', function() {
			var globs = [ Minimatch('!abc'), Minimatch('def'), Minimatch('!ghi') ];
			expect(GlobStream.prefixes(globs)).to.have.length(1);
		});

		it('should include all items of all sets', function() {
			var globs = [ Minimatch('{a,b}/c'), Minimatch('d') ];
			expect(GlobStream.prefixes(globs)).to.have.length(3);
		});
	});

	describe('#match', function() {
		it('should return true when an entry matches all globs', function() {
			var globs = [ Minimatch('{a,b}/*'), Minimatch('*/c') ];
			expect(GlobStream.prototype.match.call({
				globs: globs
			}, {
				Key: 'b/c'
			})).to.be.true;
		});

		it('should return false when an entry fails to match any glob', function() {
			var globs = [ Minimatch('{a,b}/*'), Minimatch('*/c') ];
			expect(GlobStream.prototype.match.call({
				globs: globs
			}, {
				Key: 'a/b'
			})).to.be.false;
		});
	});

	describe('#_read', function() {

		beforeEach(function() {
			this.sandbox = sinon.sandbox.create();
			this.s3 = sinon.createStubInstance(AWS.S3);
			this.s3.listObjects = sinon.stub();
			this.stream = GlobStream(this.s3, { Bucket: 'test' }, [ '*', '!b' ]);
		});

		afterEach(function() {
			this.sandbox.restore();
		});

		it('should make no parallel AWS calls', function(done) {
			this.s3.listObjects.callsArgWithAsync(1, null, {
				IsTruncated: false,
				Contents: [ { Key: 'a' } ]
			});
			this.stream.on('readable', function() {
				var entry;
				while (null !== (entry = this.read())) {
					expect(entry).to.not.be.null;
				}
			}).on('end', function() {
				expect(this.s3.listObjects).to.be.calledTwice;
				done();
			}).on('error', function(err) {
				done(err);
			});
		});

		it('should update the marker in AWS calls', function() {
			this.s3.listObjects.callsArgWith(1, null, {
				IsTruncated: true,
				Contents: [ { Key: 'a' } ],
				NextMarker: 'b'
			});
			this.stream.read(0);
			this.stream.read(0);
			expect(this.s3.listObjects).to.be.calledWithMatch({
				Marker: 'b'
			});
		});

		it('should emit all matching AWS results', function() {
			var keys = [ { Key: 'a' }, { Key: 'b' }, { Key: 'c' } ];
			var spy = this.sandbox.spy(this.stream, 'push');
			this.s3.listObjects.callsArgWith(1, null, {
				IsTruncated: false,
				Contents: keys
			});
			this.stream.read(0);
			expect(spy).to.be.calledWith(keys[0]);
			expect(spy).to.be.calledWith(keys[2]);
		});

		it('should emit an error when AWS errors', function() {
			var spy = this.sandbox.stub(this.stream, 'emit');
			this.s3.listObjects.callsArgWith(1, 'aws-error');
			this.stream.read(0);
			expect(spy).to.be.calledOnce.and.calledWith('error', 'aws-error');
		});

		it('should emit EOS when no more states are left', function() {
			var spy = this.sandbox.spy(this.stream, 'push');
			this.s3.listObjects.callsArgWith(1, null, {
				IsTruncated: false,
				Contents: [  ]
			});
			this.stream.read(0);
			expect(spy).to.be.calledOnce.and.calledWith(null);
		});

		it('should use the last key when no NextMarker is provided', function() {
			this.s3.listObjects.callsArgWith(1, null, {
				IsTruncated: true,
				Contents: [ { Key: 'a' } ]
			});
			this.stream.read(0);
			this.stream.read(0);
			expect(this.s3.listObjects).to.be.calledWithMatch({
				Marker: 'a'
			});
		});
	});
});

import { Minimatch } from 'minimatch';
import { expect } from 'chai';
import { sandbox } from 'sinon';
import GlobStream from '../../src/glob';

describe('GlobStream', function() {
  beforeEach(function() {
    this.sandbox = sandbox.create();
    this.s3 = { listObjects: this.sandbox.stub() };
  });

  afterEach(function() {
    this.sandbox.restore();
  });

  describe('#constructor', function() {
    beforeEach(function() {
      this.sandbox.stub(GlobStream.prototype, '_read');
    });

    it('should throw an error when given an invalid S3 object', function() {
      expect(() => new GlobStream([ '*' ], { s3: null }))
        .to.throw(TypeError);
    });

    it('should throw an error when given no globs', function() {
      expect(() => new GlobStream([ ], { s3: this.s3 }))
        .to.throw(TypeError);
    });

    it('should throw an error when given invalid globs', function() {
      expect(() => new GlobStream(false))
        .to.throw(TypeError);
      expect(() => new GlobStream(5))
        .to.throw(TypeError);
      expect(() => new GlobStream([ 10 ]))
        .to.throw(TypeError);
    });

    it('should throw an error when given only negative globs', function() {
      expect(() => new GlobStream([ '!test' ]))
        .to.throw(TypeError);
    });

    it('should throw an error with no bucket', function() {
      expect(() => new GlobStream([ 'test' ]))
        .to.throw(TypeError);
    });

    it('should throw an error when given invalid format', function() {
      expect(() => new GlobStream('s3://a/b', {
        s3: this.s3,
        format: 'foo',
      })).to.throw(TypeError);
    });

    it('should always put the stream into object mode', function() {
      const stream = new GlobStream('s3://a/b', { objectMode: false });
      expect(stream._readableState).to.have.property('objectMode', true);
    });

    it('should create minimatch objects from a singular glob', function() {
      const stream = new GlobStream('*', { awsOptions: { Bucket: 'foo' } });
      expect(stream.states).to.have.length(1);
      expect(stream.states[0].match).to.be.an.instanceof(Minimatch);
    });

    it('should create minimatch objects from an object', function() {
      const stream = new GlobStream({ Key: 'foo', Bucket: 'foo' });
      expect(stream.states).to.have.length(1);
      expect(stream.states[0].match).to.be.an.instanceof(Minimatch);
    });
  });

  describe('prefix', function() {
    it('should make single prefix from minimatch set', function() {
      const set = Minimatch('a/b/c').set[0];
      expect(GlobStream.prefix(set)).to.equal('a/b/c');
    });
    it('should make single prefix from complex minimatch set', function() {
      const set = Minimatch('a/b/foo*.gz').set[0];
      expect(GlobStream.prefix(set)).to.equal('a/b');
    });
  });

  describe('prefixes', function() {
    it('should create prefixes from a minimatch', function() {
      const set = Minimatch('a/b/{c,d}');
      expect(GlobStream.prefixes(set)).to.deep.equal([ 'a/b/c', 'a/b/d' ]);
    });
    it('should create a prefixes from a complex minimatch', function() {
      const set = Minimatch('{a,c}/b/foo*.gz');
      expect(GlobStream.prefixes(set)).to.deep.equal([ 'a/b', 'c/b' ]);
    });
  });

  describe('#match', function() {
    beforeEach(function() {
      this.stream = new GlobStream([ 's3://x/{a,b}/*', 's3://x/*/c', '!d/c' ]);
      this.search = { match: Minimatch('{a,b}/*') };
    });

    it('should be true when an entry matches search', function() {
      expect(this.stream.match(this.search, {
        Key: 'a/b',
      })).to.be.true;
    });

    it('should be false when an entry fails to match search', function() {
      expect(this.stream.match(this.search, {
        Key: 'e/f/g',
      })).to.be.false;
    });

    it('should be false when an entry matches a negate', function() {
      expect(this.stream.match(this.search, {
        Key: 'd/c',
      })).to.be.false;
    });
  });

  describe('#_read', function() {
    beforeEach(function() {
      this.stream = new GlobStream([ 's3://test/*?Foo=1', '!b' ], {
        s3: this.s3,
      });
    });

    it('should make no parallel AWS calls', function(done) {
      this.s3.listObjects.callsArgWithAsync(1, null, {
        IsTruncated: false,
        Contents: [ { Key: 'a' } ],
      });
      this.stream.on('readable', function() {
        let entry;
        while ((entry = this.read()) !== null) {
          expect(entry).to.not.be.null;
        }
      }).on('end', function() {
        expect(this.s3.listObjects).to.be.calledOnce;
        done();
      }).on('error', function(err) {
        done(err);
      });
    });

    it('should update the marker in AWS calls', function() {
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: true,
        Contents: [ { Key: 'a' } ],
        NextMarker: 'b',
      });
      this.stream.read(0);
      this.stream.read(0);
      expect(this.s3.listObjects).to.be.calledWithMatch({
        Marker: 'b',
      });
    });

    it('should emit all matching AWS results', function() {
      const keys = [ { Key: 'a' }, { Key: 'b' }, { Key: 'c' } ];
      const spy = this.sandbox.spy(this.stream, 'push');
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: false,
        Contents: keys,
      });
      this.stream.read(0);
      expect(spy).to.be.calledWith(keys[0]);
      expect(spy).to.be.calledWith(keys[2]);
    });

    it('should emit an error when AWS errors', function() {
      const spy = this.sandbox.stub(this.stream, 'emit');
      this.s3.listObjects.callsArgWith(1, 'aws-error');
      this.stream.read(0);
      expect(spy).to.be.calledOnce.and.calledWith('error', 'aws-error');
    });

    it('should emit an error if invalid format', function() {
      const spy = this.sandbox.stub(this.stream, 'emit');
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: false,
        Contents: [ { Key: 'a' } ],
      });
      this.stream.format = 'foo';
      this.stream.read(0);
      expect(spy).to.be.calledOnce.and.calledWith('error');
    });

    it('should emit EOS when no more states are left', function() {
      const spy = this.sandbox.spy(this.stream, 'push');
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: false,
        Contents: [ ],
      });
      this.stream.read(0);
      expect(spy).to.be.calledOnce.and.calledWith(null);
    });

    it('should use the last key when there is no NextMarker', function() {
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: true,
        Contents: [ { Key: 'a' } ],
      });
      this.stream.read(0);
      this.stream.read(0);
      expect(this.s3.listObjects).to.be.calledWithMatch({
        Marker: 'a',
      });
    });

    it('should correct query object when in query mode', function() {
      this.stream.format = 'query';
      const keys = [ { Key: 'a' } ];
      const spy = this.sandbox.spy(this.stream, 'push');
      this.s3.listObjects.callsArgWith(1, null, {
        IsTruncated: false,
        Contents: keys,
      });
      this.stream.read(0);
      expect(spy).to.be.calledWith({
        Bucket: 'test',
        Key: 'a',
        Foo: '1',
      });
    });
  });
});

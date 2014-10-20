# s3-glob

> Retrieve object list entries in S3 using minimatch-style globbing.

![build status](http://img.shields.io/travis/izaakschroeder/s3-glob.svg?style=flat)
![coverage](http://img.shields.io/coveralls/izaakschroeder/s3-glob.svg?style=flat)
![license](http://img.shields.io/npm/l/s3-glob.svg?style=flat)
![version](http://img.shields.io/npm/v/s3-glob.svg?style=flat)
![downloads](http://img.shields.io/npm/dm/s3-glob.svg?style=flat)


Features:
 * Full support for minimatch syntax,
 * Streaming output.

```javascript
var S3 = require('aws-sdk').S3,
	GlobStream = require('s3-glob');

var stream = GlobStream(new S3(), { Bucket: 'my-bucket' }, [ '*.jpg', '!test*' ]);

stream.on('readable', function() {
	var entry;
	while (null !== (entry = this.read())) {
		console.log(entry);
	}
});
```

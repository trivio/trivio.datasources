# Portions of code derived from
# https://github.com/Yelp/mrjob/blob/master/tests/mockboto.py

import urllib
import boto


class MockS3Connection(object):
    """Mock out boto.s3.Connection
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None,
                 host=None, debug=0, https_connection_factory=None,
                 calling_format=None, path='/', provider='aws',
                 bucket_class=None, mock_s3_fs=None):
        """Mock out a connection to S3. Most of these args are the same
        as for the real S3Connection, and are ignored.

        You can set up a mock filesystem to share with other objects
        by specifying mock_s3_fs. The mock filesystem is just a map
        from bucket name to key name to bytes.
        """
        # use mock_s3_fs even if it's {}
        self.mock_s3_fs =  mock_s3_fs or {}
        self.endpoint = host or 's3.amazonaws.com'

    def get_bucket(self, bucket_name, **kw):
        if bucket_name in self.mock_s3_fs:
            return MockBucket(connection=self, name=bucket_name)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_all_buckets(self):
        return [self.get_bucket(name) for name in self.mock_s3_fs]

    def create_bucket(self, bucket_name, headers=None, location='',
                      policy=None):
        if bucket_name in self.mock_s3_fs:
            raise boto.exception.S3CreateError(409, 'Conflict')
        else:
            self.mock_s3_fs[bucket_name] = {'keys': {}, 'location': ''}


class MockBucket:
    """Mock out boto.s3.Bucket
    """
    def __init__(self, connection=None, name=None, location=None):
        """You can optionally specify a 'data' argument, which will instantiate
        mock keys and mock data. data should be a map from key name to bytes
        and time last modified.
        """
        self.name = name
        self.connection = connection

    def mock_state(self):
        """Returns a dictionary from key to data representing the
        state of this bucket."""
        if self.name in self.connection.mock_s3_fs:
            return self.connection.mock_s3_fs[self.name]['keys']
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def new_key(self, key_name):
        if key_name not in self.mock_state():
            self.mock_state()[key_name] = ('',
                    to_iso8601(datetime.utcnow()))
        return MockKey(bucket=self, name=key_name)

    def get_key(self, key_name):
        if key_name in self.mock_state():
            return MockKey(bucket=self, name=key_name, date_to_str=to_rfc1123)
        else:
            return None
    lookup=get_key

    def get_location(self):
        return self.connection.mock_s3_fs[self.name]['location']

    def set_location(self, new_location):
        self.connection.mock_s3_fs[self.name]['location'] = new_location

    def list(self, prefix='', delimiter='', marker=''):
        for key_name in sorted(self.mock_state()):
            if key_name.startswith(prefix) and (key_name > marker):
                yield MockKey(bucket=self, name=key_name,
                              date_to_str=to_iso8601)

    def get_all_keys(self, maxkeys=1000, **args):
      return list(self.list(**args))[:maxkeys]
        
      


class MockKey(object):
    """Mock out boto.s3.Key"""

    def __init__(self, bucket=None, name=None, date_to_str=None):
        """You can optionally specify a 'data' argument, which will fill
        the key with mock data.
        """
        self.bucket = bucket
        self.name = name
        self.date_to_str = date_to_str or to_iso8601

    def generate_url(self, seconds_good_for,force_http=True):
        #http://trivio.test.s3.amazonaws.com/compensation/dt%3D2011-05-31T00%3A00%3A00/compensation_final.csv?
        if force_http == True:
          scheme = 'http'
        else:
          scheme = 'https'
          
          
        return "{scheme}://{bucket}.s3.amazonaws.com/{name}?".format(
          scheme=scheme,
          bucket=self.bucket.name, 
          name=urllib.quote(self.name)
        )
        
    def read_mock_data(self):
        """Read the bytes for this key out of the fake boto state."""
        if self.name in self.bucket.mock_state():
            return self.bucket.mock_state()[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')
    read = read_mock_data
    def write_mock_data(self, data):
        if self.name in self.bucket.mock_state():
            self.bucket.mock_state()[self.name] = (data, datetime.utcnow())
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_contents_to_filename(self, path, headers=None):
        with open(path, 'w') as f:
            f.write(self.read_mock_data())

    def set_contents_from_filename(self, path):
        with open(path) as f:
            self.write_mock_data(f.read())

    def get_contents_as_string(self):
        return self.read_mock_data()

    def set_contents_from_string(self, string):
        self.write_mock_data(string)

    def delete(self):
        if self.name in self.bucket.mock_state():
            del self.bucket.mock_state()[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def make_public(self):
        pass

    def __iter__(self):
        data = self.read_mock_data()
        i = 0
        while i < len(data):
            yield data[i:min(len(data), i + SIMULATED_BUFFER_SIZE)]
            i += SIMULATED_BUFFER_SIZE

    def _get_last_modified(self):
        if self.name in self.bucket.mock_state():
            return self.date_to_str(self.bucket.mock_state()[self.name][1])
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    # option to change last_modified time for testing purposes
    def _set_last_modified(self, time_modified):
        if self.name in self.bucket.mock_state():
            data = self.bucket.mock_state()[self.name][0]
            self.bucket.mock_state()[self.name] = (data, time_modified)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    last_modified = property(_get_last_modified, _set_last_modified)

    def _get_etag(self):
        m = hashlib.md5()
        m.update(self.get_contents_as_string())
        return m.hexdigest()

    etag = property(_get_etag)

    @property
    def size(self):
        return len(self.get_contents_as_string())


### EMR ###

def to_iso8601(when):
    """Convert a datetime to ISO8601 format.
    """
    return when.strftime(boto.utils.ISO8601)

def to_rfc1123(when):
    """Convert a datetime to RFC1123 format.
    """
    # AWS sends us a time zone in all cases, but in Python it's more
    # annoying to figure out time zones, so just fake it.
    assert when.tzinfo is None
    return when.strftime(RFC1123) + 'GMT'

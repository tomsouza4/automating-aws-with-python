# -*- coding: utf-8 -*-

"""Classes for S3 Buckets."""

from pathlib import Path
from botocore.exceptions import ClientError
import mimetypes
from functools import reduce
import boto3
from hashlib import md5
import util

class BucketManager:
    """Manage an S3 Bucket."""

    """Class level constant"""
    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """Create a BucketManager object."""
        self.session = session
        self.s3 = session.resource('s3')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_chunksize = self.CHUNK_SIZE,
            multipart_threshold = self.CHUNK_SIZE
        )
        self.manifest = {}


    def get_region_name(self, bucket):
        """Get the bucket's region name."""
        client = self.s3.meta.client
        bucket_location = client.get_bucket_location(
            Bucket=bucket.name)

        return bucket_location["LocationConstraint"] or 'us-east-1'

    def get_bucket_url(self, bucket):
        """Get the URL for this bucket, also will be used to find out auto
        which url should be used. Was taken from Google research instead of
        looking into AWS API."""
        return "http://{}.{}".format(
            bucket.name,
            util.get_endpoint(self.get_region_name(bucket)).host)

    def all_buckets(self):
        """Get an iterator for all buckets."""
        return self.s3.buckets.all()


    def all_objects(self, bucket_name):
        """Get an iterator for all objects."""
        return self.s3.Bucket(bucket_name).objects.all()


    def init_bucket(self, bucket_name):
        """Create new bucket, or return existent one by name."""
        s3_bucket = None
        try:
            s3_bucket = self.s3.create_bucket(Bucket=bucket_name)



        except ClientError as error:
            if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                s3_bucket = s3.Bucket(bucket_name)
            else:
                raise error

        return s3_bucket

    def set_policy(self, bucket):
        """Set bucket policy to be readable for everyone."""
        policy = """
        {
          "Version":"2012-10-17",
          "Statement":[{
          "Sid":"PublicReadGetObject",
          "Effect":"Allow",
          "Principal": "*",
              "Action":["s3:GetObject"],
              "Resource":["arn:aws:s3:::%s/*"
              ]
            }
          ]
        }
        """ % bucket.name
        """Strip removes the initial space on before the policy text."""
        policy = policy.strip()

        pol = bucket.Policy()
        pol.put(Policy=policy)

    def configure_website(self, bucket):
        """Configure s3 website hosting for bucket, sets index.html as the
        go to page when accessing the URL as well error.html(which doesn't
        exist as of now)."""
        bucket.Website().put(WebsiteConfiguration={
            'ErrorDocument': {
                'Key': 'error.html'
            },
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        })

    def load_manifest(self, bucket):
        """Load manifest for caching purposes. It helps to list all the
        files like a book making it easier to upload since will be using
        the ETag to compare if a file already exists in the S3. An obj will
        be a dictionay and will contain ETag, Key, LastModified, Size and
        StorageClass"""
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket.name):
            for obj in page.get('Contents', []):
                self.manifest[obj['Key']] = obj['ETag']

    @staticmethod
    def hash_data(data):
        """It will take some data in and then hash in md5 those."""
        hash = md5()
        hash.update(data)

        return hash

    def gen_etag(self, path):
        """Generate etag for each file"""
        hashes = []

        """Will open and read the file as binary limiting it to the CHUNK_SIZE."""
        with open(path, 'rb') as f:
            while True:
                data = f.read(self.CHUNK_SIZE)

                if not data:
                    break

                hashes.append(self.hash_data(data))

        if not hashes:
            return
        elif len(hashes) == 1:
            """The double quotes here are bc the ETag comes with them automatically
            i.e: 'ETag': '"51568bc93ada05c778f47e6dc55ea085"'."""
            """It should work fine for most of the cases but for big files it may
            not be the best way to do it, bc it breaks the file in parts/chuncks."""
            return '"{}"'.format(hashes[0].hexdigest())
        else:
            """AWS takes hash of each part of the data and then hashes it all
            together.Reduce() will take another function in this case lambda()
            and will take a list of things and then will iterate through it and
            append each item to the previous one.
            i.e: HASH_OF_HASHES(hash1+hash2+hash3...hashN)"""
            hash = self.hash_data(reduce(lambda x, y: x + y, (h.digest() for h in hashes)))

            """First argument will return the hash of hashes and the second
            is the number of chuncks of our data."""
            return '"{}-{}"'.format(hash.hexdigest(), len(hashes))


    def upload_file(self, bucket, path, key):
        """Upload files to a bucket.
        bukcet is the S3 to receive the files
        path is where files will be updated to
        key is the name of the file."""
        """mimetypes here will identify which kinda file we'll be updating to S3."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'

        """Generating ETags for path."""
        etag = self.gen_etag(path)

        """Will get the key from AWS and if it doesn't exist will create a new
        one."""
        if self.manifest.get(key, '') == etag:
            """print("Skipping {}, ETags match".format(key))"""
            return

        return bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
            },
            Config=self.transfer_config)

    def sync(self, pathname, bucket_name):
        """Sync contents of a path to bucket."""
        bucket = self.s3.Bucket(bucket_name)
        self.load_manifest(bucket)

        root = Path(pathname).expanduser().resolve()
        """Checks if the current structure is a folder or a file before
        uploading to S3"""
        def handle_directory(target):
            for p in target.iterdir():
                if p.is_dir():
                    handle_directory(p)
                if p.is_file():
                    self.upload_file(bucket, str(p), str(p.relative_to(root)))


        handle_directory(root)

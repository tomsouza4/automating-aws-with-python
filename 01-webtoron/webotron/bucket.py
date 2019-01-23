# -*- coding: utf-8 -*-

"""Classes for S3 Buckets."""

from pathlib import Path
from botocore.exceptions import ClientError
import mimetypes

import util

class BucketManager:
    """Manage an S3 Bucket."""

    def __init__(self, session):
        """Create a BucketManager object."""
        self.session = session
        self.s3 = session.resource('s3')

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

    @staticmethod
    def upload_file(bucket, path, key):
        """Upload files to a bucket.
        bukcet is the S3 to receive the files
        path is where files will be updated to
        key is the name of the file."""
        """mimetypes here will identify which kinda file we'll be updating to S3."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'

        return bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
            })

    def sync(self, pathname, bucket_name):
        """Sync contents of a path to bucket."""
        bucket = self.s3.Bucket(bucket_name)

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

"""

Synchronize an S3 bucket or compatible cloud storage to a mounted dotmesh dot,
retaining version integrity via recording a _set of object versions_.

This python prototype would eventually be translated into Go and embedded in
dotmesh as:

    b) an implementation of an interface for server-side remotes which allows
       clone/push/pull to and from object storage as well as other dotmesh
       clusters.

"""

import boto3
import pprint
#import botocore
import sys, os
from botocore.handlers import set_list_objects_encoding_type_url
from botocore.client import Config
from boto3.session import Session

# add debugging:
#boto3.set_stream_logger('')

class VersionedSync(object):

    def __init__(self, args):
        self.args = args
        self.session = boto3.session.Session()
        self.s3 = self.session.resource(
            service_name='s3',
            aws_access_key_id=args['OBJECT_STORE_KEY_ID'],
            aws_secret_access_key=args['OBJECT_STORE_ACCESS_KEY'],
            endpoint_url=args['OBJECT_STORE_ENDPOINT_URL'],
        )

        session = Session(aws_access_key_id=args['OBJECT_STORE_KEY_ID'],
          aws_secret_access_key=args['OBJECT_STORE_ACCESS_KEY'],
          region_name="US-CENTRAL1")

        session.events.unregister('before-parameter-build.s3.ListObjects',
              set_list_objects_encoding_type_url)

        self.s3 = session.resource('s3', endpoint_url=args['OBJECT_STORE_ENDPOINT_URL'],
              config=Config(signature_version='s3v4'))
        self.s3_client = session.client('s3', endpoint_url=args['OBJECT_STORE_ENDPOINT_URL'],
              config=Config(signature_version='s3v4'))





    def clone(self):
        bucket_name = self.args["OBJECT_STORE_BUCKET"]
        bucket_versioning = self.s3.BucketVersioning(bucket_name)

        if bucket_versioning.status is None:
            print("enabling bucket versioning")
            bucket_versioning.enable()

        print("versioning state", bucket_versioning.status)

        versions = self.s3_client.list_object_versions(Bucket=bucket_name)

        pprint.pprint(versions)
        return

        versions = self.s3.Bucket(bucket_name).object_versions
        for v in versions.all():
            o = v.get()
            print(o)
        return

        set_of_object_versions = set()

        bucket = self.s3.Bucket(bucket_name)
        for o in bucket.objects.all():
            full_object = self.s3.Object(bucket_name, o.key)
            set_of_object_versions.add((o.key, full_object.version_id))

        print(set_of_object_versions)

        # TODO

        self.s3_client.download_file(
            bucket_name, 'hello.txt', '/tmp/hello.txt', ExtraArgs={'VersionId': 'foo'},
        )

        """
        try:
            s3.Bucket(BUCKET_NAME).download_file(KEY, 'my_local_image.jpg')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

        pass
        """

    def pull(self):
        raise NotImplementedError()

    def push(self):
        raise NotImplementedError()


requiredEnv = [
    "OBJECT_STORE_ENDPOINT_URL",
    "OBJECT_STORE_KEY_ID",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_BUCKET",
    "DOTMESH_CLUSTER_URL",
    "DOTMESH_USERNAME",
    "DOTMESH_API_KEY",
    "DOTMESH_DOTNAME",
]

def main():
    missing = []
    for e in requiredEnv:
        if e not in os.environ:
            missing.append(e)
    if missing != []:
        print("Missing env vars: %s" % (missing,))
        sys.exit(1)

    if len(sys.argv) != 2:
        print("Usage: sync.py clone/push/pull")
        sys.exit(1)

    v = VersionedSync(os.environ)
    if sys.argv[1] == "clone":
        v.clone()
    else:
        print("Operand must be one of clone, push, pull")
        sys.exit(1)

if __name__ == "__main__":
    main()

import boto3
from botocore.exceptions import NoCredentialsError
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

import urllib3
import hashlib
import hmac
import base64


class S3Dict:
    """
    A class for accessing an S3 bucket with a dict-like interface.
    """

    def __init__(self, bucket_name, region_name, access_key=None, secret_key=None):
        """
        Initialize the S3Dict object with the bucket name, region, and optional access/secret keys.

        Args:
            bucket_name (str): Name of the S3 bucket.
            region_name (str): Region of the S3 bucket.
            access_key (str, optional): AWS access key. Defaults to None.
            secret_key (str, optional): AWS secret key. Defaults to None.
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3 = boto3.resource(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def get(self, key):
        """
        Get an object from the S3 bucket using the provided key.

        Args:
            key (str): Key of the object to retrieve.

        Returns:
            BytesIO: BytesIO object representing the retrieved object.
        """
        try:
            obj = self.s3.Object(self.bucket_name, key)
            return BytesIO(obj.get()["Body"].read())
        except NoCredentialsError:
            raise Exception("No AWS credentials found.")
        
    def get_urllib(self, key):
        # Create the HTTP connection pool
        http = urllib3.PoolManager()

        # Generate the S3 request URL
        endpoint = f'https://{self.bucket_name}.s3.amazonaws.com/{key}'
        # endpoint = 'https://smartexaibucket.s3.eu-west-1.amazonaws.com/Hemp+Industry'

        # Generate the signature for the request
        http_method = 'GET'
        content_md5 = ''
        content_type = ''
        date = ''
        canonicalized_amz_headers = ''
        canonicalized_resource = f'/{self.bucket_name}/{key}'

        string_to_sign = f'{http_method}\n{content_md5}\n{content_type}\n{date}\n{canonicalized_amz_headers}{canonicalized_resource}'

        signature = base64.b64encode(
            hmac.new(self.secret_key.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha1).digest()
        ).decode('utf-8')

        # Create the headers with the AWS access keys and signature
        headers = {
            'Host': f'{self.bucket_name}.s3.amazonaws.com',
            'Date': date,
            'Authorization': f'AWS {self.access_key}:{signature}'
        }

        # Send the request
        response = http.request(http_method, endpoint, headers=headers)

        # Get the response data
        response_data = response.data.decode('utf-8')

        # Print the response
        print(response_data)

    def put_urllib(self, key, value):
        # Create a PoolManager instance
        http = urllib3.PoolManager()

        # Define the URL and file path
        # Generate the S3 request URL
        endpoint = f'https://{self.bucket_name}.s3.amazonaws.com/{key}'
        
        # Make the PUT request
        response = http.request('PUT', endpoint, body=value)

        # Get the response status code
        status_code = response.status

        # Print the response status code
        print(f'Status code: {status_code}')

    def put(self, key, value):
        """
        Put a new object in the S3 bucket.

        Args:
            key (str): Key of the object to put.
            value (BytesIO): BytesIO object representing the content of the object.
        """
        try:
            self.put_urllib(key, value)
            self.s3.Object(self.bucket_name, key).put(Body=value)
        except NoCredentialsError:
            raise Exception("No AWS credentials found.")

    def pop(self, key):
        """
        Remove an object from the S3 bucket and return it.

        Args:
            key (str): Key of the object to remove.

        Returns:
            BytesIO: BytesIO object representing the removed object.
        """
        obj = self.get(key)
        try:
            self.s3.Object(self.bucket_name, key).delete()
        except NoCredentialsError:
            raise Exception("No AWS credentials found.")
        return obj

    def __getitem__(self, key):
        """
        Get an object from the S3 bucket using the provided key.

        Args:
            key (str): Key of the object to retrieve.

        Returns:
            BytesIO: BytesIO object representing the retrieved object.
        """
        return self.get(key)

    def __setitem__(self, key, value):
        """
        Put a new object in the S3 bucket.

        Args:
            key (str): Key of the object to put.
            value (BytesIO): BytesIO object representing the content of the object.
        """
        self.put(key, value)

    def __delitem__(self, key):
        """
        Remove an object from the S3 bucket.

        Args:
            key (str): Key of the object to remove.
        """
        self.pop(key)

    def __contains__(self, key):
        """
        Check if an object exists in the S3 bucket.

        Args:
            key (str): Key of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        try:
            self.s3.Object(self.bucket_name, key).load()
            return True
        except NoCredentialsError:
            raise Exception("No AWS credentials found.")
        except Exception:
            return False

    def keys(self, prefix=""):
        """
        Generate the keys from the S3 bucket with an optional prefix filter.

        Args:
            prefix (str, optional): Prefix to filter the keys. Defaults to ''.

        Yields:
            str: Key from the S3 bucket.
        """
        try:
            bucket = self.s3.Bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=prefix):
                yield obj.key
        except NoCredentialsError:
            raise Exception("No AWS credentials found.")

    def items(self, prefix=""):
        """
        Generate tuples of key-value pairs from the S3 bucket with an optional prefix filter.

        Args:
            prefix (str, optional): Prefix to filter the keys. Defaults to ''.

        Yields:
            tuple: Key-value pair from the S3 bucket.
        """
        with ThreadPoolExecutor() as executor:
            for key in self.keys(prefix):
                future = executor.submit(self.get, key)
                yield key, future.result()

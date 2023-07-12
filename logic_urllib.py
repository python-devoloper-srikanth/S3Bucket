import threading
from io import BytesIO
from urllib import request
import urllib


class S3Dict:
    def __init__(self, bucket: str, region: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key
    
    def get(self, key: str) -> BytesIO:
        url = self._get_url(key)
        response = request.urlopen(url)
        return BytesIO(response.read())
    
    def put(self, key: str, value: BytesIO):
        url = self._get_url(key)
        data=value.read()
        req = urllib.request.Request(url, data=data, method='POST')
        request.urlopen(req)
    
    def pop(self, key: str) -> BytesIO:
        value = self.get(key)
        self._delete(key)
        return value
    
    def __getitem__(self, key: str) -> BytesIO:
        return self.get(key)
    
    def __setitem__(self, key: str, value: BytesIO):
        self.put(key, value)
    
    def __delitem__(self, key: str):
        self._delete(key)
    
    def __contains__(self, key: str) -> bool:
        url = self._get_url(key)
        try:
            request.urlopen(url)
            return True
        except request.HTTPError:
            return False
    
    def keys(self, prefix: str = ''):
        url = self._get_url(prefix)
        response = request.urlopen(url)
        keys = [line.decode().split('<Key>')[1].split('</Key>')[0] for line in response]
        yield from keys
    
    def items(self, prefix: str = ''):
        keys = self.keys(prefix)
        for key in keys:
            value = self.get(key)
            yield key, value
    
    def _get_url(self, key: str) -> str:
        base_url = f"https://{self.bucket}.s3.{self.region}.amazonaws.com/"
        return base_url + key
    
    def _delete(self, key: str):
        url = self._get_url(key)
        request.urlopen(url, method='DELETE')

    def _threaded_items(self, prefix: str):
        keys = self.keys(prefix)
        for key in keys:
            value = self.get(key)
            yield key, value
    
    def threaded_items(self, prefix: str = ''):
        keys = self.keys(prefix)
        threads = []
        for key in keys:
            t = threading.Thread(target=self._threaded_items, args=(key,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()


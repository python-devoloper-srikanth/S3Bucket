Design a python3 class for accessing S3 with an interface similar to a python `dict`.

Use 'boto3' module to make interface 
It should define the following methods:

- `__init__(...)` - initialize the object with the bucket/region/AWS-keys to get access to the bucket;
- `get(key: str) -> BytesIO` - to get an object from the S3 bucket from a key;
- `put(key: str, value: BytesIO)` - to put a new object in the bucket;
- `pop(key: str) -> >BytesIO` - to remove an object from the bucket and return it;
- `__getitem__(key: str) -> BytesIO` - nicer interface for `get`;
- `__setitem__(key: str, value: BytesIO)` - nicer interface for `put`;
- `__delitem__(key: str)` - nicer interface to `pop` (that does not return anything);
- `__contains__(key: str) -> bool` - checks if object exists;
- `keys(prefix: str = '') -> str` - generator that yields the keys from S3 bucket with the option to filter with a prefix;
- `items(prefix: str = '') -> tuple` - similar to keys, but the generator yields `tuples` of key-value pairs (bonus: make this method multi-threaded);

Suggestion: The usual module to make this interface would be `boto3`, however, to get public objects from a bucket without AWS-keys, `urllib` can be used.

Deliverable: A python script with the class implementation and some basic tests of each functionality. The class and methods should have standard documentation.


https://www.youtube.com/watch?v=7r2z3Qn3Qz8



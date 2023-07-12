from io import BytesIO

from logic.logic_urllib import S3Dict

# Create an instance of S3Dict
s3_dict = S3Dict(bucket='smartexaibucket', region='eu-west-1', access_key='AKIAUEFP45MURZEESJGO', secret_key='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')

# Call the put method
data_to_put = BytesIO(b'This is the data to put in S3')
s3_dict.put('key', data_to_put)

# Call the get method
object_data = s3_dict.get('key')


# Call the pop method
popped_object = s3_dict.pop('key')

# Call the __getitem__ method
object_data = s3_dict['key']

# Call the __setitem__ method
s3_dict['key'] = data_to_put

# Call the __delitem__ method
del s3_dict['key']

# Call the __contains__ method
if 'key' in s3_dict:
    print('Key exists in the S3 bucket')
else:
    print('Key does not exist in the S3 bucket')

# Call the keys method
for key in s3_dict.keys(prefix='prefix'):
    print(key)

# Call the items method
for key, value in s3_dict.items(prefix='prefix'):
    print(key, value)

# Call the threaded_items method
for key, value in s3_dict.threaded_items(prefix='prefix'):
    print(key, value)

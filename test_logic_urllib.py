from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib import request

import pytest

from logic.logic_urllib import S3Dict


@pytest.fixture
def s3_dict():
    return S3Dict(bucket='test-bucket', region='test-region', access_key='test-access-key', secret_key='test-secret-key')

def test_get(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_response = MagicMock()
        mock_response.read.return_value = b'Test data'
        mock_urlopen.return_value = mock_response
        
        # Call the get method
        result = s3_dict.get('key')
        
        # Assert the result
        assert isinstance(result, BytesIO)
        assert result.getvalue() == b'Test data'
        
        # Assert that the correct URL was used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key')

def test_put(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Call the put method
        data_to_put = BytesIO(b'Test data')
        s3_dict.put('key', data_to_put)
        
        # Assert that the correct URL and data were used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key', data=b'Test data')

def test_pop(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        with patch.object(request, 'HTTPError') as mock_http_error:
            # Set up the mock response
            mock_response = MagicMock()
            mock_response.read.return_value = b'Test data'
            mock_urlopen.return_value = mock_response
            
            # Call the pop method
            result = s3_dict.pop('key')
            
            # Assert the result
            assert isinstance(result, BytesIO)
            assert result.getvalue() == b'Test data'
            
            # Assert that the correct URL was used
            mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key')
            
            # Assert that the delete method was called
            mock_http_error.assert_called_once()
            mock_http_error.assert_called_with().code = 204

def test_getitem(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_response = MagicMock()
        mock_response.read.return_value = b'Test data'
        mock_urlopen.return_value = mock_response
        
        # Call the __getitem__ method
        result = s3_dict['key']
        
        # Assert the result
        assert isinstance(result, BytesIO)
        assert result.getvalue() == b'Test data'
        
        # Assert that the correct URL was used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key')

def test_setitem(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Call the __setitem__ method
        data_to_put = BytesIO(b'Test data')
        s3_dict['key'] = data_to_put
        
        # Assert that the correct URL and data were used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key', data=b'Test data')

def test_delitem(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        with patch.object(request, 'HTTPError') as mock_http_error:
            # Call the __delitem__ method
            del s3_dict['key']
            
            # Assert that the correct URL was used
            mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/key')
            
            # Assert that the delete method was called
            mock_http_error.assert_called_once()
            mock_http_error.assert_called_with().code = 204

def test_contains(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_urlopen.side_effect = [None, request.HTTPError('url', 404, 'Not Found', {}, None)]
        
        # Check if key exists
        assert 'key' in s3_dict
        
        # Check if non-existent key exists
        assert 'nonexistent-key' not in s3_dict
        
        # Assert that the correct URLs were used
        assert mock_urlopen.call_count == 2
        mock_urlopen.assert_any_call('https://test-bucket.s3.test-region.amazonaws.com/key')
        mock_urlopen.assert_any_call('https://test-bucket.s3.test-region.amazonaws.com/nonexistent-key')

def test_keys(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_response = MagicMock()
        mock_response.__iter__.return_value = iter([b'<Key>key1</Key>', b'<Key>key2</Key>'])
        mock_urlopen.return_value = mock_response
        
        # Call the keys method
        result = list(s3_dict.keys(prefix='prefix'))
        
        # Assert the result
        assert result == ['key1', 'key2']
        
        # Assert that the correct URL was used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/?prefix=prefix')

def test_items(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_response = MagicMock()
        mock_response.__iter__.return_value = iter([b'<Key>key1</Key>', b'<Key>key2</Key>'])
        mock_urlopen.return_value = mock_response
        
        # Call the items method
        result = list(s3_dict.items(prefix='prefix'))
        
        # Assert the result
        assert result == [('key1', s3_dict.get('key1')), ('key2', s3_dict.get('key2'))]
        
        # Assert that the correct URL was used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/?prefix=prefix')

def test_threaded_items(s3_dict):
    with patch.object(request, 'urlopen') as mock_urlopen:
        # Set up the mock response
        mock_response = MagicMock()
        mock_response.__iter__.return_value = iter([b'<Key>key1</Key>', b'<Key>key2</Key>'])
        mock_urlopen.return_value = mock_response
        
        # Call the threaded_items method
        result = list(s3_dict.threaded_items(prefix='prefix'))
        
        # Assert the result
        assert result == [('key1', s3_dict.get('key1')), ('key2', s3_dict.get('key2'))]
        
        # Assert that the correct URL was used
        mock_urlopen.assert_called_once_with('https://test-bucket.s3.test-region.amazonaws.com/?prefix=prefix')


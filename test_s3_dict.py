import pytest
from io import BytesIO
from botocore.exceptions import NoCredentialsError
from logic.s3_dict import S3Dict
from unittest.mock import Mock


@pytest.fixture
def s3_dict():
    # Create an instance of S3Dict with a test bucket and region
    return S3Dict(bucket_name="test_bucket", region_name="us-east-1")


@pytest.fixture
def sample_data():
    # Sample data for testing
    return {
        "key1": b"This is the content of object 1",
        "key2": b"This is the content of object 2",
        "key3": b"This is the content of object 3",
    }


def test_put_and_get_with_mocks(s3_dict, sample_data, monkeypatch):
    # Mock the put method of the S3 object
    mock_put = Mock()
    monkeypatch.setattr(s3_dict.s3.Object, "put", mock_put)

    # Mock the load method of the S3 object
    mock_load = Mock()
    monkeypatch.setattr(s3_dict.s3.Object, "load", mock_load)

    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))
        mock_put.assert_called_with(Body=BytesIO(value))

    # Get objects from the bucket and assert their content
    for key, value in sample_data.items():
        mock_load.reset_mock()
        mock_load.return_value = {"Body": BytesIO(value)}
        obj = s3_dict.get(key)
        assert obj.read() == value
        mock_load.assert_called()

    # Test non-existent key
    with pytest.raises(Exception):
        s3_dict.get("non_existent_key")


def test_put_and_get(s3_dict, sample_data):
    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))

    # Get objects from the bucket and assert their content
    for key, value in sample_data.items():
        obj = s3_dict.get(key)
        assert obj.read() == value

    # Test non-existent key
    with pytest.raises(Exception):
        s3_dict.get("non_existent_key")


def test_delitem(s3_dict, sample_data):
    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))

    # Delete objects from the bucket and assert they are removed
    for key in sample_data.keys():
        del s3_dict[key]
        assert key not in s3_dict

    # Test deleting a non-existent key
    with pytest.raises(Exception):
        del s3_dict["non_existent_key"]


def test_contains(s3_dict, sample_data):
    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))

    # Test existing keys
    for key in sample_data.keys():
        assert key in s3_dict

    # Test non-existent key
    assert "non_existent_key" not in s3_dict


@pytest.mark.parametrize("prefix", ["", "key"])
def test_keys(s3_dict, sample_data, prefix):
    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))

    # Test keys with prefix
    keys_with_prefix = [key for key in sample_data.keys() if key.startswith(prefix)]
    assert list(s3_dict.keys(prefix=prefix)) == keys_with_prefix


@pytest.mark.parametrize("prefix", ["", "key"])
def test_items(s3_dict, sample_data, prefix):
    # Put objects in the bucket
    for key, value in sample_data.items():
        s3_dict.put(key, BytesIO(value))

    # Test items with prefix
    items_with_prefix = [
        (key, BytesIO(value))
        for key, value in sample_data.items()
        if key.startswith(prefix)
    ]
    assert list(s3_dict.items(prefix=prefix)) == items_with_prefix


def test_no_credentials_error(s3_dict, sample_data):
    # Try to perform operations without credentials
    with pytest.raises(NoCredentialsError):
        for key, value in sample_data.items():
            s3_dict.put(key, BytesIO(value))

    with pytest.raises(NoCredentialsError):
        for key in sample_data.keys():
            s3_dict.get(key)

    with pytest.raises(NoCredentialsError):
        for key in sample_data.keys():
            del s3_dict[key]

    with pytest.raises(NoCredentialsError):
        for key in sample_data.keys():
            key in s3_dict


def test_bad_testcases(s3_dict, sample_data):
    # Bad testcase: Accessing a non-existent key should raise an exception
    with pytest.raises(Exception):
        s3_dict.get("non_existent_key")

    # Bad testcase: Deleting a non-existent key should not raise an exception
    s3_dict.pop("non_existent_key")

    # Bad testcase: Accessing keys with an invalid prefix
    with pytest.raises(Exception):
        list(s3_dict.keys(prefix="invalid"))

    # Bad testcase: Accessing items with an invalid prefix
    with pytest.raises(Exception):
        list(s3_dict.items(prefix="invalid"))

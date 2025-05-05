import pytest
from crawler.storage import Storage

@pytest.fixture
def storage():
    return Storage({'cache_dir': 'test_cache', 'bloom_capacity': 1000, 'bloom_error_rate': 0.01, 'cache_ttl_days': 7})

def test_add_visited(storage):
    url = "http://example.com"
    storage.add_visited(url)
    assert storage.is_visited(url) is True

def test_cache(storage):
    url = "http://example.com"
    content = "<html>Test</html>"
    storage.save_to_cache(url, content)
    cached_content = storage.get_from_cache(url)
    assert cached_content == content

from etl_project.assets.extract_news import News
import pytest
from dotenv import load_dotenv
import os

@pytest.fixture
def setup():
    load_dotenv()
    
def test_News_client_extract_payload(setup):
    api_key = os.environ.get("API_KEY")
    News_client = News(api_key=api_key, which_news = 'news', language = 'en', timeframe = 2, prioritydomain= 'top', 
        size = 10,)
    result = News_client.get_news()
    
    assert len(result) > 0
    assert type(result) == dict

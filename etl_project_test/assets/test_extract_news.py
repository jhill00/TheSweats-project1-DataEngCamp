from etl_project.assets.extract_news import News
from etl_project.assets.extract_news import loaded
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

def test_build_params(setup):
    # Assemble
    news = News(api_key = os.environ.get('API_KEY'))

    # Act
    result = news._build_params(param = 'hello', value = 'world')

    # Assert
    assert type(result) == dict
    assert len(result) > 0

def test_next_page_news(setup):
    # Assemble
    news = News(api_key = os.environ.get('API_KEY'))
    response = news.get_news()

    # Act
    next_response = news.next_page_news(response = response)

    # Assert
    assert type(next_response) == dict
    assert len(next_response) > 0



def test_loaded():
    # Mock the data and external dependencies
    mock_response = [
        {
            "title": "Test Title 1",
            "article_link": "https://example.com/article1",
            
        },
        {
            "title": "Test Title 2",
            "article_link": "https://example.com/article2",
         
        },
    ]


    # Call the function
    result = loaded(df=mock_response, postgresql_client=None, table=None, metadata=None, load_method="overwrite")

    # Assert that the result is as expected
    assert result is True






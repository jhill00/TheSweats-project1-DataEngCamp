from etl_project.assets.extract_news import News
from etl_project.assets.extract_news import json_news_to_df, rename_and_select_columns_news, download_from_s3, calculate_word_frequency, process_articles
import pandas as pd
import pytest
from dotenv import load_dotenv
import os
# Mocking the AWS S3 client for testing purposes
import moto
from moto import mock_s3
import boto3

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

def test_json_news_to_df(setup):
    api_key = os.environ.get("API_KEY")
    News_client = News(api_key=api_key, which_news = 'news', language = 'en', timeframe = 2, prioritydomain= 'top', 
        size = 10,)
    df = json_news_to_df(News_client.get_news())
    assert type(df) == pd.DataFrame

# Sample data for testing
sample_df_data = {
    # Convert to a list because of ValueError looked it up on stackoverflow
    "status": ["status"],  
    "totalResults": [10],  
    "title": ["title"],  
    "link": ["link"],  
    "source_id": ["source1"],  
    "source_priority": [1],  
    "keywords": ["keyword"], 
    "creator": ["author"],  
    "pubDate": ["2023-01-01"], 
    "image_url": ["image_url"], 
    "video_url": ["video"],  
    "description": ["description"],  
    "content": ["content"],  
    "category": ["category"],  
    "country": ["country"],  
    "language": ["language"], 
}

def test_rename_and_select_columns_news(setup):
    expected_columns = [
        'title', 'article_link', 'keywords', 'author', 
        'keywords', 'publish_date', 'article_contents', 
        'category', 'country', 'language'
    ]

    renamed_df = rename_and_select_columns_news(pd.DataFrame(sample_df_data))
    assert list(renamed_df.columns) == expected_columns

def test_download_from_s3(setup):

    with moto.mock_s3():
        s3_bucket = 'test-bucket'
        key = 'test_file.csv'
        s3_resource = boto3.resource('s3')
        s3_resource.create_bucket(Bucket=s3_bucket)
        s3_resource.Object(s3_bucket, key).put(Body="test_data")

        df = download_from_s3('ACCESS_KEY', 'SECRET_KEY', s3_bucket, key)
        assert isinstance(df, pd.DataFrame)

def test_calculate_word_frequency(setup):
    article_contents = "This is a sample text. Sample text contains sample words."
    word = "sample"
    frequency = calculate_word_frequency(article_contents, word)
    assert frequency == 3

def test_process_articles(setup):
    word_by_grade_level_data = {
        'Word': ['sample', 'text'],
        'Grade_Lv': [5, 6]
    }
    newspaper_articles_data = {
        'title': ['Article 1', 'Article 2'],
        'article_contents': ['This is a sample text.', 'Another sample text.']
    }

    results_df = process_articles(word_by_grade_level_data, newspaper_articles_data)
    assert type(results_df) == pd.DataFrame
    assert len(results_df) == 4  
from dotenv import load_dotenv
import os
from etl_project.connectors.postgresql import *
from etl_project.assets.extract_news import *
from sqlalchemy import *


if __name__ == "__main__":
    loads = load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")
    ACCESS_KEY = os.environ.get("ACCESS_KEY") #need to grab this from IAM user and save to .env
    SECRET_KEY = os.environ.get("SECRET_KEY") #need to grab this from IAM user and save to .env

    news = News(api_key=API_KEY, which_news='news', language='en', timeframe=24, size=10, country='us')
    news_data = news.get_news()

    postgresql_client = PostgreSqlClient(server_name=SERVER_NAME, database_name=DATABASE_NAME, username=DB_USERNAME, password=DB_PASSWORD, port=PORT)
    metadata = MetaData()

    # Pulling the newspaper api data and then loading it 
  
    df_news_data = json_news_to_df(news_data)
    df_renamed_news_data = rename_and_select_columns_news(df_news_data)

    news_raw_table = Table(
    'news_raw_table',  
    metadata,
    Column('title', String),  
    Column('article_link', String, primary_key=True),
    Column('keywords', String),
    Column('author', String),
    Column('publish_date', String), 
    Column('article_contents', Text),
    Column('category', String),
    Column('country', String),
    Column('language', String)
    )

    loaded(df=df_renamed_news_data, postgresql_client=postgresql_client, table=news_raw_table, metadata=metadata, load_method="upsert")
    
    # Processing the newspaper df using vocabulary by grade level, then loading it 
    df_vocabulary_by_gradelv = download_from_s3(ACCESS_KEY=ACCESS_KEY, SECRET_KEY=SECRET_KEY, s3_bucket="thesweats-project1", key="vocabulary_by_gradelv.csv")
    df_processed = process_articles(word_by_grade_level_df=df_vocabulary_by_gradelv, newspaper_articles_df=df_renamed_news_data)

    df_processed.to_sql(name='grade_level_word_frequency', con=postgresql_client.engine, if_exists='replace', index=False)
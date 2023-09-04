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

    news = News(api_key=API_KEY, which_news='news', language='en', timeframe=24, size=10, country='us')
    news_data = news.get_news()

    
    trans_data = json_news_to_df(news_data)
    trans_data = rename_and_select_columns_news(trans_data)

   

    postgresql_client = PostgreSqlClient(server_name=SERVER_NAME, database_name=DATABASE_NAME, username=DB_USERNAME, password=DB_PASSWORD, port=PORT)
    metadata = MetaData()



    table = Table(
    'news',  
    metadata,
    Column('title', String),  
    Column('article_link', String),
    Column('keywords', String),
    Column('author', String),
    Column('publish_date', String), 
    Column('article_contents', Text),
    Column('category', String),
    Column('country', String),
    Column('language', String)
)
  

    loaded(df=trans_data, postgresql_client=postgresql_client, table=table, metadata=metadata, load_method="overwrite")
   

   



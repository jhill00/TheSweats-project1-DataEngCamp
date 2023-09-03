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

    # API_KEY = os.getenv("API_KEY")
    # SERVER_NAME = os.getenv("SERVER_NAME")
    # DATABASE_NAME = os.getenv("DATABASE_NAME")
    # DB_USERNAME = os.getenv("DB_USERNAME")
    # DB_PASSWORD = os.getenv("DB_PASSWORD")
    # PORT = os.getenv("PORT")

    # Extract: Fetch news data

    news = News(api_key=API_KEY, which_news='news', language='en', timeframe=24, size=10, country='us')
    news_data = news.get_news()

    # transform: convert json to dataframe
    trans_data = json_news_to_df(news_data)
    trans_data = rename_and_select_columns_news(trans_data)

    # load: load data to postgresql

    postgresql_client = PostgreSqlClient(server_name=SERVER_NAME, database_name=DATABASE_NAME, username=DB_USERNAME, password=DB_PASSWORD, port=PORT)
    metadata = MetaData()



    table = Table(
    'news',  # The table name is 'news'
    metadata,
    Column('title', String),  # Replace 'String' with the appropriate data type for each column
    Column('article_link', String),
    Column('keywords', String),
    Column('author', String),
    Column('publish_date', String),  # You might want to use a different data type for dates
    Column('article_contents', Text),
    Column('category', String),
    Column('country', String),
    Column('language', String)
)
    print("Debugging: Checking variable types and values before calling load()")
    print(f"Type of 'load': {type(loaded)}")
    print(f"Type of 'trans_data': {type(trans_data)}")
    print(f"Type of 'postgresql_client': {type(postgresql_client)}")
    print(f"Type of 'table': {type(table)}")
    print(f"Type of 'metadata': {type(metadata)}")

# Check for boolean values
    if isinstance(loaded, bool):
        print("Variable 'load' is a boolean!")

# Add similar checks for other variables if needed

# Now, call the load function
    loaded(df=trans_data, postgresql_client=postgresql_client, table=table, metadata=metadata, load_method="overwrite")
    # loaded(trans_data=trans_data, postgresql_client=postgresql_client, table=table, metadata=metadata, load_method="upsert")

   



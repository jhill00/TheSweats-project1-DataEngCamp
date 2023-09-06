from dotenv import load_dotenv
import os
from etl_project.connectors.postgresql import PostgreSqlServer, PostgreSqlClient, loaded
from etl_project.assets.extract_news import News, json_news_to_df, rename_and_select_columns_news, rename_and_select_columns_trends
from sqlalchemy import *
import datetime
from loguru import logger

if __name__ == "__main__":
    load_dotenv()

    # Load environment variables for logging database
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    

    # log files will be saved
    log_directory = "etl_project/logs"  # Replace with your desired directory

    # log file name
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    
    logger.add(os.path.join(log_directory, f"database_{timestamp}.log"), rotation="500 MB", mode="a")

    try:
        logger.info("Loading for news table")
        API_KEY = os.environ.get("API_KEY_ID")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        PORT = os.environ.get("PORT")

        logger.info("Extracting news data")

        news = News(api_key=API_KEY, which_news='news', language='en', timeframe=24, size=10, country='us')
        news_data = news.get_news()

        trans_data = json_news_to_df(news_data)
        trans_data = rename_and_select_columns_news(trans_data)

        logger.info("Connecting to PostgreSQL database")

        postgresql_client = PostgreSqlClient(server_name=SERVER_NAME,
                                             database_name=DATABASE_NAME,
                                             username=DB_USERNAME,
                                             password=DB_PASSWORD,
                                             port=PORT)


        metadata = MetaData()

        table = Table(
            'news',
            metadata,
            Column("id", Integer, primary_key=True),
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

        logger.info("Loading data to PostgreSQL database")
        loaded(df=trans_data,
               postgresql_client=postgresql_client,
               table=table,
               metadata=metadata,
               load_method="overwrite")
        logger.info("Data loaded to PostgreSQL database")
        logger.success("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        logger.error(f'logs are saved in {log_directory}')
        raise e

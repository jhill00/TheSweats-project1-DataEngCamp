from etl_project.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime

# Load environment variables
load_dotenv()

@pytest.fixture
def setup_postgresql_client():
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    return postgresql_client

@pytest.fixture
def setup_table():
    table_name = "test_table"  # Adjust the table name if needed
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column('id', Integer, primary_key=True),
        Column('title', String),
        Column('article_link', String),
        Column('keywords', String),
        Column('author', String),
        Column('publish_date', DateTime),  # Use DateTime for dates
        Column('article_contents', String),
        Column('category', String),
        Column('country', String),
        Column('language', String)
    )
    return table, metadata, table_name

def test_postgresql_client(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table, metadata, table_name = setup_table

    # Create the table
    postgresql_client.create_table(metadata)

    # Insert data
    data = [
        {
            "title": "McCarthy travels to Maui after deadly wildfires: 'Sheer devastation", 
            "article_link": "https://thehill.com/homenews/house/4184849-mccarthy-travels-to-maui-after-deadly-wildfires/",
            "keywords": "House,News",
            "author": "author",
            "publish_date": "2023-09-03 03:46:33",
            "article_contents": "article_contents",
            "category": "category",
            "country": "country",
            "language": "english"
        }
    ]

    # Insert data into the table
    postgresql_client.insert(data=data, table=table, metadata=metadata)

    # Retrieve data
    result = postgresql_client.select_all(table=table)
    assert len(result) == 1  # Check the length of the inserted data

    # Drop the table
    postgresql_client.drop_table(table_name)

def test_postgresqlclient_upsert(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table, metadata, table_name = setup_table

    # Create the table
    postgresql_client.create_table(metadata)

    # Insert data
    data = [
        {
            "title": "McCarthy travels to Maui after deadly wildfires: 'Sheer devastation",
            "article_link": "https://thehill.com/homenews/house/4184849-mccarthy-travels-to-maui-after-deadly-wildfires/",
            "keywords": "House,News",
            "author": "author",
            "publish_date": "2023-09-03 03:46:33",
            "article_contents": "article_contents",
            "category": "category",
            "country": "country",
            "language": "english"
        }
    ]

    # Insert data into the table
    postgresql_client.insert(data=data, table=table, metadata=metadata)

    # Print existing records for debugging
    existing_records = postgresql_client.select_all(table=table)
    print("Existing Records:", existing_records)

    # Perform the upsert operation
    postgresql_client.upsert(data=data, table=table, metadata=metadata)

    # Retrieve data and perform assertions
    result = postgresql_client.select_all(table=table)
    print("Result:", result)  # Print result for debugging
    assert len(result) == 1  # Check the length after upsert

    # Drop the table
    postgresql_client.drop_table(table_name)


def test_postgresqlclient_overwrite(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table, metadata, table_name = setup_table

    # Create the table
    postgresql_client.create_table(metadata)

    # Insert data
    data = [
        {
            "title": "McCarthy travels to Maui after deadly wildfires: 'Sheer devastation", 
            "article_link": "https://thehill.com/homenews/house/4184849-mccarthy-travels-to-maui-after-deadly-wildfires/",
            "keywords": "House,News",
            "author": "author",
            "publish_date": "2023-09-03 03:46:33",
            "article_contents": "article_contents",
            "category": "category",
            "country": "country",
            "language": "english"
        }
    ]

    # Insert data into the table
    postgresql_client.insert(data=data, table=table, metadata=metadata)
    
    # Perform the overwrite operation
    postgresql_client.overwrite(data=data, table=table, metadata=metadata)
    
    # Retrieve data and perform assertions
    result = postgresql_client.select_all(table=table)
    assert len(result) == 1  # Check the length after overwrite

    # Drop the table
    postgresql_client.drop_table(table_name)

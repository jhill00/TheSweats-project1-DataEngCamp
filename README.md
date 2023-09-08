# TheSweats-project1-DataEngCamp

This project is an ETL pipeline using the NewsData.io api. We'll be extracting the latest news articles, transforming them into a usable format, and loading to RDS Postgrs. The entire end-to-end pipeline will be hosted in the AWS infrastructure. 

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Running](#installation)

## Features

- Retrieve news articles from NewsData.io API.
- Process and analyze news data by enriching with a grade level by word CSV.
- Load data into a PostgreSQL database.
- Perform SQL queries to answer business questions. 
- Easily customize API requests using various parameters.
- Supports different endpoints, including news, crypto, and archive.

## Running the projct
This pipeline is run on AWS through an ECR ECS EC2 infrastructure/


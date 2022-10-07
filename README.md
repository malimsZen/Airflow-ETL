# About

This program does a simple Extract, Transform, and Load(ETL) function where data gets downloaded from a web resources, gets a format transformation before loading it into a PostgreSQL database where the table schema to hold the data is already constructed. The ETL process is run from a bashfile, highlighting the sequence of operation.

## Table of Content


### This script shell script does the following

- Extract user name, user id and home directory path of each user account  defined in the passwd file.
- Save the data into a comma separated(CSV) format.
- Load the data in the csv fil into a table in PostgreSQL database.

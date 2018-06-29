# PyPgRep Package

This is the PyPGREP package, this is a utility application which aims to simplify PostgreSQL database replication, it builds on top of logical replication enabled with the pg_create_logical_replication_slots function and currently uses the data extracted with the test plugin which is experimental and as such this projects also experimental and one should use it with caution and thorough testing. the utility only supports number, varchar, date and timestamp datatypes and does not support bytea data.

PrePreqs
========
Psycopg2

Setup
=====
1. Create the replication slot in the master database
2. Create a directory for the extracted change files, this location has to be specified in the parfiles.
3. Setup parameter file for extraction
4. Start the pypgrep with nohup (as a daemon) with the extract parfile
5. Setup parameter for apply of extracted changes
6. Start the pypgrep with nohup (as a daemon) with the apply parfile
[Github-flavored Markdown](https://guides.github.com/features/mastering-markdown/)
to write your content.



# About

- This project should stream the logical change data capture events from PostgreSQL to a file
- Should capture the existing data
- Should capture create, update and delete operation payload

## Who is the end user?

- Software Engineers who want to migrate data from postgres database to any other data storage

## What is the scope of this project?

- This project captures the logical change data capture payloads from postgres and streams to the file.

## How end users will use this project?

- This should be a single binary file
- End user will specify the source postgres database connection details in YML file
- End user will specify the destination disk storage where the change data capture payload files can be written

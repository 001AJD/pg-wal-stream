# About

- This project should stream the logical change data capture events from PostgreSQL to a file
- Should capture create, update and delete operation payload
- Should capture the existing data

## Who is the end user?

- Anyone who wants to stream the change events from postgres

## What is the scope of this project?

- This project captures the logical change data capture payloads from postgres and streams to the file.

## How end users will use this project?

- This should be a single binary file
- End user will specify the source postgres database connection details in YML file
- End user will specify the destination disk storage where the change data capture payload files can be written

### Internal components

- entry point it can be main.go for now
  -postgres module
  - take care of postgres connection
  - sending keep alive messages to the postgres
  - receiving messages from replication slot
  - parse the messages as they are received from replication slot
  - send the parsed message to the dispatcher

- Dispatcher module
  - It is reponsible to receive the parsed message from the postgres module
  - It should dispatch the parsed message over to the sink connector API handler

- Sink Module
  - It is a destination where the change events payload will be pushed or written to. For now add support for 1 sink ie localfile inside `destination` dir in the current project root dir.
  - It should have capability to support multiple sinks.
  - The sink module should expose the Sink API handler that receives the parsed payload from the dispatcher module and write the actual data to the destination sink
  - The messages should be acknowledged back to the postgres module after the sink has successfully written the message to the destination.
  - Use the JSONL file to store the payloads

### Done

- Currently the local file sink setup combines the file location + type of the file. Decouple the location and the type. Research more on this. --done
- Configure production grade logger. --done

### Next up

- In case of localfiles or events streamed to files, should the files be splitted up perday, per hours. As of now there is only 1 file where all the data is being written. One file can grow pretty big eventually and become bottleneck. Think about it.
- monitoring module. Current status. Consumer lag. how many rows are written to the destination. Think about it.
- The LSN is not committed, maintained anywhere. Every run starts the processing from the start

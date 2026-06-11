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

## Architecture & Modules

Here is a high-level overview of how the internal modules work together to stream changes from PostgreSQL to the destination.

```mermaid
graph LR
    PostgresDB[PostgreSQL] -->|WAL Stream| Streamer[Streamer]

    subgraph PostgresModule["Postgres Module"]
        Streamer -->|Raw WAL Data| Parser[Parser]
        Parser -->|cdc.Event| Dispatcher[Dispatcher]
    end

    subgraph SinkModule["Sink Pipeline"]
        Dispatcher -->|cdc.Event| Encoder[JSONL Encoder]
        Encoder -->|Encoded JSONL Payload| LocalFileSink[Local File Sink]
        LocalFileSink -->|Write / Sync| Disk[Local File System]
    end

    subgraph StateManagement["State Management"]
        LocalFileSink -->|Acknowledge LSN| Tracker[LSN Tracker]
        Tracker -->|Flushed LSN| Streamer
    end

    Streamer -.->|Standby Status Update| PostgresDB

    classDef postgres fill:#E3F2FD,stroke:#1E88E5,color:#000000
    classDef pipeline fill:#E8F5E9,stroke:#43A047,color:#000000
    classDef storage fill:#FFF3E0,stroke:#FB8C00,color:#000000
    classDef state fill:#F3E5F5,stroke:#8E24AA,color:#000000

    class PostgresDB,Streamer,Parser postgres
    class Dispatcher,Encoder pipeline
    class LocalFileSink,Disk storage
    class Tracker state
```

- **Main (Entry Point)**: Initializes configuration, logger, and bootstraps the pipeline by linking the Postgres, Dispatcher, and Sink modules together.
- **Postgres Module**: Manages the connection to the PostgreSQL database. It listens to logical replication slots, receives WAL (Write-Ahead Log) messages, parses them into structured `cdc.Event` payloads, and forwards them to the Dispatcher. It also handles sending keepalive and standby status updates back to Postgres to advance the LSN (Log Sequence Number).
- **Dispatcher Module**: Acts as the router. It receives parsed `cdc.Event` payloads from the Postgres module and dispatches them to the configured downstream Sink handlers.
- **Sink Module**: The destination for CDC events. It provides an interface to support multiple sinks. The primary implementation is `localfile`, which receives events from the dispatcher and appends them to local files in JSONL format.
- **CDC Module**: Defines the shared domain models (like `Event`) that represent the data payload traversing the system.
- **Config & Logger**: Provide centralized configuration management and structured logging across all components.

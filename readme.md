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
        Parser -->|cdc.Event| Streamer
    end

    Streamer -->|cdc.Event| Dispatcher[Dispatcher]

    subgraph DispatcherModule["Dispatcher Module"]
        Dispatcher
    end

    Dispatcher -->|cdc.Event| SinkHandler[Sink Handler]

    subgraph EncoderModule["Encoder Module"]
        Encoder[JSONL Encoder]
    end

    subgraph SinkModule["Sink Module"]
        SinkHandler
        LocalFileSink[Local File Sink]
    end

    SinkHandler <-->|Encode| Encoder
    SinkHandler -->|cdc.EncodedEvent| LocalFileSink

    subgraph Storage["Storage"]
        LocalFileSink -->|Write / Sync| Disk[Local File System]
    end

    subgraph StateManagement["State Management"]
        Tracker[LSN Tracker]
    end

    LocalFileSink -->|Acknowledge LSN| Tracker
    Tracker -->|Flushed LSN| Streamer
    Streamer -.->|Standby Status Update| PostgresDB

    classDef postgres fill:#E3F2FD,stroke:#1E88E5,color:#000000
    classDef dispatcher fill:#FCE4EC,stroke:#D81B60,color:#000000
    classDef sink fill:#E8F5E9,stroke:#43A047,color:#000000
    classDef encoder fill:#FFFDE7,stroke:#FBC02D,color:#000000
    classDef storage fill:#FFF3E0,stroke:#FB8C00,color:#000000
    classDef state fill:#F3E5F5,stroke:#8E24AA,color:#000000

    class PostgresDB,Streamer,Parser postgres
    class Dispatcher dispatcher
    class SinkHandler,LocalFileSink sink
    class Encoder encoder
    class Disk storage
    class Tracker state
```

- **Main (Entry Point)**: Initializes configuration, logger, and bootstraps the pipeline by linking the Postgres, Dispatcher, Encoder, and Sink modules together.
- **Postgres Module**: Manages the connection to the PostgreSQL database. It listens to logical replication slots, receives WAL (Write-Ahead Log) messages, parses them into structured `cdc.Event` payloads via the internal Parser, and forwards them to the Dispatcher. It also handles sending keepalive and standby status updates back to Postgres to advance the LSN (Log Sequence Number).
- **Dispatcher Module**: Acts as the router. It receives parsed `cdc.Event` payloads from the Postgres module and dispatches them to the configured downstream Sink handlers.
- **Encoder Module**: Responsible for transforming the internal `cdc.Event` into a specific storage format. The `jsonl` implementation encodes events as line-delimited JSON objects.
- **Sink Module**: The destination for CDC events. The `Sink Handler` coordinates the flow by using an Encoder to format the event and then writing the result to one or more Sinks. The primary implementation is `localfile`, which appends encoded events to local files.
- **State Management**: Uses an `LSN Tracker` to keep track of the last successfully flushed LSN. This ensures that in case of a restart, the streamer can resume from the correct position.
- **CDC Module**: Defines the shared domain models (like `Event` and `EncodedEvent`) that represent the data payload traversing the system.
- **Config & Logger**: Provide centralized configuration management and structured logging across all components.

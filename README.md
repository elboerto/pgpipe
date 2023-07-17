# PgPipe

PgPipe is a Python package designed to efficiently synchronize data between two PostgreSQL databases. 
It is specifically designed to handle large amounts of data by dividing the transfer process into smaller, 
manageable chunks.

## Features

- Efficient data transfer using the COPY command
- Divide data into smaller chunks for improved performance
- Track transfer progress and allow restarting from checkpoints
- Handle foreign key dependencies by ensuring the correct order of table transfers

## Installation

To install PgPipe, simply use pip:

```bash
pip install pgpipe
```

## Usage

The PgPipe tool provides a command line interface (CLI) for synchronizing data between two PostgreSQL databases. The 
following sections will guide you on how to use this tool effectively.

### Setting up the CLI

PgPipe requires you to specify several settings to connect to your PostgreSQL databases and define the synchronization parameters. These settings can be provided through command line arguments or environment variables. Here is the list of command line arguments you can use:

- **`--log-level`**: Set the log level. Default is 'INFO'.
- **`--schemas`**: Specify the list of schemas. Default is 'public'.
- **`--chunk-size-offset`**: Set the chunk size for offset-based chunking. Default is 50000.
- **`--chunk-size-timedelta`**: Set the chunk size for time-range-based chunking in days. Default is 1.
- **`--db-schema-path`**: Specify the path to the DB schema file. Default is './db_schema.json'.
- **`--chunk-data-path`**: Specify the path to the chunk data file. Default is './chunk_data.json'.
- **`--source-dsn`**: Specify the source database DSN. If not set, it is read from the environment variable 'PGPIPE_SOURCE_DSN'.
- **`--destination-dsn`**: Specify the destination database DSN. If not set, it is read from the environment variable 'PGPIPE_DESTINATION_DSN'.

### Running the Synchronization Steps

The synchronization process is divided into three steps, each corresponding to a command:

1. **Create Database Schema**: To execute this step, run the **`create_db_schema`** command. This will read the source database metadata and store it in the DB schema file specified by **`--db-schema-path`**.

```commandline
pgpipe create_db_schema --source-dsn="your_source_dsn" --db-schema-path="your_db_schema_path"
```

2. **Prepare Chunks**: Run the **`prepare_chunks`** command to prepare chunk metadata for data transfer. The metadata will be saved to the chunk data file specified by **`--chunk-data-path`**.

```commandline
pgpipe prepare_chunks --source-dsn="your_source_dsn" --db-schema-path="your_db_schema_path" --chunk-data-path="your_chunk_data_path" --chunk-size-offset=50000 --chunk-size-timedelta=1
```

3. **Transfer Data**: The **`transfer_data`** command starts the data transfer process, using the prepared metadata to guide the process.

```commandline
pgpipe transfer_data --source-dsn="your_source_dsn" --destination-dsn="your_destination_dsn" --chunk-data-path="your_chunk_data_path"
```

If no command is specified, the CLI will print a help message with a list of available commands and settings.

## License

PgPipe is released under the MIT License. See the [LICENSE](./LICENSE) file for more details. 
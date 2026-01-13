# Rosetta

Rosetta is a high-performance, real-time ETL engine written in Rust. It captures data changes from a **PostgreSQL** database using Logical Replication (CDC - Change Data Capture) and streams them efficiently to **Snowflake**.

Designed for speed and reliability, Rosetta leverages the `etl` framework to handle the replication stream and ensures secure data ingestion into Snowflake using Key-Pair Authentication.

## Project Flow

The data flows through the system in real-time:

1.  **Source (PostgreSQL)**: Rosetta connects to a PostgreSQL database and listens to a logical replication slot. Any changes (INSERT, UPDATE, DELETE) are captured immediately from the WAL (Write-Ahead Log).
2.  **Processing (Rosetta/Rust)**: The Rust application processes these change events. It handles data conversion and batching to optimize throughput.
3.  **Authentication**: Rosetta uses Key-Pair Authentication (RSA) to securely connect to Snowflake. It supports encrypted private keys (PKCS#8) for enhanced security.
4.  **Destination (Snowflake)**: The processed data is ingested into the specified Snowflake table (Landing Table).

## How to Run

### Prerequisites

*   [Rust](https://www.rust-lang.org/tools/install) (latest stable)
*   [Docker](https://www.docker.com/) & Docker Compose (for running the local PostgreSQL instance)
*   OpenSSL (for generating keys)

### Step 1: Generate Private & Public Keys

Rosetta uses Key-Pair Authentication for Snowflake. You need to generate an RSA key pair.

**1. Generate an Encrypted Private Key (PKCS#8)**

You will be asked to enter a passphrase. Remember this passphrase; you will need it for the `.env` configuration.

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 des3 -out rsa_key.p8
```
*   `rsa_key.p8`: This is your private key file. Keep it safe!

**2. Generate the Public Key**

Extract the public key from your private key (you will need to enter the passphrase you just created).

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

**3. Configure Snowflake User**

Log in to your Snowflake account and assign the public key to your user. Open the `rsa_key.pub` file, copy the content (removing the `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` headers and newlines), and run:

```sql
ALTER USER <YOUR_SNOWFLAKE_USER> SET RSA_PUBLIC_KEY='<PASTE_YOUR_PUBLIC_KEY_CONTENT_HERE>';
```

### Step 2: Configuration

Create a `.env` file in the root directory. You can use the example below:

```bash
# Logging
RUST_LOG=info

# Postgres Source
PG_HOST=localhost
PG_PORT=5433
PG_DB=postgres
PG_USER=postgres
PG_PASS=postgres
PG_PUB_NAME=my_publication

# Snowflake Destination
SNOWFLAKE_ACCOUNT=<YOUR_ACCOUNT_LOCATOR> # e.g., XY12345
SNOWFLAKE_USER=<YOUR_SNOWFLAKE_USER>
SNOWFLAKE_DB=<YOUR_DB>
SNOWFLAKE_SCHEMA=<YOUR_SCHEMA>
SNOWFLAKE_TABLE=<YOUR_TARGET_TABLE>
SNOWFLAKE_ROLE=<YOUR_ROLE>

# Authentication
SNOWFLAKE_PRIVATE_KEY_PATH=/absolute/path/to/your/rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<YOUR_PASSPHRASE>
```

### Step 3: Start PostgreSQL

Start the local PostgreSQL instance (configured with logical replication enabled) using Docker Compose:

```bash
docker-compose up -d
```

Validating Postgres is running:
```bash
docker ps
```

### Step 4: Run Rosetta

Run the application using Cargo:

```bash
cargo run
```

Accessing logs:
The application uses `tracing` for logging. You can see the output in your terminal.

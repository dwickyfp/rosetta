import duckdb

# 1. Initialize DuckDB and load the Postgres extension
con = duckdb.connect()
con.sql("INSTALL postgres; LOAD postgres;")

# 2. Attach the Postgres database
# REPLACE this string with your actual Postgres connection details
pg_connection = "dbname=postgres user=postgres host=172.16.62.98 port=5455 password=postgres"
con.sql(f"ATTACH '{pg_connection}' AS pg (TYPE POSTGRES);")

# 3. Create Dummy Data in DuckDB (The Source)
# We create two rows:
# - ID 101: Exists in Postgres (Stock 10 -> will update to 50)
# - ID 102: New item (will insert)
con.sql("""
    CREATE OR REPLACE TABLE duckdb_updates AS 
    SELECT * FROM (VALUES 
        (101, 'Malvin Gamtemg', 50),
        (103, 'New Gadget', 20)
    ) AS t(product_id, product_name, stock_count);
""")

print("--- Data in DuckDB (Source) ---")
con.sql("SELECT * FROM duckdb_updates").show()

# 4. Execute the MERGE Command
# This pushes changes from DuckDB -> Postgres
con.sql("""
    MERGE INTO pg.public.inventory AS target
    USING duckdb_updates AS source
    ON target.product_id = source.product_id
    
    -- Update existing records (ID 101)
    WHEN MATCHED THEN
        UPDATE SET 
            stock_count = source.stock_count,
            product_name = source.product_name,
            last_updated = NOW()
            
    -- Insert new records (ID 102)
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, stock_count, last_updated)
        VALUES (source.product_id, source.product_name, source.stock_count, NOW());
""")

print("--- Merge Complete! ---")

# 5. Verify results by querying Postgres through DuckDB
print("--- Final Data in Postgres (Target) ---")
con.sql("SELECT * FROM pg.public.inventory ORDER BY product_id").show()
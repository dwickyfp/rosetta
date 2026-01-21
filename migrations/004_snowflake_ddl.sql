-- 1. Create the Role
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS ROSETTA_ROLE;

-- 2. Create the Warehouse
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS ROSETTA_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Grant usage of the warehouse to the role
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE ROSETTA_WH TO ROLE ROSETTA_ROLE;

----------------------------------------------------------------------
-- 3. Grants for First Path: DATABASE ENIGMA / SCHEMA SILKROAD
----------------------------------------------------------------------
-- Grant Usage on Database
GRANT USAGE ON DATABASE ENIGMA TO ROLE ROSETTA_ROLE;

-- Grant Usage on Schema
GRANT USAGE ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE;

-- Specific permissions requested (Create Table, Pipe, Stream, Task)
GRANT CREATE TABLE ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE;
GRANT CREATE PIPE ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE;
GRANT CREATE STREAM ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE;
GRANT CREATE TASK ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE;
GRANT CREATE STAGE ON SCHEMA ENIGMA.SILKROAD TO ROLE ROSETTA_ROLE; -- Required for Snowpipe

-- Note: To execute/resume tasks, the role needs the account-level privilege
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ROSETTA_ROLE;

----------------------------------------------------------------------
-- 4. Grants for Second Path: DATABASE SILKROAD / SCHEMA BRONZE
----------------------------------------------------------------------
-- Grant Usage on Database
GRANT USAGE ON DATABASE SILKROAD TO ROLE ROSETTA_ROLE;

-- Grant Usage on Schema
GRANT USAGE ON SCHEMA SILKROAD.BRONZE TO ROLE ROSETTA_ROLE;

-- Specific permissions requested
GRANT CREATE TABLE ON SCHEMA SILKROAD.BRONZE TO ROLE ROSETTA_ROLE;
GRANT CREATE PIPE ON SCHEMA SILKROAD.BRONZE TO ROLE ROSETTA_ROLE;
GRANT CREATE STAGE ON SCHEMA SILKROAD.BRONZE TO ROLE ROSETTA_ROLE;

----------------------------------------------------------------------
-- 5. Create the User
----------------------------------------------------------------------
CREATE USER IF NOT EXISTS ROSETTA_USER
    PASSWORD = 'PlaceholderPassword123!' -- Change this immediately
    DEFAULT_ROLE = ROSETTA_ROLE
    DEFAULT_WAREHOUSE = ROSETTA_WH
    COMMENT = 'User for Kafka Connector Service';

-- Grant the Role to the User
GRANT ROLE ROSETTA_ROLE TO USER ROSETTA_USER;

USE ROLE SECURITYADMIN;

ALTER USER ROSETTA_USER SET RSA_PUBLIC_KEY = '';

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ROSETTA_ROLE;
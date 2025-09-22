-- Switch to the database
\c {{ .Values.storage.databases.tickers }}

-- Create a role
CREATE ROLE table_admins;

-- Create users
CREATE USER {{ .Values.storage.users.benthos }} WITH PASSWORD '{{ .Values.storage.passwords.benthos }}';
CREATE USER {{ .Values.storage.users.pgadmin }} WITH PASSWORD '{{ .Values.storage.passwords.pgadmin }}';

-- Set the timezone to Europe/London
SET TIME ZONE 'Europe/London';

-- Create crypto raw table
CREATE TABLE IF NOT EXISTS {{ .Values.storage.tables.crypto }} (
  benthos_ts timestamptz NOT NULL,
  change numeric,
  change_percent numeric,
  circulating_supply numeric,
  currency text,
  day_high numeric,
  day_low numeric,
  day_volume bigint,
  event_id text,
  exchange text,
  ingestion_ts timestamptz NOT NULL,
  from_currency text,
  last_size bigint,
  market_cap numeric,
  market_hours smallint,
  open_price numeric,
  postgres_ts timestamptz DEFAULT CURRENT_TIMESTAMP,
  price numeric,
  price_hint smallint,
  quote_type smallint,
  source_api text,
  ticker text,
  ticker_ts timestamptz NOT NULL,
  vol_24hr bigint,
  vol_all_currencies bigint
) PARTITION BY RANGE (ticker_ts);

-- Create currency raw table
CREATE TABLE IF NOT EXISTS {{ .Values.storage.tables.currency }} (
  benthos_ts timestamptz NOT NULL,
  change numeric,
  change_percent numeric,
  day_high numeric,
  day_low numeric,
  event_id text,
  exchange text,
  ingestion_ts timestamptz NOT NULL,
  market_hours smallint,
  postgres_ts timestamptz DEFAULT CURRENT_TIMESTAMP,
  price numeric,
  price_hint smallint,
  quote_type smallint,
  source_api text,
  ticker text,
  ticker_ts timestamptz NOT NULL
) PARTITION BY RANGE (ticker_ts);

-- Create stocks raw table
CREATE TABLE IF NOT EXISTS {{ .Values.storage.tables.stocks }} (
  benthos_ts timestamptz NOT NULL,
  change numeric,
  change_percent numeric,
  day_volume bigint,
  event_id text,
  exchange text,
  ingestion_ts timestamptz NOT NULL,
  last_size bigint,
  market_hours smallint,
  postgres_ts timestamptz DEFAULT CURRENT_TIMESTAMP,
  price numeric,
  price_hint smallint,
  quote_type smallint,
  source_api text,
  ticker text,
  ticker_ts timestamptz NOT NULL
) PARTITION BY RANGE (ticker_ts);

-- Grant ALL privileges on the table to the role
GRANT ALL ON {{ .Values.storage.tables.crypto }} TO table_admins;
GRANT ALL ON {{ .Values.storage.tables.currency }} TO table_admins;
GRANT ALL ON {{ .Values.storage.tables.stocks }} TO table_admins;

-- Function to generate monthly partitions dynamically
CREATE OR REPLACE FUNCTION create_monthly_partitions(
    tables_list TEXT[],
    start_date DATE,
    months_ahead INTEGER
) RETURNS VOID AS $$
DECLARE
    current_date DATE;
    partition_name TEXT;
    table_name TEXT;
    start_of_month DATE;
    end_of_month DATE;
    i INTEGER;
BEGIN
    FOREACH table_name IN ARRAY tables_list LOOP
        -- Loop through the number of months to create partitions for
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = table_name AND relkind = 'p'
        ) THEN
            RAISE WARNING 'Table % does not exist or is not partitioned, skipping', table_name;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Processing table: %', table_name;
        FOR i IN 0..months_ahead LOOP
            -- Calculate the start and end dates for the partition
            start_of_month := start_date + (i * INTERVAL '1 month');
            end_of_month := start_of_month + INTERVAL '1 month';

            -- Generate partition name (e.g., stocks_y2025m08)
            partition_name := table_name ||'_y' || EXTRACT(YEAR FROM start_of_month) || 'm' || LPAD(EXTRACT(MONTH FROM start_of_month)::TEXT, 2, '0');

            -- Create the partition if it doesn't exist
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                table_name,
                start_of_month,
                end_of_month
            );

            -- Grant all privileges on the partition to the role
            EXECUTE format(
                'GRANT ALL ON %I TO table_admins',
                partition_name
            );
        END LOOP;

        RAISE NOTICE 'Created partitions for % months starting from %', months_ahead + 1, start_date;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call function to create partitions from the current date
SELECT create_monthly_partitions(
    ARRAY['{{ .Values.storage.tables.currency }}', '{{ .Values.storage.tables.crypto }}', '{{ .Values.storage.tables.stocks }}'],
    (CURRENT_DATE - INTERVAL '1 month')::DATE, -- Start from last month
    12 -- Create partitions for the next 12 months
);

-- Assign the role to the users
GRANT table_admins TO postgres;
GRANT table_admins TO benthos;

from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Set up the Flink Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Define the Kafka Source Table (reading your JSON messages)
t_env.execute_sql("""
    CREATE TABLE green_trips_source (
        lpep_pickup_datetime STRING,
        lpep_dropoff_datetime STRING,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        -- Convert the string to a real timestamp so Flink can window it
        pickup_time AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
        -- Tell Flink to use this time for our tumbling windows
        WATERMARK FOR pickup_time AS pickup_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips-fresh',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-window-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Define the PostgreSQL Sink Table
t_env.execute_sql("""
    CREATE TABLE trip_counts_sink (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/postgres',
        'table-name' = 'trip_counts_by_location',
        'username' = 'postgres',
        'password' = 'postgres'
    )
""")

print("Submitting the Flink Tumbling Window Job...")

# 4. Execute the Tumbling Window Aggregation!
t_env.execute_sql("""
    INSERT INTO trip_counts_sink
    SELECT 
        TUMBLE_START(pickup_time, INTERVAL '5' MINUTE) AS window_start,
        PULocationID,
        COUNT(*) AS num_trips
    FROM green_trips_source
    GROUP BY
        TUMBLE(pickup_time, INTERVAL '5' MINUTE),
        PULocationID
""").wait() # wait() keeps the cell running while Flink processes the stream
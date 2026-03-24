from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_green_trips_source(t_env):
    table_name = "green_trips_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            -- Convert the string to a Flink timestamp
            pickup_time AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
            -- Set the watermark for the tumbling windows
            WATERMARK FOR pickup_time AS pickup_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_trip_counts_sink(t_env):
    table_name = 'trip_counts_by_location'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            PULocationID INTEGER,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def run_tumbling_window_job():
    # 1. Setup the Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Create the tables
    source_table = create_green_trips_source(t_env)
    sink_table = create_trip_counts_sink(t_env)

    # 3. Execute the 5-minute Tumbling Window logic
    t_env.execute_sql(
        f"""
        INSERT INTO {sink_table}
        SELECT 
            TUMBLE_START(pickup_time, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM {source_table}
        GROUP BY
            TUMBLE(pickup_time, INTERVAL '5' MINUTE),
            PULocationID
        """
    ).wait()

if __name__ == '__main__':
    run_tumbling_window_job()
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Tumble
import os
import json

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(
            APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_source_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                foo DOUBLE
              )
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )


def create_sink_table(table_name, bucket_name):
    return """ CREATE TABLE {0} (
                foo_sum DOUBLE
              )
              WITH (
                  'connector'='filesystem',
                  'path'='s3a://{1}/',
                  'format'='csv',
                  'sink.partition-commit.policy.kind'='success-file',
                  'sink.partition-commit.delay' = '1 min'
              ) """.format(
        table_name, bucket_name)


def tumbling_window_table(table_env, input_table_name):
    return (
        table_env.from_path(input_table_name).window(
            Tumble.over("1.minute")
            .on("APPROXIMATE_ARRIVAL_TIME").alias("one_minute_window")
        )
        .group_by("one_minute_window")
        .select("SUM(foo) as foo_sum")
    )


def main():
    table_env = StreamTableEnvironment.create(
        environment_settings=EnvironmentSettings.new_instance(
        ).in_streaming_mode().use_blink_planner().build()
    )

    props = get_application_properties()
    input_property_map = property_map(props, "consumer.config.0")
    output_property_map = property_map(props, "sink.config.0")

    input_table_name = "input_table"
    output_table_name = "output_table"

    table_env.execute_sql(
        create_source_table(
            input_table_name,
            input_property_map["input.stream.name"],
            input_property_map["aws.region"],
            input_property_map["flink.stream.initpos"]
        )
    )
    table_env.execute_sql(
        create_sink_table(
            output_table_name,
            output_property_map["output.bucket.name"]
        )
    )

    tumbling_window_table(
        table_env,
        input_table_name
    ).execute_insert(
        output_table_name
    ).wait()

    table_env.create_statement_set().execute()


if __name__ == "__main__":
    main()

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
        """CREATE TABLE {0} (
                foo BIGINT
            ) WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
            )""".format(
            input_table_name,
            input_property_map["input.stream.name"],
            input_property_map["aws.region"],
            input_property_map["flink.stream.initpos"])
    )

    table_env.execute_sql(
        """CREATE TABLE {0} (
                bar BIGINT
            ) WITH (
                'connector' = 'filesystem',
                'path' = 's3a://{1}/',
                'format' = 'csv',
                'sink.partition-commit.policy.kind' = 'success-file',
                'sink.partition-commit.delay' = '1 min'
            )""".format(
            output_table_name,
            output_property_map["output.bucket.name"])
    )

    table_env.execute_sql(
        """INSERT INTO {0}
            SELECT foo as bar
            FROM {1}
        """.format(output_table_name, input_table_name)
    )


if __name__ == "__main__":
    main()

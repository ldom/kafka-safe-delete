import argparse

from confluent_kafka.admin import AdminClient

from cli_utils import read_json_input
from safe_delete import topics_safe_delete, topics_recreate
from topic_storage import get_latest_applied, set_latest_applied


UID_TOPIC_NAME = "uids"
READ_TIMEOUT = 2.0

"""
example input JSON
{
    'uid': 17,
    'environment': 'RND',
    'topics_to_recreate': [
        'aaa',
        'bbb'
    ]
}
"""

JSON_INPUT_UID = "uid"
JSON_INPUT_ENV = "environment"
JSON_INPUT_RECREATE_TOPICS = "topics_to_recreate"


def handle_arguments():
    parser = argparse.ArgumentParser(
        description="Reads a JSON file with topics to delete. Returns 0 for success, otherwise 1. "
    )

    parser.add_argument("--json-input",
                        help="JSON input file to read (default = './topics.json'). ",
                        default="./topics.json")

    parser.add_argument("-c", "--config", help="Config properties for connecting to the cluster, in JSON format. "
                                               "Minimum = '{ \"bootstrap.servers\": \"<ip-or-dns-name>:9092\" }'")

    return parser.parse_args()


def main():
    # get the command line arguments
    args = handle_arguments()

    ####################################################################################################
    # client options
    ####################################################################################################
    bootstrap_servers = '192.168.0.129:9092'  # put default server here
    admin_options = {'bootstrap.servers': bootstrap_servers}
    if args.config:
        admin_options = read_json_input(args.config)

    consumer_options = admin_options.copy()
    consumer_options.update({'group.id': 'safe_delete'})

    producer_options = admin_options
    ####################################################################################################

    # read JSON input data
    input_data = read_json_input(args.json_input)

    # read uid from topic
    latest_applied_uid = get_latest_applied(consumer_options, UID_TOPIC_NAME, READ_TIMEOUT)

    # if not the first time, we check that we haven't applied this one yet
    if latest_applied_uid:
        if int(input_data[JSON_INPUT_UID]) <= int(latest_applied_uid):
            print("Already done!")
            return False  # we do nothing if it's been applied already

    # TODO test the env (how to get the current env?)
    # required env received in --> input_data[JSON_INPUT_ENV]

    # apply the actions
    a = AdminClient(admin_options)
    success, results = topics_recreate(a, input_data[JSON_INPUT_RECREATE_TOPICS])

    print(results)

    # write uid in topic
    set_latest_applied(producer_options, UID_TOPIC_NAME, str(input_data['uid']))

    if success:
        return True
    else:
        return False


if __name__ == "__main__":
    main()

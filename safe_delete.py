from collections import namedtuple
from distutils.util import strtobool
import time
from typing import Optional, Tuple

from confluent_kafka.admin import ConfigResource, ConfigSource, NewTopic, RESOURCE_TOPIC, RESOURCE_BROKER
from confluent_kafka import KafkaException, KafkaError


TopicInfo = namedtuple('TopicInfo', ["full_config",
                                     "non_default_config",
                                     "replication_factor",
                                     "partitions"])


def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))


def gather_cluster_info(admin_client):
    return admin_client.list_topics(timeout=10)


def gather_topic_info(admin_client, topic_name) -> Optional[TopicInfo]:  # returns None if the topic does not exist
    fs = admin_client.describe_configs([ConfigResource(RESOURCE_TOPIC, topic_name)])

    topic_config = {}
    topic_non_default_config = {}

    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                topic_config[config.name] = config.value
                if not config.is_default:
                    topic_non_default_config[config.name] = config.value
                # print_config(config, 1)

        except KafkaException as e:
            # print("Failed to describe {}: {}".format(res, e))
            return None
        except Exception:
            raise

    topic_data = admin_client.list_topics(topic_name)
    topic_partitions = topic_data.topics[topic_name].partitions

    replication_factor = len(topic_partitions.get(0).replicas)

    return TopicInfo(full_config=topic_config, non_default_config=topic_non_default_config,
                     partitions=topic_partitions,
                     replication_factor=replication_factor)


def gather_broker_details(admin_client, broker_ids):
    brokers_config = {}

    for b in broker_ids:
        fs = admin_client.describe_configs([ConfigResource(RESOURCE_BROKER, str(b))])
        brokers_config[b] = {}

        # Wait for operation to finish.
        for res, f in fs.items():
            try:
                configs = f.result()
                for config in iter(configs.values()):
                    brokers_config[b][config.name] = config.value
                    # print_config(config, 1)

            except KafkaException as e:
                print("Failed to describe {}: {}".format(res, e))
            except Exception:
                raise

    return brokers_config


def all_partitions_online(topic_partitions):
    return True, ""


def all_brokers_have_delete_topic_enabled(brokers_config):
    not_enabled = []
    all_ok = True
    for broker_id, configs in brokers_config.items():
        if not configs.get("delete.topic.enable"):
            all_ok = False
            not_enabled.append(str(broker_id))

    not_enabled_str = ', '.join(not_enabled)

    return all_ok, not_enabled_str


def auto_create_topics_enabled(brokers_config):
    first_broker = list(brokers_config.values())[0]
    return strtobool(first_broker.get("auto.create.topics.enable", "false"))


def topic_exists(admin_client, topic_name):
    all_topics = admin_client.list_topics()
    topic_info = all_topics.topics.get(topic_name)
    # print(f"topic_info.topics({topic_name}) = {topic_info}")
    return topic_info is not None


def consumer_groups_on_topic():
    return 0


def topics_safe_delete(admin_connection, topic_names, dry_run=False) -> Tuple[bool, dict]:
    results = {}
    success = True

    for topic_name in topic_names:
        ret, msg, topic_info = topic_safe_delete(admin_connection, topic_name, dry_run)
        results[topic_name] = {'success': ret, 'message': msg, 'topic_info': topic_info}
        if not ret:
            success = False

    return success, results


def topic_safe_delete(admin_connection, topic_name, dry_run=False) -> Tuple[bool, str, Optional[TopicInfo]]:
    # print("🧐 gathering cluster and topic information...")
    cluster_info = gather_cluster_info(admin_connection)

    topic_info = gather_topic_info(admin_connection, topic_name)
    broker_ids = list(cluster_info.brokers.keys())
    brokers_config = gather_broker_details(admin_connection, broker_ids)

    # print("checking that the topic exists...")
    if not topic_exists(admin_connection, topic_name):
        # print(f"Topic {topic_name} does not exist")
        return True, f"Topic {topic_name} does not exist", None

    # print("checking auto.create.topics.enable...")
    if auto_create_topics_enabled(brokers_config):
        return False, \
               f"auto.create.topics.enable is set to True!, " \
               f"querying a deleted topic will re-create it (which is not acceptable).", \
               topic_info

    # print("checking consumer groups...")
    nb_consumer_groups = consumer_groups_on_topic()
    if nb_consumer_groups:
        return False, \
               f"there are {nb_consumer_groups} consumer group(s) on topic {topic_name}.", \
               topic_info

    # print("checking all partitions are online...")
    all_online, partitions_not_online = all_partitions_online(topic_info.partitions)
    if not all_online:
        return False, \
               f"not all partitions are online for topic {topic_name}: {partitions_not_online} are offline.", \
               topic_info

    # print("checking that no reassignments are in progress...")

    # print("checking that `delete.topic.enable=true` for all brokers")
    all_enabled, brokers_not_enabled = all_brokers_have_delete_topic_enabled(brokers_config)
    if not all_enabled:
        return False, \
               f"broker(s) {brokers_not_enabled} do(es) not have `delete.topic.enable=true`.", \
               topic_info

    if dry_run:
        return True, "👋 dry run...", topic_info

    # print("💥 deleting topic...")
    admin_connection.delete_topics([topic_name])

    # wait loop until verified that the topic has been removed
    # print("verifying that the topic has been deleted...")
    while topic_exists(admin_connection, topic_name):
        time.sleep(0.2)

    # print(f"Topic {topic_name} has been deleted.")
    return True, f"Topic {topic_name} has been deleted.", topic_info


def topics_recreate(admin_connection, topic_names, dry_run=False) -> Tuple[bool, dict]:
    results = {}
    success = True

    for topic_name in topic_names:
        ret, delete_msg, topic_info = topic_safe_delete(admin_connection, topic_name, dry_run)

        create_msg = ""
        if ret:
            if not topic_info:
                create_msg = "Cannot re-create a topic that doesn't exist."
                ret = False
            else:
                ret, create_msg = topic_create(admin_connection, topic_name,
                                               num_partitions=len(topic_info.partitions),
                                               replication_factor=topic_info.replication_factor,
                                               topic_settings=topic_info.non_default_config,
                                               already_exists_retries=20,
                                               retry_wait=0.5,
                                               )

        results[topic_name] = {'success': ret, 'message': ' '.join([delete_msg, create_msg])}
        if not ret:
            success = False

    return success, results


def topic_create(admin_connection, topic_name, num_partitions, replication_factor, topic_settings,
                 already_exists_retries=10, retry_wait=0.2):
    topic = NewTopic(topic_name,
                     num_partitions=num_partitions,
                     replication_factor=replication_factor,
                     config=topic_settings)
    nb_retries = 0
    while True:
        fs = admin_connection.create_topics([topic])
        for t, f in fs.items():
            try:
                f.result()  # The result itself is None
                # print(f"Topic {t} created with options: {topic_settings}.")
                return True, f"Topic {t} created with options: {topic_settings}."
            except KafkaException as e:
                # print(f"Error creating topic {t}: {e}.")
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    time.sleep(retry_wait)
                    # print(f"wait {retry_wait} s")
                    if nb_retries >= already_exists_retries:
                        # print(f"nb_retries = {nb_retries}, already_exists_retries = {already_exists_retries}")
                        return False, f"Error creating topic {t}: {e} ({nb_retries} tries)."
                    nb_retries += 1
                    continue
                # print(f"Error creating topic {t}: {e}.")
                return False, f"Error creating topic {t}: {e}."

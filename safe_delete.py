from distutils.util import strtobool
import time
from typing import Tuple

from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource, NewTopic, RESOURCE_TOPIC, RESOURCE_BROKER
from confluent_kafka import KafkaException


def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))


def gather_cluster_info(admin_client):
    return admin_client.list_topics(timeout=10)


def gather_topic_info(admin_client, topic_name):
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
            print("Failed to describe {}: {}".format(res, e))
        except Exception:
            raise

    topic_data = admin_client.list_topics(topic_name)
    topic_partitions = topic_data.topics[topic_name].partitions

    return topic_config, topic_non_default_config, topic_partitions


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
    topic_info = admin_client.list_topics(topic_name)
    return len(topic_info.topics[topic_name].partitions) > 0


def consumer_groups_on_topic():
    return 0


def get_topic_config(bootstrap_servers, topic_name):
    print(f"🧐 gathering cluster and topic information...{bootstrap_servers}, {topic_name}")
    a = AdminClient({'bootstrap.servers': bootstrap_servers})
    topic_config, topic_non_default_config, topic_partitions = gather_topic_info(a, topic_name)
    return topic_config, topic_non_default_config


def topics_safe_delete(admin_connection, topic_names, dry_run=False) -> Tuple[bool, dict]:
    results = {}
    success = True

    for topic_name in topic_names:
        ret, msg, config, non_def_config = topic_safe_delete(admin_connection, topic_name, dry_run)
        results[topic_name] = {'success': ret, 'message': msg, 'topic_config': config}
        if not ret:
            success = False

    return success, results


def topic_safe_delete(admin_connection, topic_name, dry_run=False) -> Tuple[bool, str, dict, dict]:
    # print("🧐 gathering cluster and topic information...")
    cluster_info = gather_cluster_info(admin_connection)

    topic_config, topic_non_default_config, topic_partitions = gather_topic_info(admin_connection, topic_name)
    broker_ids = list(cluster_info.brokers.keys())
    brokers_config = gather_broker_details(admin_connection, broker_ids)

    # print("checking that the topic exists...")
    if not topic_exists(admin_connection, topic_name):
        return True, f"Topic {topic_name} does not exist", {}, {}

    # print("checking auto.create.topics.enable...")
    if auto_create_topics_enabled(brokers_config):
        return False, f"auto.create.topics.enable is set to True!, querying a " \
                      f"deleted topic will re-create it (which is not acceptable).", \
               topic_config, topic_non_default_config

    # print("checking consumer groups...")
    nb_consumer_groups = consumer_groups_on_topic()
    if nb_consumer_groups:
        return False, f"there are {nb_consumer_groups} consumer group(s) on topic {topic_name}.", \
               topic_config, topic_non_default_config

    # print("checking all partitions are online...")
    all_online, partitions_not_online = all_partitions_online(topic_partitions)
    if not all_online:
        return False, f"not all partitions are online for topic {topic_name}: {partitions_not_online} are offline.", \
               topic_config, topic_non_default_config

    # print("checking that no reassignments are in progress...")

    # print("checking that `delete.topic.enable=true` for all brokers")
    all_enabled, brokers_not_enabled = all_brokers_have_delete_topic_enabled(brokers_config)
    if not all_enabled:
        return False, f"broker(s) {brokers_not_enabled} do(es) not have `delete.topic.enable=true`.", \
               topic_config, topic_non_default_config

    if dry_run:
        return True, "👋 dry run...", topic_config, topic_non_default_config

    # print("💥 deleting topic...")
    admin_connection.delete_topics([topic_name])

    # wait loop until verified that the topic has been removed
    # print("verifying that the topic has been deleted...")
    while topic_exists(admin_connection, topic_name):
        time.sleep(0.2)

    return True, f"Topic {topic_name} has been deleted.", topic_config, topic_non_default_config


def topics_recreate(admin_connection, topic_names, dry_run=False) -> Tuple[bool, dict]:
    results = {}
    success = True

    for topic_name in topic_names:
        ret, delete_msg, topic_config, topic_non_default_config = topic_safe_delete(admin_connection, topic_name, dry_run)

        create_msg = ""
        if ret:
            ret, create_msg = topic_create(admin_connection, topic_name, topic_non_default_config)

        results[topic_name] = {'success': ret, 'message': ' '.join([delete_msg, create_msg])}
        if not ret:
            success = False

    return success, results


def topic_create(admin_connection, topic_name, topic_settings):
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1, config=topic_settings)
    admin_connection.create_topics([topic])
    return True, f"Topic {topic_name} created with options: {topic_settings}."

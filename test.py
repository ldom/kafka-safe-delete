import time
import unittest

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

from safe_delete import gather_topic_info, topic_exists, topics_recreate, topic_safe_delete, topics_safe_delete
from topic_storage import get_latest_applied, set_latest_applied


class TestDelete(unittest.TestCase):
    bootstrap_servers = '127.0.0.1:9092'
    consumer_options = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test_safe_delete'
    }

    def test_existing(self):
        topic_name = "truc_machin"
        topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        a.create_topics([topic])
        ret, _, _ = topic_safe_delete(admin_connection=a, topic_name=topic_name)
        self.assertTrue(ret)

    def test_multiple(self):
        topic_names = ["truc_machin", "chose"]
        topic1 = NewTopic(topic_names[0], num_partitions=3, replication_factor=1)
        topic2 = NewTopic(topic_names[1], num_partitions=3, replication_factor=1)
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        a.create_topics([topic1, topic2])
        ret, _ = topics_safe_delete(admin_connection=a, topic_names=topic_names)
        self.assertTrue(ret)

    def test_non_existing(self):
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        ret, _, _ = topic_safe_delete(admin_connection=a, topic_name="does_not_exist")
        self.assertTrue(ret)

    def test_latest_applied(self):
        consumer_options = self.consumer_options
        producer_options = {'bootstrap.servers': self.bootstrap_servers}

        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        topic_name = "uids"  # number of partitions = 1, replication as desired

        topic_safe_delete(admin_connection=a, topic_name=topic_name)
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        a.create_topics([topic])

        no_value = get_latest_applied(consumer_options, topic_name)
        self.assertIsNone(no_value)

        set_latest_applied(producer_options, topic_name, "3")

        time.sleep(1)

        a_value = get_latest_applied(consumer_options, topic_name)
        self.assertEqual(a_value, "3")

    def test_recreate(self):
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        topic_names = ["test_4", "test_5"]

        # make sure they don't exist
        topics_safe_delete(admin_connection=a, topic_names=topic_names)

        # then create them
        topic1 = NewTopic(topic_names[0], num_partitions=6, replication_factor=1, config={
            "compression.type": "snappy",
            "max.message.bytes": "123456"
        })
        topic2 = NewTopic(topic_names[1], num_partitions=3, replication_factor=1)

        print(f"create {topic_names}")
        a.create_topics([topic1, topic2])
        time.sleep(2)

        ret, _ = topics_recreate(admin_connection=a, topic_names=topic_names)
        self.assertTrue(ret)

        # get the topic1 config
        topic1_info = gather_topic_info(a, topic_names[0])

        self.assertEqual(len(topic1_info.partitions), 6)
        self.assertEqual(topic1_info.full_config.get("max.message.bytes"), "123456")
        self.assertEqual(topic1_info.non_default_config.get("compression.type"), "snappy")

        # cleanup
        ret, _ = topics_safe_delete(admin_connection=a, topic_names=topic_names)
        self.assertTrue(ret)

    def test_recreate_fail_if_not_exist(self):
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        topic_names = ["test_3"]
        ret, _ = topics_recreate(admin_connection=a, topic_names=topic_names)
        self.assertFalse(ret)

import time
import unittest

from confluent_kafka.admin import AdminClient, NewTopic

from safe_delete import topic_safe_delete, topics_safe_delete
from topic_storage import get_latest_applied, set_latest_applied


class TestDelete(unittest.TestCase):
    bootstrap_servers = '192.168.0.129:9092'
    consumer_options = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test_safe_delete'
    }

    def test_existing(self):
        topic_name = "truc_machin"
        topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)
        a = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        a.create_topics([topic])
        ret, _ = topic_safe_delete(admin_connection=a, topic_name=topic_name)
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
        ret, _ = topic_safe_delete(admin_connection=a, topic_name="does_not_exist")
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

    def test_read_json(self):
        pass

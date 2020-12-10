import unittest

from confluent_kafka.admin import AdminClient, NewTopic

from app.lib.safe_delete import topic_safe_delete, topics_safe_delete


class TestDelete(unittest.TestCase):
    bootstrap_servers = '192.168.0.129:9092'

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
        bootstrap_servers = '192.168.0.129:9092'
        a = AdminClient({'bootstrap.servers': bootstrap_servers})
        ret, _ = topic_safe_delete(admin_connection=a, topic_name="does_not_exist")
        self.assertTrue(ret)

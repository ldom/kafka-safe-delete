from confluent_kafka import Consumer, Producer, TopicPartition


def get_latest_applied(client_options, topic_name, read_timeout=1.0):
    client_options.update({
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
    })
    c = Consumer(client_options)

    partition = TopicPartition(topic_name, 0)
    low, high = c.get_watermark_offsets(partition)

    if low is not None and high is not None and high > 0:
        last_msg_offset = high - 1
    else:
        last_msg_offset = 0

    partition = TopicPartition(topic_name, 0, last_msg_offset)
    c.assign([partition])

    read = None

    msg = c.consume(num_messages=1, timeout=read_timeout)
    if msg:
        read = msg[0].value().decode('utf-8')
        # print('Read: {}'.format(read))

    c.close()
    return read


def set_latest_applied(client_options, topic_name, data):
    p = Producer(client_options)

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {}, partition {}'.format(msg.topic(), msg.partition()))

    p.poll(0)
    p.produce(topic_name, data.encode('utf-8'), callback=delivery_report)
    p.flush()

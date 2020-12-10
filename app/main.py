import os

from flask import Flask

from lib.safe_delete import topic_safe_delete, get_topic_config


app = Flask(__name__)

# 192.168.65.2
# host.docker.internal
bootstrap_servers = os.environ.get('KAFKA_SRV', 'host.docker.internal')


@app.route("/")
def hello():
    print(f"env KAFKA_SRV: {bootstrap_servers}")
    return "Hello World from Flask"


@app.route("/topics/<topic>", methods=['DELETE'])
def delete_topic(topic):
    return topic_safe_delete(bootstrap_servers, topic)


@app.route("/topics/<topic>", methods=['GET'])
def topic_info(topic):
    return get_topic_config(bootstrap_servers, topic)


if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True, port=80)

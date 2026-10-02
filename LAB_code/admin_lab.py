from confluent_kafka.admin import AdminClient, NewTopic

config = {
    'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)
topic_name = 'gutenberg-book'

# Création du topic
admin_client.create_topics([
    NewTopic(topic_name, num_partitions=1, replication_factor=1)
])

# Vérification
x = admin_client.list_topics(timeout=10)
print("Topics existants :")
for t in x.topics.keys():
    print(f"- {t}")
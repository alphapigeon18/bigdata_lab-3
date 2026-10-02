import string
from pathlib import Path
from confluent_kafka import Consumer

# Configuration du consommateur
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'book-cleaner-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'gutenberg-book'
consumer.subscribe([topic])

# Définition et création automatique du dossier 'output'
current_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
output_dir = current_dir / 'output'
output_dir.mkdir(parents=True, exist_ok=True)

output_file_path = output_dir / 'cleaned_book.txt'
MAX_EMPTY_POLLS = 10  
empty_polls = 0

print(f"Consumer started on '{topic}'.")
print(f"Writing cleaned data to : {output_file_path}...")

with open(output_file_path, 'w', encoding='utf-8') as out_f:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("No more messages to consume.")
                break
            continue

        if msg.error():
            print(f"Consumer error : {msg.error()}")
            continue

        empty_polls = 0
        raw_text = msg.value().decode('utf-8')

        text_lower = raw_text.lower()
        clean_text = text_lower.translate(str.maketrans('', '', string.punctuation))
        words = clean_text.split()

        if words:
            out_f.write(" ".join(words) + "\n")

consumer.close()
print("Consumer stopped gracefully.")
import socket
import time
from pathlib import Path
from confluent_kafka import Producer

# Configuration du producteur
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)
topic = 'gutenberg-book'

# Chemins des dossiers et fichiers
current_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
book_dir = current_dir / 'book'

txt_files = list(book_dir.glob('*.txt'))
if not txt_files:
    raise FileNotFoundError(f"No .txt files found in directory: {book_dir}")

file_path = txt_files[0]
print(f"Reading from : {file_path}")
print(f"Starting to send to Kafka topic '{topic}'...")

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        cleaned_line = line.strip()
        if cleaned_line:  
            producer.produce(topic=topic, value=cleaned_line.encode('utf-8'))
            producer.poll(0)
            time.sleep(0.01)

producer.flush()
print("End of book sending.")
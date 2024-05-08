from hdfs import InsecureClient
from confluent_kafka import Producer
import json
import pandas as pd

def delivery_report(err, msg):
    """
    Fonction de rappel pour gérer les retours de livraison des messages Kafka.
    """
    if err is not None:
        print('Échec de livraison du message : {}'.format(err))
    else:
        print('Message livré pour le prétraitement')

def produce_sensor_data(producer, topic, file_name, file_content):
    """
    Fonction pour produire des données de capteur vers Kafka.
    """
    group = file_name[2:4]

    message = {
        "file_name": file_name,
        "content": file_content,
        "group": group
    }
   
    producer.produce(topic, key=file_name, value=json.dumps(message), callback=delivery_report)
    producer.poll(0)
    producer.flush()

# Remplacez avec les détails de votre cluster HDFS
hdfs_url = "http://localhost:9870"
data_lake_path = "data_lake/parkinson_data"

client = InsecureClient(hdfs_url)

# Liste des fichiers dans le répertoire Data Lake
files_in_data_lake = client.list(data_lake_path)

# Configuration du producteur Kafka
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Création de l'instance du producteur Kafka
producer = Producer(producer_conf)

# Lire le contenu de chaque fichier
for file_name in files_in_data_lake:
    hdfs_file_path = f"{data_lake_path}/{file_name}"
    
    with client.read(hdfs_file_path, encoding='utf-8') as reader:
        file_content = reader.read()
    
    for line in file_content.split("\n"):
        if line == "":
            continue
        produce_sensor_data(producer, "sensor_data", file_name.split(".")[0], line)

    break
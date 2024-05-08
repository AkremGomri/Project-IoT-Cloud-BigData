from confluent_kafka import Consumer, KafkaError
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from pyspark.sql.functions import mean
import numpy as np
from cassandra.cluster import Cluster

class Cassandra:
    def __init__(self, create_keyspace_command, data):
        """
        Initialiser une connexion à Cassandra, créer un espace de clés et une table,
        puis insérer des données dans la table.
        """
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute(create_keyspace_command)
        self.session.execute("USE Parkinson")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS data (
                L1 FLOAT,
                L2 FLOAT,
                L3 FLOAT,
                L4 FLOAT,
                L5 FLOAT,
                L6 FLOAT,
                L7 FLOAT,
                L8 FLOAT,
                R1 FLOAT,
                R2 FLOAT,
                R3 FLOAT,
                R4 FLOAT,
                R5 FLOAT,
                R6 FLOAT,
                R7 FLOAT,
                R8 FLOAT,
                L FLOAT,
                R FLOAT,
                Class INT,
            )
        """)
        self.session.execute("TRUNCATE data")
        self.insert_data(data)

    def insert_data(self, data):
        """
        Insérer des données dans la table Cassandra.
        """
        for d in data:
            self.session.execute(f"""
                INSERT INTO data (
                    L1, L2, L3, L4, L5, L6, L7, L8, R1, R2, R3, R4, R5, R6, R7, R8, L, R, Class
                ) VALUES (
                    {d[0]}, {d[1]}, {d[2]}, {d[3]}, {d[4]}, {d[5]}, {d[6]}, {d[7]}, {d[8]}, {d[9]}, {d[10]}, {d[11]}, {d[12]}, {d[13]}, {d[14]}, {d[15]}, {d[16]}, {d[17]}, {d[18]}
                )
            """)

    def close(self):
        """
        Fermer la connexion Cassandra.
        """
        self.cluster.shutdown()

def retrieve_data(consumer, topic):
    """
    Récupérer les données à partir d'un topic Kafka.
    """
    consumer.subscribe([topic])
    patient_data = dict()
    print("En attente de messages...")
    first_message = False
    count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if first_message:
                break
            else:
                continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        try:
            first_message = True
            data = json.loads(msg.value())
            row = data["content"].split(";")
            if len(row) == 19:
                row = [float(x) for x in row]
                row.append(0 if data["group"] == "Co" else 1)
                if data["file_name"] not in patient_data:
                    patient_data[data["file_name"]] = []
                patient_data[data["file_name"]].append(row)
            
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON : {e}")
        except KeyError as e:
            print(f"Clé manquante dans le JSON : {e}")

    consumer.close()
    print("Terminé")
    return patient_data

def preprocess_data(patient_data):
    """
    Prétraiter les données en calculant la moyenne pour chaque groupe de 100 lignes.
    """
    spark = SparkSession.builder.appName('PatientData').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([ 
        StructField("Time", FloatType(), True),  
        StructField("L1", FloatType(), True), 
        StructField("L2", FloatType(), True), 
        StructField("L3", FloatType(), True),
        StructField("L4", FloatType(), True),
        StructField("L5", FloatType(), True),
        StructField("L6", FloatType(), True),
        StructField("L7", FloatType(), True),
        StructField("L8", FloatType(), True),
        StructField("R1", FloatType(), True), 
        StructField("R2", FloatType(), True), 
        StructField("R3", FloatType(), True),
        StructField("R4", FloatType(), True),
        StructField("R5", FloatType(), True),
        StructField("R6", FloatType(), True),
        StructField("R7", FloatType(), True),
        StructField("R8", FloatType(), True),
        StructField("L", FloatType(), True),
        StructField("R", FloatType(), True),
        StructField("Class", IntegerType(), True)
    ])

    for patient in patient_data:
        mean_values = []
        patient_data[patient] = spark.createDataFrame(patient_data[patient], schema)
        lenght = patient_data[patient].count()

        for i in range(0, lenght, 100):
            df_with_row_number = patient_data[patient].withColumn("row_number", row_number().over(Window.orderBy("Time")))

            if i+100 > lenght:
                end = lenght
            else:
                end = i+100
            result_df = df_with_row_number.filter((col("row_number") >= i) & (col("row_number") < end))

            result_df = result_df.drop("row_number")

            mean_values.append(np.asarray(result_df.select(mean(result_df.L1), mean(result_df.L2), mean(result_df.L3), \
                            mean(result_df.L4), mean(result_df.L5), mean(result_df.L6), \
                            mean(result_df.L7), mean(result_df.L8), mean(result_df.R1), \
                            mean(result_df.R2), mean(result_df.R3), mean(result_df.R4), \
                            mean(result_df.R5), mean(result_df.R6), mean(result_df.R7), \
                            mean(result_df.R8), mean(result_df.L), mean(result_df.R), \
                            mean(result_df.Class)).collect()).tolist()[0])
        
        patient_data[patient] = mean_values

    return patient_data

# Configuration du consommateur Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

# Création de l'instance du consommateur Kafka
consumer = Consumer(consumer_conf)

patient_data = retrieve_data(consumer, "sensor_data")

preprocessed_data = preprocess_data(patient_data)

# Définition des paramètres de l'espace de clés Cassandra
keyspace_name = 'ParkinsonData'
replication_strategy = 'SimpleStrategy'
replication_factor = 3

# Création de la requête de création de l'espace de clés
create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {{'class': '{replication_strategy}', 'replication_factor': {replication_factor}}};
"""

# Initialisation de l'instance Cassandra, insertion des données, sélection des données et fermeture de la connexion
cassandra = Cassandra(create_keyspace_query, preprocessed_data)
cassandra.close()
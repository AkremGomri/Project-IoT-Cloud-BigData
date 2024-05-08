import paho.mqtt.client as mqtt
import time
import json
from hdfs import InsecureClient

def on_connect(client, userdata, flags, rc):
    """
    Fonction de rappel pour gérer la connexion au courtier MQTT.
    """
    if rc == 0:
         print("Connecté au broker")
         global Connected                
         Connected = True               
    else:
         print("Échec de la connexion")

def on_message(client, userdata, message):
    """
    Fonction de rappel pour gérer les messages MQTT.
    """
    json_object = json.loads(str(message.payload.decode("utf-8")).replace("'", '"'))
    # Stocker les valeurs de données du message dans une chaîne
    data_values = ""
    for key, value in json_object.items():
        if key != "Patient":
            data_values += str(value) + ";"
    data_values = data_values[:-1]
    # Enregistrer la chaîne dans un fichier CSV avec la clé comme nom de colonne du fichier
    local_file = json_object["Patient"]+'.csv'
    with open("csv data/"+local_file, 'a') as file: # Si le fichier n'existe pas dans HDFS on le sauvegarde
        file.write(data_values + "\n")

    hdfs_file_path = f"{data_lake_path}/{local_file}"
    if not hdfs_client.status(hdfs_file_path, strict=False):
        hdfs_client.upload(hdfs_file_path, "csv data/"+local_file)
    
    print("Données enregistrées dans le datalake HDFS")

Connected = False
 
broker_address = "broker.hivemq.com"
port = 1883                   
 
hdfs_url = "http://localhost:9870"
hdfs_client = InsecureClient(hdfs_url)
data_lake_path = "data_lake/parkinson_data"
if not hdfs_client.status(data_lake_path, strict=False): # Si le répertoire n'existe pas dans HDFS on le crée
    hdfs_client.makedirs(data_lake_path)
 
print("Création d'une nouvelle instance")
client = mqtt.Client("python_test")
client.on_message = on_message          # Attacher la fonction au rappel
client.on_connect = on_connect
print("Connexion au broker")
client.connect(broker_address, port)  # Connexion au broker
client.loop_start()                   # Démarrer la boucle
 
while not Connected:                  # Attendre la connexion
    time.sleep(0.1)
 
print("Abonnement au sujet", "test/parkinson")
client.subscribe("test/parkinson")
 
try:
    while True: 
        time.sleep(1)

except KeyboardInterrupt:
    print("Sortie")
    client.disconnect()
    client.loop_stop()

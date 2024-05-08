import requests
from cassandra.cluster import Cluster
import numpy as np
import pymongo

class Cassandra:
    def __init__(self):
        """
        Initialiser une connexion à Cassandra et utiliser l'espace de clés Parkinson.
        """
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute("USE Parkinson")

    def get_data(self):
        """
        Récupérer les données à partir de la table Cassandra.
        """
        return np.asarray(self.session.execute("SELECT * FROM data"))

    def close(self):
        """
        Fermer la connexion à Cassandra.
        """
        self.cluster.shutdown()

url = 'http://127.0.0.1:5000/api/model'

# Initialiser la connexion à Cassandra, récupérer les données et fermer la connexion
cassandra = Cassandra()
dataset = cassandra.get_data()
cassandra.close()

# Envoyer les données au serveur Flask
response = requests.post(url, files={'data': dataset})

# Initialiser la connexion à MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb = myclient["mydatabase"]
mycol = mydb["parkinson"]

# Insérer un document de test, puis le supprimer
mycol.insert_one({"test": "test"})
mycol.drop()

# Réinitialiser la collection
mycol = mydb["parkinson"]

# Vérifier si la base de données existe
dblist = myclient.list_database_names()
if "mydatabase" in dblist:
    print("La base de données existe.")
else:
    print("La base de données n'existe pas.")

# Insérer le résultat de la requête Flask dans MongoDB
mycol.insert_one(response.json())

print("Modèle et métriques d'évaluation sauvegardés dans MongoDB.")
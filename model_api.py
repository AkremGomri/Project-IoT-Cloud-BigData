from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve, auc
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from mlxtend.evaluate import bias_variance_decomp
from sklearn.neighbors import KNeighborsClassifier
import pickle

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", "allow_headers": {"Access-Control-Allow-Origin"}}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/api/model', methods=['POST'])
@cross_origin(origin='*', headers=['content-type'])
def model():
    """
    API endpoint pour entraîner un modèle KNN sur les données fournies.
    """
    if request.method == 'POST':
        data = request.files.get('data')
        columns_name = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "L", "R", 'Class']
        df = pd.DataFrame(data)

        X = df.drop('Class', axis=1)
        y = df['Class']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        clf = KNeighborsClassifier(n_neighbors=5)
        clf.fit(X_train, y_train)

        # sauvegarder le modèle avec pickle
        filename = 'knn.sav'
        pickle.dump(clf, open(filename, 'wb'))

        # charger le modèle avec pickle
        loaded_model = pickle.load(open(filename, 'rb'))
        
        y_pred = clf.predict(X_test)

        # Évaluer les performances du classifieur
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='weighted')
        recall = recall_score(y_test, y_pred, average='weighted')
        f1 = f1_score(y_test, y_pred, average='weighted')

        # Courbe ROC pour une classification multi-classe
        y_prob = clf.predict_proba(X_test).argmax(axis=1)
        macro_roc_auc_ovo = roc_auc_score(y_test.to_numpy(), y_prob, multi_class="ovo", average="macro")

        # Matrice de confusion
        cm = confusion_matrix(y_test, y_pred)

        # Obtenir les valeurs TP, TN, FP, FN
        FP = cm.sum(axis=0) - np.diag(cm)  
        FN = cm.sum(axis=1) - np.diag(cm)
        TP = np.diag(cm)
        TN = cm.sum() - (FP + FN + TP)

        # Obtenir le biais et la variance du classifieur
        loss, bias, var = bias_variance_decomp(clf, X_train, y_train.to_numpy(), X_test, y_test.to_numpy(), loss='0-1_loss', random_seed=23)

        return jsonify({'model': filename,
                        'accuracy': accuracy,
                        'precision': precision,
                        'recall': recall,
                        'f1': f1,
                        'macro_roc_auc_ovo': macro_roc_auc_ovo,
                        'confusion_matrix': cm,
                        'TP': TP,
                        'TN': TN,
                        'FP': FP,
                        'FN': FN,
                        'bias': bias,
                        'variance': var,
                        'loss': loss})

if __name__ == '__main__':
    app.run(debug=True)
import os
import sys
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score

import pandas as pd
import numpy as np 

DIR_ARTIFACT = 'artifacts'
PATH_BASENAME = os.path.dirname(os.path.abspath(__file__))
PATH_RELATIVE_ROOT = '../'
PATH_ROOT = os.path.realpath(os.path.join(PATH_BASENAME, PATH_RELATIVE_ROOT))
sys.path.append(PATH_ROOT)


@task
def download_dataset_task()->str:

    context = get_current_context()
    run_id = context['run_id']
    print("run_id:", run_id)
    print("new2")


    iris = load_iris()
    iris = pd.DataFrame(
        data=np.c_[iris['data'], iris['target']],
        columns = iris['feature_names'] + ['target']
    )

    file_dir = os.path.join(PATH_ROOT, DIR_ARTIFACT, run_id)
    os.makedirs(file_dir, exist_ok=True)
    file_path = os.path.join(file_dir, "00_iris_dataset.csv")

    iris.to_csv(file_path, index=False)
    return file_path

@task
def data_processing_task(path_data:str):

    context = get_current_context()
    run_id = context['run_id']
    print("run_id:", run_id)

    final = pd.read_csv(path_data)
    cols = ["sepal length (cm)","sepal width (cm)","petal length (cm)","petal width (cm)"]
    final[cols] = final[cols].fillna(final[cols].mean())

    file_dir = os.path.join(PATH_ROOT, DIR_ARTIFACT, run_id)
    file_path = os.path.join(file_dir, "01_iris_dataset_clean.csv")

    final.to_csv(file_path, index=False)
    return file_path

@task
def ml_training_RandomForest_task(path_data)-> dict[float]:

    final = pd.read_csv(path_data)
    X_train, X_test, y_train, y_test = train_test_split(final.iloc[:,0:4],final.iloc[:,-1], test_size=0.3)
    clf = RandomForestClassifier(n_estimators = 100)  
    clf.fit(X_train, y_train)
  
    # performing predictions on the test dataset
    y_pred = clf.predict(X_test)
     
    # using metrics module for accuracy calculation
    print("ACCURACY OF THE MODEL: ", accuracy_score(y_test, y_pred))
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}


@task
def ml_training_Logisitic_task(path_data)-> dict[float]:

    final = pd.read_csv(path_data)
    X_train, X_test, y_train, y_test = train_test_split(final.iloc[:,0:4],final.iloc[:,-1], test_size=0.3)
    logistic_regression = LogisticRegression(multi_class="ovr")
    lr = logistic_regression.fit(X_train, y_train)

    y_pred = lr.predict(X_test)

    print("ACCURACY OF THE MODEL: ", accuracy_score(y_test, y_pred))
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}

@task
def identify_best_model_task(acc_rf, acc_log):
    best_model = 'randomForest' if acc_rf > acc_log else 'Logistic'
    score_model = acc_rf if acc_rf > acc_log else acc_log
    print(f'The best model is [{best_model}]: {score_model}')
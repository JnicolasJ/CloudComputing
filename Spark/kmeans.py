from __future__ import print_function

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn.datasets.samples_generator import make_blobs
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("KMeansExample")\
        .getOrCreate()

    n_samples=100000
    n_features=3
    X, y = make_blobs(n_samples=n_samples, centers=10, n_features=n_features, random_state=42)

# agregar un índice de fila como una cadena
    pddf = pd.DataFrame(X, columns=['x', 'y', 'z'])
    pddf['id'] = 'row'+pddf.index.astype(str)

#mover el primero a la izquiwerda
    cols = list(pddf)
    cols.insert(0, cols.pop(cols.index('id')))
    pddf = pddf.loc[:, cols]
    pddf.head()

# guardar ndarray como un csv
    pddf.to_csv('input.csv', index=False)
    threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
    threedee.scatter(X[:,0], X[:,1], X[:,2], c=y)
    threedee.set_xlabel('x')
    threedee.set_ylabel('y')
    threedee.set_zlabel('z')
    plt.savefig('data_kmeans.png')

    sqlContext = SQLContext(sc)
    FEATURES_COL = ['x', 'y', 'z']
    path = 'input.csv'
    df = sqlContext.read.csv(path, header=True) # requires spark 2.0
    #df.show()

    lines = sc.textFile(path)
    data = lines.map(lambda line: line.split(","))
    data.take(2)

    df = data.toDF(['id', 'x', 'y', 'z'])
    print (df)

    df_feat = df.select(*(df[c].cast("float").alias(c) for c in df.columns[1:]))
    #df_feat.show()

    for col in df.columns:
        if col in FEATURES_COL:
            df = df.withColumn(col,df[col].cast('float'))
    #df.show()

    df = df.na.drop()
    #df.show()

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(df).select('id', 'features')
    #df_kmeans.show()

    cost = np.zeros(20)
    for k in range(2,20):
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans.sample(False,0.1, seed=42))
        cost[k] = model.computeCost(df_kmeans) # requires Spark 2.0 or later

    fig, ax = plt.subplots(1,1, figsize =(8,6))
    ax.plot(range(2,20),cost[2:20])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')

    #entrenamos el modelo
    k = 10
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)

    transformed = model.transform(df_kmeans).select('id', 'prediction')
    rows = transformed.collect()
    print(rows[:3])
    df_pred = sqlContext.createDataFrame(rows)
    #df_pred.show()

    df_pred = df_pred.join(df, 'id')
    #df_pred.show()
    pddf_pred = df_pred.toPandas().set_index('id')
    pddf_pred.head()

    threedee = plt.figure(figsize=(12,10)).gca(projection='3d')
    threedee.scatter(pddf_pred.x, pddf_pred.y, pddf_pred.z, c=pddf_pred.prediction)
    threedee.set_xlabel('x')
    threedee.set_ylabel('y')
    threedee.set_zlabel('z')
    plt.savefig('resultado.png')
    sc.stop()
    spark.stop()

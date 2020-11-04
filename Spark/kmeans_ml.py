from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("KMeansExample")\
        .getOrCreate()
    # Cargar data
    dataset = spark.read.format("libsvm").load("S3://sparkcloudjimenez/data/simple_kmeans_data.txt")
    # Entrenar el modelo
    kmeans = KMeans().setK(2).setSeed(1)
    model = kmeans.fit(dataset)
    # Ejecutar predicciones
    predictions = model.transform(dataset)

    # Evaluar el Cluster con computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette con distancia euclidiana al cuadrado = " + str(silhouette))

    # Monstrar resultados
    centers = model.clusterCenters()
    print("Centroides del cluster: ")
    for center in centers:
        print(center)

    spark.stop()

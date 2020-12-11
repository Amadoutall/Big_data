# import package
from pyspark import SparkContext

# Creation de la session sparkContext
sc = SparkContext(master="local",appName="Spark Demo")
# importation du fichier sample.txt
file = sc.textFile("sample.txt")
# regroupement mot par mot
words = file.flatMap(lambda line : line.split(" "))
wordsCounts= words.countByValue()
# affichage
for word, count in wordsCounts.items():
    print("{} :{}".format(word,count))


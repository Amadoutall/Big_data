###### TP1: Outils BIG DATA ######
# Noms des membres du groupe
# AMADOU Tall
# BAMBOU Regis Aymar

# import package
from pyspark import SparkContext,SparkConf

# Creation de la session sparkContext
conf= SparkConf().setAppName("collect").setMaster("local[*]")
sc = SparkContext(conf=conf)
# importation du fichier sample.txt
file = sc.textFile("sample.txt")

# regroupement mot par mot
words = file.flatMap(lambda line : line.split(" "))
wordsCounts= words.map(lambda line:(line,1)).reduceByKey(lambda a,b: a+b)

# affichage

for word in wordsCounts.collect():
    print("{} ".format(word))

# exportation
#wordsCounts.coalesce(1).saveAsTextFile('base.text')


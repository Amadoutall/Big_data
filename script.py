# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 22:57:33 2021

@author: Amadou tall, Régis bambou
"""
# importation des packages
import findspark
findspark.init("C:/spark")

from pyspark import SparkContext

import numpy as np
import random
import time
import math 
import pandas as pd
  


# session sparkContext
sc = SparkContext(master="local",appName="Spark Demo")

# function qui permet de verifier si un point est dans le cercle ou pas
def is_point_inside_unit_circle(p):
    
    x, y = random.random(), random.random()   # simuler deux point  x et y
    return 1 if x*x + y*y < 1 else 0 # vérifier si ces deux point sont dans la cercle


# permet d'estimer pi en utilisant les Rdds de spark
def pi_estimator_spark(n):
    tot_in=0
    count = sc.parallelize(range(0, n))
    test=count.map(is_point_inside_unit_circle).collect()
    #print(test)
    for i in range(n):
    
        if test[i]==1:
           tot_in += 1 
    
    # calcul de pi

    pi=(4*tot_in)/n
    print("valeur de pi =",pi)
    return pi


# permet d'estimer pi en utilisant numpy
def pi_estimator_numpy(n):
    tot_in=0
    tot_out=0
    for i in range(n):
        test=is_point_inside_unit_circle(n)
        if test==1:
            tot_in+=1

    # calcul de pi

    pi=(4*tot_in)/n
    print("valeur de pi =",pi)
    return pi


# permet d'afficher les estimations de pi en utilisant les deux fonctions 
def affichage_tab(n):
    #spark
   
    start_time_spark = time.time()
    pi_spark =pi_estimator_spark(n)
    interval_spark = time.time() - start_time_spark  
    # numpy
    start_time_numpy = time.time()
    pi_numpy=pi_estimator_numpy(n)
    interval_numpy = time.time() - start_time_numpy 
    
    aff={"N = "+str(n):["estimation pi","Ecart % Math.pi","Total time in seconds"],"spark":[pi_spark,pi_spark-math.pi,interval_spark],"numpy":[pi_numpy,pi_numpy-math.pi,interval_numpy]}
    aff=pd.DataFrame(aff)
    display(aff)
    
# cas pour n=100000
n=100000
random.seed(135)
affichage_tab(n)

# cas pour n=1000000
n=1000000
random.seed(135)
affichage_tab(n)

sc.stop()


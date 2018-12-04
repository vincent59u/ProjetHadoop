#!/bin/bash

#Clean & Install le projet
mvn clean install

#Ajoute les fichier de data dans le HDFS d'Hadoop
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_products_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_customers_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_geolocation_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_order_items_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_order_payments_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_order_reviews_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_orders_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/olist_sellers_dataset.csv

#Suppression du fichier dans le HDFS
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question4

#Lancement des classe java
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question4.Question olist_products_dataset.csv

#Suppression du fichier dans le projet
rm -Rf ./output/question4/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question4 ./output/



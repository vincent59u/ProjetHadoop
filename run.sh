#!/bin/bash

#Clean & Install le projet
mvn -DskipTests clean install

#Lancement du HDFS 
/usr/local/Cellar/hadoop/3.1.1/sbin/start-dfs.sh

#Suppression du safemode
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfsadmin -safemode leave

#Clean le header de chaque csv
#sed '1d' ./data/olist_customers_dataset.csv > ./data/cleaned/olist_customers_dataset.csv
#sed '1d' ./data/olist_geolocation_dataset.csv > ./data/cleaned/olist_geolocation_dataset.csv
#sed '1d' ./data/olist_order_items_dataset.csv > ./data/cleaned/olist_order_items_dataset.csv
#sed '1d' ./data/olist_order_payments_dataset.csv > ./data/cleaned/olist_order_payments_dataset.csv
#sed '1d' ./data/olist_order_reviews_dataset.csv > ./data/cleaned/olist_order_reviews_dataset.csv
#sed '1d' ./data/olist_orders_dataset.csv > ./data/cleaned/olist_orders_dataset.csv
#sed '1d' ./data/olist_products_dataset.csv > ./data/cleaned/olist_products_dataset.csv
#sed '1d' ./data/olist_sellers_dataset.csv > ./data/cleaned/olist_sellers_dataset.csv
#sed '1d' ./data/product_category_name_translation.csv > ./data/cleaned/product_category_name_translation.csv

#Ajoute les fichier de data dans le HDFS d'Hadoop
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_products_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_customers_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_geolocation_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_order_items_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_order_payments_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_order_reviews_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_orders_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/olist_sellers_dataset.csv
#/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -put ./data/cleaned/centroids.csv

#Suppression du fichier dans le HDFS
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question1
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question2
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question3
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question4
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question5
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -rm -r -f output/question5-centroids

#Lancement des classe java
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question1.Question olist_products_dataset.csv olist_order_items_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question2.Question olist_customers_dataset.csv olist_sellers_dataset.csv olist_geolocation_dataset.csv olist_orders_dataset.csv olist_order_items_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question3.Question olist_orders_dataset.csv olist_order_reviews_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question4.Question olist_products_dataset.csv
/usr/local/Cellar/hadoop/3.1.1/bin/hadoop jar ./target/Question-1.0-SNAPSHOT.jar fr.miage.matthieu.question5.Question customer_order_location-r-00000 centroids.csv

#Suppression du fichier dans le projet
rm -Rf ./output/question1/
rm -Rf ./output/question2/
rm -Rf ./output/question3/
rm -Rf ./output/question4/
rm -Rf ./output/question5/
rm -Rf ./output/question5-centroids/

#Récupération des données dans le HDFS
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question1 ./output/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question2 ./output/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question3 ./output/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question4 ./output/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question5/ ./output/
/usr/local/Cellar/hadoop/3.1.1/bin/hdfs dfs -get output/question5-centroids/ ./output/

#Fermeture du HDFS 
/usr/local/Cellar/hadoop/3.1.1/sbin/stop-dfs.sh
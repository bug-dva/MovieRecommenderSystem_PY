###Movie Recommender System - MapReduce


**What is a movie recommender system?**

There are two types of recommendation for users. 

* Systems that attempt to predict item that users may be interested in. e.g. Amazon products recommendation.
* Systems that help people find information that may interest them. e.g. search engine

This project only help to predict what users might be interested in.
**Dataset**
**Algorithm**

In general, we are using item-item Collabrative Filtering. The simple logic is shown below:

- Build two matrixes, a rating matrix and a co-occurence matrix
- Normalization
- Mutiply two matrixes
- Sum up for each user
- Give out recommendations
**Steps**
**How to run this project?**
Configure your Mac or Linux

- Docker installed
- You are familiar with github, and command lines

Open you terminal

```
$ mkdir bigdata-class2
$ cd bigdata-class2 # create a file
$ sudo docker pull joway/hadoop-cluster # pull mirrow from a onine source
$ git clone https://github.com/joway/hadoop-cluster-docker # pull the template for starting hadoop
$ sudo docker network create --driver=bridge hadoop #build a bridge for communication between networks
$ cd hadoop-cluster-docker

```
go to your container

```
$ sudo ./start-container.sh
```

start hadoop

```
$ ./start-hadoop.sh
```

Clone this project, and go to the RecommenderSystem folder

```
cd RecommenderSystem
hdfs dfs -mkdir /input
hdfs dfs -put input/* /input  #upload user's rating document
hdfs dfs -rm -r /dataDividedByUser
hdfs dfs -rm -r /coOccurrenceMatrix
hdfs dfs -rm -r /Normalize
hdfs dfs -rm -r /Multiplication
hdfs dfs -rm -r /Sum
cd src/main/java/
hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class
hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum
hdfs dfs -cat /Sum/*
```

- args0: original dataset
- args1: output directory for DividerByUser job
args2: output directory for coOccurrenceMatrixBuilder job
- args3: output directory for Normalize job
- args4: output directory for Multiplication job
- args5: output directory for Sum job

```
hdfs dfs -ls
```




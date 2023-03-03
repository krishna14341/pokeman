FROM ubuntu:latest
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y wget
RUN wget https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz   
RUN tar xvf spark-3.3.2-bin-hadoop3.tgz   
RUN mv spark-3.3.2-bin-hadoop3.tgz  /usr/local/spark
RUN apt-get install -y python3.9
RUN apt-get install -y python3-pip
RUN pip3 install pyspark

ENV SPARK_HOME=/spark-3.3.2-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin


# Expose Spark UI and Jupyter Notebook port
# EXPOSE 4040 8888 8080

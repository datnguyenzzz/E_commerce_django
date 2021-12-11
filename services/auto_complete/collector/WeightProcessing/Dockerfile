FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

ARG AVRO_LIBPATH=avro-tools-1.8.2.jar
ARG SCHEMA_FILE=word-schema.avsc

# Map Reduce jobs
COPY WeightCompute.java /HADOOP_TASKS/WeightCompute/WeightCompute.java
COPY WeightSort.java /HADOOP_TASKS/WeightSort/WeightSort.java

# Avro tool
COPY avro/ /HADOOP_TASKS/avro/

#compile schema to class 
RUN cd /HADOOP_TASKS/avro && java -jar $AVRO_LIBPATH compile schema $SCHEMA_FILE /HADOOP_TASKS/WeightCompute/

#sh tasks
COPY util/build-jar.sh /HADOOP_TASKS/build-jar.sh
COPY util/do-tasks.sh /HADOOP_TASKS/do-tasks.sh
RUN chmod a+x /HADOOP_TASKS/do-tasks.sh
RUN chmod a+x /HADOOP_TASKS/build-jar.sh

CMD ["/HADOOP_TASKS/do-tasks.sh"] 
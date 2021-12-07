FROM autocomplete/hbase

COPY do-tasks.sh /HADOOP_TASKS/do-tasks.sh
COPY adding-words-weight /HADOOP_TASKS/adding-words-weight

RUN chmod a+x /HADOOP_TASKS/do-tasks.sh

CMD ["/HADOOP_TASKS/do-tasks.sh"]
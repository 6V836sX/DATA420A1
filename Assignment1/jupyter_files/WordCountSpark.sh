# Submit WordCount.py script to spark to run programmatically 

NAME=abc123

submit_pyspark_script "WordCount.py hdfs:///data/helloworld hdfs:///user/$NAME/word-count-spark"
hdfs dfs -ls /user/$NAME/word-count-spark/
hdfs dfs -cat /user/$NAME/word-count-spark/*

# Clean up

hdfs dfs -rm -R /user/$NAME/word-count-spark/

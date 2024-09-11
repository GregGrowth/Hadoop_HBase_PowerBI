cp /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar .
./start-hadoop.sh
start-hbase.sh
./hbase_drop.sh
./hbase_create.sh
hbase-daemon.sh start thrift
hdfs dfs -mkdir -p input
hdfs dfs -rm input/word.txt
hdfs dfs -put word.txt input
hdfs dfs -rm -r outputjob01
hadoop jar hadoop-streaming-2.7.2.jar -file mapper.py -mapper "python3 mapper.py" -file reducer.py -reducer "python3 reducer.py" -input input/word.txt -output outputjob01

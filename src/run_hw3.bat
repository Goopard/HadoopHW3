ssh root@localhost -p 2222 command mkdir /usr/jobs/auto_test/
ssh root@localhost -p 2222 command mkdir /usr/jobs/auto_test/input/

scp -P 2222 city_stats_job.py root@localhost:/usr/jobs/auto_test/test.py
scp -P 2222 ..\input\imp.20131019.txt root@localhost:/usr/jobs/auto_test/input/test0.txt
scp -P 2222 ..\input\imp.20131020.txt root@localhost:/usr/jobs/auto_test/input/test1.txt
scp -P 2222 ..\input\imp.20131021.txt root@localhost:/usr/jobs/auto_test/input/test2.txt
scp -P 2222 ..\input\imp.20131022.txt root@localhost:/usr/jobs/auto_test/input/test3.txt
scp -P 2222 ..\input\imp.20131023.txt root@localhost:/usr/jobs/auto_test/input/test4.txt
scp -P 2222 ..\input\imp.20131024.txt root@localhost:/usr/jobs/auto_test/input/test5.txt
scp -P 2222 ..\input\imp.20131025.txt root@localhost:/usr/jobs/auto_test/input/test6.txt
scp -P 2222 ..\input\imp.20131026.txt root@localhost:/usr/jobs/auto_test/input/test7.txt
scp -P 2222 ..\input\imp.20131027.txt root@localhost:/usr/jobs/auto_test/input/test8.txt


scp -P 2222 ..\input\city.en.txt root@localhost:/usr/jobs/auto_test/city.en.txt

ssh root@localhost -p 2222 command hadoop fs -mkdir /user/raj_ops/input/
ssh root@localhost -p 2222 command hadoop fs -copyFromLocal /usr/jobs/auto_test/input/* /user/raj_ops/input/
ssh root@localhost -p 2222 command hadoop fs -copyFromLocal /usr/jobs/auto_test/city.en.txt /user/raj_ops/city.en.txt

ssh root@localhost -p 2222 command "python3 /usr/jobs/auto_test/test.py -r hadoop hdfs:///user/raj_ops/input/ --hadoop-streaming-jar=/usr/hdp/2.6.4.0-91/hadoop-mapreduce/hadoop-streaming.jar --cities=hdfs:///user/raj_ops/city.en.txt > /usr/jobs/auto_test/output.txt --partitioner --reduces=5"

scp -P 2222 root@localhost:/usr/jobs/auto_test/output.txt ..\output\output.txt

rem ssh root@localhost -p 2222 command rm -r /usr/jobs/auto_test
rem ssh root@localhost -p 2222 command hadoop fs -rm -r -skipTrash /user/raj_ops/*
ssh root@localhost -p 2222

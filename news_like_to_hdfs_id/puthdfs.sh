hdfs dfs -test -d wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/$1
if [ $? != 0 ];then
    hdfs dfs -mkdir wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/$1
fi

hdfs dfs -test -e wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/$1/$2
if [ $? == 0 ];then
    hdfs dfs -rm wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/$1/$2
fi
hdfs dfs -put  ./data/$1/$2 wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/$1/$2

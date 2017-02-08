#! /bin/bash
source ./join_script/config.py

function input_join(){
    INPUT=
    input_date=$1
    for i in $(seq 1 $INPUT_NUM);do
        tmp_delta_left="DELTA_LEFT_$i"
        tmp_delta_right="DELTA_RIGHT_$i"

        for j in $(seq  ${!tmp_delta_left} ${!tmp_delta_right});do
            if [ $j -lt 0 ]
            then
                input_date_current=(`date -d "$input_date ${j:1:1} hour ago" +"%Y%m%d %H"`)
            else
                input_date_current=(`date -d "$input_date $j hour" +"%Y%m%d %H"`)
            fi
            input_date_date=${input_date_current[0]}
            input_date_hour=${input_date_current[1]}
            tmp_input="INPUT_$i"
            input_current=${!tmp_input//DATE/$input_date_date}
            input_current=${input_current//HOUR/$input_date_hour}

            tmp_check="CHECK_DONE_$i"
            check_current=${!tmp_check//DATE/$input_date_date}
            check_current=${check_current//HOUR/$input_date_hour}
            while true
            do
                checkcmd="hdfs dfs -test -e $check_current"
                echo $checkcmd
                eval $checkcmd
                if [ $? -eq 0 ]
                then
                    echo $input_current' ready'
                    INPUT=$INPUT",${input_current}"
                    break
                else
                    echo $input_current' not ready, wait 300s'
                    sleep 300
                fi
            done
        done

    done
    INPUT=' -input '${INPUT:1}
}

function updatefiles(){
    UPDATE_FILES=' -files join_script'
}

function run(){
    cmd="yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
             -D mapred.job.name='click_show_zan_join_id' \
             -D mapred.reduce.tasks=50 \
             -D mapreduce.map.memory.mb=1024 \
             -D mapreduce.reduce.memory.mb=1024\
             -D mapred.map.tasks=20 \
             -D mapred.job.map.capacity=30 \
             -D mapred.job.reduce.capacity=15 \
             -D mapred.job.priority=NORMAL\
             -D stream.num.map.output.key.fields=4\
             -D mapred.text.key.partitioner.options=\"-k1,2\" \
             -D  mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
             -D mapred.text.key.comparator.options=\"-k1,4\"\
             -files join_script \
             -reducer \"python join_script/reducer.py \"\
             -mapper \"python join_script/mapper.py\" \
             -output $OUTPUT_CURRENT \
             -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner\
        "
    cmd=$cmd${INPUT}
    echo date +'%Y-%m-%d %H:%M' $cmd
    hdfs dfs -rm -r $OUTPUT_CURRENT
    eval $cmd
}

updatefiles
start_date="2017-02-06 13:00"

while true
do
    input_join "$start_date"
    start_date_array=($start_date)
    date_part=${start_date_array[0]}
    hour_part=${start_date_array[1]}
    OUTPUT_CURRENT=${OUTPUT//DATE/$date_part}
    OUTPUT_CURRENT=${OUTPUT_CURRENT//-/}
    OUTPUT_CURRENT=${OUTPUT_CURRENT//HOUR/${hour_part:0:2}}

    run
    if [ $? -ne 0 ]
    then
        echo "run error, date=["$input_date"]"
        break
    fi
    start_date=`date -d "$start_date 1 hour" +"%Y%m%d %H:%M"`
done


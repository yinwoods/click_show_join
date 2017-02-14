time=`da`
date=`date +%Y%m%d`
echo $date
hour_left1=`date -d -1hour +%H`
hour_left2=`date -d -2hour +%H`
hour_left3=`date -d -3hour +%H`
hour_left4=`date -d -4hour +%H`
hour_left8=`date -d -8hour +%H`
hour_left9=`date -d -9hour +%H`
hour_left10=`date -d -10hour +%H`
hour_left11=`date -d -11hour +%H`
hour_left12=`date -d -12hour +%H`

echo $date, $hour_left1, $hour_left12

hive\
    -hiveconf date=$date\
    -hiveconf hour_left1=$hour_left1\
    -hiveconf hour_left2=$hour_left2\
    -hiveconf hour_left3=$hour_left3\
    -hiveconf hour_left4=$hour_left4\
    -hiveconf hour_left8=$hour_left8\
    -hiveconf hour_left9=$hour_left9\
    -hiveconf hour_left10=$hour_left10\
    -hiveconf hour_left11=$hour_left11\
    -hiveconf hour_left12=$hour_left12\
    -f getData.sql

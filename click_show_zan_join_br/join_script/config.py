#python & shell config
# SCHEMA PATH

SCHEMA_PREFER="SCHEMA_"
FILENAME="INPUT"
DELTA_LEFT_PREFER="DELTA_LEFT"
DELTA_RIGHT_PREFER="DELTA_RIGHT"
LENGTH_PREFER="LENGTH"
JOIN_KEY_PREFER='KEY'

FILES="util.py,mapper.py,reducer.py,config.py,like.schema,click.schema,show.schema"

INPUT_NUM=3

#input 1
SCHEMA_INPUT_1="./join_script/show.schema"
INPUT_1="wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsImpressionSummary-DATE/HOUR"
CHECK_DONE_1="wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsImpressionSummary-DATE/HOUR.done"
DELTA_LEFT_1=0
DELTA_RIGHT_1=0
LEGTH_1=32
KEY_1="3,2,0,1"

#input1
SCHEMA_INPUT_2="./join_script/click.schema"
INPUT_2="wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-DATE/HOUR"
CHECK_DONE_2="wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-DATE/HOUR.done"
DELTA_LEFT_2=-1
DELTA_RIGHT_2=3
LEGTH_2=25
KEY_2="3,2,0,1"

#input 3
SCHEMA_INPUT_3="./join_script/like.schema"
INPUT_3="wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/DATE/DATEHOUR00"
CHECK_DONE_3="wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/DATE/DATEHOUR00"
DELTA_LEFT_3=-8
DELTA_RIGHT_3=-4
LEGTH_3=4
KEY_3="0,1,-,-"

SCHEMA_OUTPUT="./join_script/output.schema"
OUTPUT="wasb://niphdbr@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/DATE/HOUR00"

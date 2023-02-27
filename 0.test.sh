#!/Git/Git/bin/bash
# -gt					>
# -eq					=
# -ne					!=
# -ge					>=
# -lt				    <
# -le					<=

echo "123"

export ETL_HOME=/inf_file/edc_etl/
export ETL_SCRIPT_LOG_PATH=/inf_file/edc_etl/04_logs/
export EDC_TNS=EDC

if [ $# -eq 3 ];then
   echo " Params Exception校验!脚本参数:脚本路径|账期|本地网"
   echo " 示例: EDP/BILL_BILL_ACCUM_A/bin/bwt_prd_pd_inst_d.pl 20160802 11"
   exit 1
fi

echo "$ETL_HOME+$ETL_SCRIPT_LOG_PATH"
echo "${ETL_HOME}$ETL_SCRIPT_LOG_PATH"


x=foo
y=${x}bar
x=xyz

${x:=yuu}
y:=$(x) btt
${x:=qwe}
echo $x
echo $y


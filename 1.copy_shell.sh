#!/Git/Git/bin/bash
#########################################
#
#功能@:执行PERL脚本程序
#入参@脚本路径|账期|本地网 / EDP/BILL_BILL_ACCUM_A/bin/bwt_prd_pd_inst_d.pl 20160802 11
#Create@：liuym
#
#########################################

kinit -k -t /home/keydir/edc_jk/edc_jk.keytab edc_jk@HNHDP.COM

#--加载环境变量------------------------------------------------------------------------------
export ORACLE_BASE=/home/oracle;
export ORACLE_HOME=$ORACLE_BASE/product/11.2.0.4/client_1
export PATH=$ORACLE_HOME/bin:$PATH
export TNS_ADMIN=$ORACLE_HOME/network/admin
export NLS_LANG=American_America.ZHS16GBK
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:/lib:/usr/lib
export CLASSPATH=$ORACLE_HOME/jlib:$ORACLE_HOME/rdbms/jlib

export ETL_HOME=/inf_file/edc_etl/
export ETL_SCRIPT_LOG_PATH=/inf_file/edc_etl/04_logs/
export EDC_TNS=EDC
export EXPORT_PARA=8

#--参数校验------------------------------------------------------------------------------
if [ $# -lt 3 ];then
   echo " Params Exception校验!脚本参数:脚本路径|账期|本地网"
   echo " 示例: EDP/BILL_BILL_ACCUM_A/bin/bwt_prd_pd_inst_d.pl 20160802 11"
   exit 1
fi

#--接收参数------------------------------------------------------------------------------
typeset -l script_path=$1
cycle_id=$2
lan_id=$3
script_name=${script_path##*/}
script_name=${script_name%.*}

msg_name="hdp_${script_name:2}"

echo "Param:${script_path}|${cycle_id}|${lan_id}"

#切换目录
cd ${ETL_HOME}

#--自定义变量-----------------------------------------------------------------------------
log_dir=""
log_day_dir=${ETL_SCRIPT_LOG_PATH}/day/${cycle_id}/${script_path%/*}
log_mon_dir=${ETL_SCRIPT_LOG_PATH}/mon/${cycle_id}/${script_path%/*}
script_path=${ETL_HOME}/03_scripts/${script_path}

if   [ ${lan_id} -eq 11 ]; then lan_code='A'
elif [ ${lan_id} -eq 13 ]; then lan_code='B'
elif [ ${lan_id} -eq 12 ]; then lan_code='C'
elif [ ${lan_id} -eq 14 ]; then lan_code='D'
elif [ ${lan_id} -eq 19 ]; then lan_code='E'
elif [ ${lan_id} -eq 10 ]; then lan_code='F'
elif [ ${lan_id} -eq 16 ]; then lan_code='G'
elif [ ${lan_id} -eq 21 ]; then lan_code='H'
elif [ ${lan_id} -eq 17 ]; then lan_code='I'
elif [ ${lan_id} -eq 15 ]; then lan_code='J'
elif [ ${lan_id} -eq 23 ]; then lan_code='K'
elif [ ${lan_id} -eq 22 ]; then lan_code='L'
elif [ ${lan_id} -eq 18 ]; then lan_code='M'
elif [ ${lan_id} -eq 20 ]; then lan_code='N'
else lan_code='Z'
fi

cycle_lenth=`expr length "${cycle_id}"`
sys_date=`date "+%Y%m%d"`

#--维护日志目录-------------------------------------------------------------------------
if [ ! -d ${log_day_dir} ] && [ ${cycle_lenth} -eq 8 ]; then
  mkdir -p ${log_day_dir}
fi

if [ ! -d ${log_mon_dir} ] && [ ${cycle_lenth} -eq 6 ]; then
  mkdir -p ${log_mon_dir}
fi

if   [ ${cycle_lenth} -eq 8 ]; then
  log_dir=${log_day_dir}
elif [ ${cycle_lenth} -eq 6 ]; then
  log_dir=${log_mon_dir}
fi

#--维护日志，判断日志文件如果存在则生成归档日志后再清空当前日志---------------------------

log_file=${log_dir}/${script_name}_${lan_code}_${cycle_id}_${sys_date}.log
log_his_file=${log_dir}/${script_name}_${lan_code}_${cycle_id}_${sys_date}_his.log
##echo ${log_file}
##echo ${log_his_file}
if [ -f ${log_file} ]; then
  echo "log_file exists "
  cat ${log_file} >> ${log_his_file}
else
  echo "log_file not exists"
fi


#--调用业务逻辑脚本-----------------------------------------------------------------------
result1=-1
log_file=${log_dir}/${script_name}_${lan_code}_${cycle_id}_${sys_date}.log
echo "perl ${script_path} ${cycle_id} ${lan_code} > ${log_file}"
`perl ${script_path} ${cycle_id} ${lan_code} 1 > ${log_file}`
result1=$?

error_msg=`grep 'Err' ${log_file} | grep -v 'table or view does not exist'`
check_msg=`grep -i 'myRunCheckMessage' ${log_file} `


#判定脚本具体返回值
etl_status=${result1}
##etl_status=0
etl_message="SUCCESS"
if [ ${result1} -ne 0 ]; then
   if [ -n "${error_msg}" ]; then
     etl_status=-1;
     etl_message=${error_msg};
   elif [ -n "${check_msg}" ]; then
     etl_status=1;
     etl_message=${check_msg};
   else
     etl_status=-1;
     etl_message="Unknown error message!please check the log file!!!";
   fi
fi


#--获取写入hive表数据量-----------------------------------------------------------------------------
echo "--================= count_table_rec =================--"
echo "log_file:"${log_file}
##table_name=`cat ${log_file}|grep -v "INFO"|tr '[A-Z]' '[a-z]'|grep "partition"|grep "insert"|awk -F' ' '{print $4}'|awk 'NR==1{print}'`
table_name=`cat ${log_file}|tr '[A-Z]' '[a-z]'|grep "partition"|grep "insert into"|grep -v "table"|awk -F' ' '{print $3}'|awk 'NR==1{print}'`
echo "table_name1:"${table_name}
if [ -z "${table_name}" ]; then
  table_name=`cat ${log_file}|tr '[A-Z]' '[a-z]'|grep "partition"|grep "insert"|grep "table"|awk -F' ' '{print $4}'|awk 'NR==1{print}'`
  echo "table_name2:"${table_name}
  if [ -z "${table_name}" ]; then
    table_name=`cat ${log_file}|tr '[A-Z]' '[a-z]'|grep "partition"|grep "insert"|awk -F' ' '{print $4}'|awk 'NR==1{print}'`
    echo "table_name3:"${table_name}
    if [ -z "${table_name}" ]; then
      table_name=`cat ${log_file}|tr '[A-Z]' '[a-z]'|grep "table"|grep "insert"|grep "overwrite"|awk -F' ' '{print $4}'|awk 'NR==1{print}'`
      echo "table_name4:"${table_name}
    fi
  fi
fi

if [ -n "${table_name}" ]; then
  table_rec_str=`cat ${log_file}|grep "${table_name}\:"|awk -F' ' '{print \$4}'`
  echo "table_rec_str:"${table_rec_str}
fi
table_rec=0;
if [ -n "${table_rec_str}" ]; then
   for variable in ${table_rec_str}
   do
     echo "variable:"${variable}
     if [ ${table_rec} -eq ${variable} ]; then
       table_rec=${variable}
     else
       table_rec=`expr ${table_rec} + ${variable}`
     fi
   done
fi

echo "--================= write_etl_message =================--"
echo "log_file:"${log_file}
echo "script_name:"${script_name}
echo "cycle_id:"${cycle_id}
echo "lan_code:"${lan_code}
echo "etl_message:"${etl_message}
echo "script_path:"${script_path}
echo "etl_status:"${etl_status}
echo "table_rec:"${table_rec}

##获取主机IP
host_ip=$(ifconfig|grep 'inet '|grep '134'|awk '{ print $2}'|tr -d 'addr:'|awk 'NR==1{print}')
echo "host_ip:${host_ip}"
etl_message="${host_ip} ""/inf_file/edc_etl/01_comm/exec_perl_hql_script.sh ""${etl_message}"
echo "etl_message:"${etl_message}


#--调用写调度日志脚本-----------------------------------------------------------------------------
result2=-1
echo "perl ${ETL_HOME}/01_comm/write_etl_message.pl ${msg_name} ${cycle_id} ${lan_code} ${etl_message} ${script_path} ${etl_status}" ${table_rec}
`perl ${ETL_HOME}/01_comm/write_etl_message.pl ${msg_name} ${cycle_id} ${lan_code} "${etl_message}" ${script_path} ${etl_status} ${table_rec} >>${log_dir}/${script_name}_${lan_code}_${cycle_id}_${sys_date}.log`
result2=$?

echo "--================= write_sk_message =================--"
sh ${ETL_HOME}/01_comm/msg_to_yingxie_log.sh "${msg_name}" "${cycle_id}" "${lan_code}" "${etl_message}" "${script_path}" "${etl_status}"

error_msg=`grep 'ORA-' ${log_file} | grep -v 'table or view does not exist'`

#写调度日志脚本报错
echo "--================= check_result2 =================--"
echo "result2:"${result2}
if [ ${result2} -ne 0 ]; then
   echo "retCode:-1"
   echo "retMes:logFailure "${error_msg}"!!Please check the log:"${log_file}
   exit ${result2}
fi

#业务逻辑脚本报错
echo "--================= check_result1 =================--"
echo "result1:"${result1}
if [ ${result1} -ne 0 ]; then
   echo "retCode:"${etl_status}
   echo "retMes:dataFailure "${etl_message}"!!Please check the log:"${log_file}
   exit ${etl_status}
fi

echo "retCode:0"
echo "retMes:successtrue"
exit ${etl_status}

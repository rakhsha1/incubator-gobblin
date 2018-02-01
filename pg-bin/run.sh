#export JAVA_HOME=/usr/lib/jvm/java-7-oracle
BASE_DIR=.
WORK_DIR=${BASE_DIR}/work_dir
CONF_DIR=${BASE_DIR}/job_conf
JOB_CONFIG=${CONF_DIR}/backup.job
SYS_CONFIG=${BASE_DIR}/conf/gobblin-standalone.properties
OUTPUT_DIR=${WORK_DIR}
export GOBBLIN_OUTPUT_DIR=${OUTPUT_DIR}

${BASE_DIR}/bin/run-gobblin.sh start --workdir ${WORK_DIR} --conf ${CONF_DIR} --jobconfig $JOB_CONFIG --sysconfig $SYS_CONFIG

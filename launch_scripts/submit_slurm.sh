#!/bin/bash
usage()
{
    cat << EOF
$(basename "$0"):
    Submit a LUTE managed Task using SLURM on S3DF.
    Options:
        -c|--config
          Path to the LUTE configuration YAML.
        -h|--help
          Display this message.
        -t|--taskname
          Name of the LUTE managed Task to run.
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
    flag="$1"

    case $flag in
    -c|--config)
      CONFIGPATH="$2"
      shift
      shift
      ;;
    -h|--help)
        usage
        exit
        ;;
    -t|--taskname)
        TASK="$2"
        shift
        shift
        ;;
    --debug)
        DEBUG=1
        shift
        shift
        ;;
    *)
        POS+=("$1")
        shift
        ;;
    esac
done
set -- "${POS[@]}"

# Assume all other arguments are for SLURM
SLURM_ARGS=$@

if [[ -z ${CONFIGPATH} || -z ${TASK} ]]; then
    echo "Path to LUTE config amd Task name are required!"
    usage
    exit
fi

# By default source the psana environment since most Tasks will use it.
if [[ $HOSTNAME =~ "sdf" ]]; then
    source /sdf/group/lcls/ds/ana/sw/conda1/manage/bin/psconda.sh
fi

export LUTE_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd | sed s/launch_scripts//g )"
EXECUTABLE="${LUTE_PATH}run_task.py"

if [[ ${DEBUG} ]]; then
    echo "Running in debug mode - verbose logging."
    CMD="python -B ${EXECUTABLE} -c ${CONFIGPATH} -t ${TASK}"
else
    echo "Running in standard mode."
    CMD="python -OB ${EXECUTABLE} -c ${CONFIGPATH} -t ${TASK}"
fi

sbatch $SLURM_ARGS --wrap "${CMD}"

#!/bin/bash

set -ue

echo "STAGE_BULK_WORK_DIR: $STAGE_BULK_WORK_DIR"
echo "DT: $DT"
echo "STAGE_JSON: $STAGE_JSON"
echo "DATA_DIR: $DATA_DIR"

WORKDIR=$PWD
mkdir $DATA_DIR || true

function download_cyclepoint {
    local nc=$1
    local cyclep=$2
    local PROJECT=$3
    local DATA_DIR=$4
    set -eu
    stdout=""
    stdout="$stdout\n[$nc] Cycle point $cyclep"
    mdss_stage_sub_arr=($(cat barra_files.out | grep $cyclep))
    stdout="$stdout\n[$nc] Found ${#mdss_stage_sub_arr[@]} files in barra_files.out that match $cyclep"
    # Stage anyway (should return instantly)
#stdout="$stdout\n[$nc] STAGING (should be instant): mdss -P $PROJECT stage -wr ${mdss_stage_sub_arr[@]}"
    stdout="$stdout\n[$nc] STAGING (should be instant): mdss -P $PROJECT stage -wr '${#mdss_stage_sub_arr[@]} files'"
    mdss -P $PROJECT stage -wr ${mdss_stage_sub_arr[@]}
    # download all files
    mkdir -p ${DATA_DIR}/${cyclep} || true
    cd ${DATA_DIR}/${cyclep}
#stdout="$stdout\n[$nc] DOWNLOADING: mdss -P $PROJECT get ${mdss_stage_sub_arr[@]} $PWD"
    stdout="$stdout\n[$nc] DOWNLOADING: mdss -P $PROJECT get '${#mdss_stage_sub_arr[@]} files' $PWD"
    mdss -P $PROJECT get ${mdss_stage_sub_arr[@]} . || { echo "Get files failed" ; exit 1 ;}
    wait
#sleep 15
    # change names of pi files
    stdout="$stdout\n[$nc] Cleaning up PI files"
    pi_files=($(ls umeasa_pi*))
    for pifl in ${pi_files[@]}; do
        pibn=$(basename $pifl)
        pisuff=${pibn##*_}
        mv $pifl ${DATA_DIR}/${cyclep}/${cyclep}_ra_pi_${pisuff}
    done
    stdout="$stdout\n[$nc] Success retrieving $cyclep files from tape"
    done_fl="${DATA_DIR}/${cyclep}/done.$cyclep"
    stdout="$stdout\n[$nc] Touching done file $done_fl"
    printf "$stdout"
    touch $done_fl
}

function make_group {
    # with the monthly groups, only the start datetime of each cycle is used, so need to get the whole month
    END_CYCLE_MONTH="$(echo $1 | rev | cut -f1 -d ' ' | rev)"
    NUM_DAYS=$(cal $(date -d `echo $END_CYCLE_MONTH | cut -c 1-8` +"%m %Y") | awk 'NF {DAYS = $NF}; END {print DAYS}')
    local local_group="$(echo $1 | cut -f1 -d ' ')-$(echo $END_CYCLE_MONTH | cut -c 1-6)${NUM_DAYS}T1800Z"
    echo $local_group
}

# get list of mdss files to download for this cycle
module use ~access/modules
module load python
module load pythonlib/pandas
GROUP="$(make_group $DT $DT)" # returns a single month range sep by space i.e 19900101T0000Z 19900131T1800Z
GROUP_TSTART=$(echo $GROUP | cut -f1 -d '-' | cut -c 1-11 | sed -e 's/T//')
GROUP_TEND=$(echo $GROUP | cut -f2 -d '-'| cut -c 1-11 | sed -e 's/T//')
find_files_cmd="""--model BARRA-R --tstart $GROUP_TSTART --tend $GROUP_TEND --xx_restart --stash_stream pi --stash_stream dst-output"
python ${MOSRS_REPO_DIR}/suite_utils/tape_mgt/find_barra_on_tape.py \
	       --model BARRA-R \
	       --xx_restart \
	       --stash_stream pi \
	       --stash_stream dst-output \
	       --var_file ${ROSE_ETC}/dst.barra-r.varlist \
	       --tstart $GROUP_TSTART \
	       --tend $GROUP_TEND
mdss_stage_arr=($(cat barra_files.out))
echo "Found ${#mdss_stage_arr[@]} files to retrieve between $GROUP_TSTART and $GROUP_TEND:"
#echo ${mdss_stage_arr[@]}

# DO A LOOP THROUGH ALL THE DATETIMES
echo "Now splitting up and retrieving one cycle point set of files at a time"
cparr=($(cat barra_files.out | grep pressure-an | rev | cut -f1 -d '-' | rev | cut -f1 -d '.' | uniq))
proc_max=8 # subtract 2 from this for actual parallel process number. 8 seems to be the sweet spot
nc=1
seconds_wait=10
ps_string="download_parallel.sh"
echo "Found ${#cparr[@]} unique cycle points. Looping through running ..."
for cyclep in ${cparr[@]}; do
    echo "Cyclepoint $cyclep"
    while true; do
        proc_count=$(ps -f -u $USER | grep $ps_string | grep "/bin/bash" 2>/dev/null | wc -l)
        if [[ $proc_count -lt $proc_max ]]; then
            time download_cyclepoint $nc $cyclep $PROJECT $DATA_DIR &
            nc=$((nc+1))
            break
        else
            echo "PROCESS COUNT of '$ps_string' = $proc_count > proc_max = $proc_max"
            echo "Sleeping for $seconds_wait"
            sleep $seconds_wait
        fi
    done
done
echo "Launched all processes"
echo "Still $(ps -f -u $USER | grep $ps_string | grep "/bin/bash" 2>/dev/null | wc -l) running"
ps -f -A -u $USER | grep $ps_string | grep "/bin/bash" 
wait
wait
echo "Done!"
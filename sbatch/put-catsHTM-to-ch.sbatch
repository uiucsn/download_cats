#!/bin/bash
###############################################################################
##                                                                           ##
##                           Campus Cluster                                  ##
##                   Sample SERIAL Job Batch Script                          ##
##                                                                           ##
## SLURM Options (To view, run the following command below)                  ##
##                                                                           ##
##     man sbatch                                                            ##
##                                                                           ##
###############################################################################
#                                                                            
#SBATCH --time=99:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
#SBATCH --cpus-per-task=20               # Number of cpu cores per task
##SBATCH --mem=192G                       # RAM per node
#SBATCH --job-name=put-catsHTM-to-ch     # Name of batch job
#SBATCH --partition=ncsa                 # Partition (queue)           
#SBATCH --mail-user=kostya@illinois.edu  # Send email notifications
#SBATCH --mail-type=BEGIN,END            # Type of email notifications to send
#                                                                            
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

PROJECT_ROOT="$HOME/download_cats/"
DATA_ROOT="/projects/caps/uiucsn/catsHTM"


###############
top -b -d 60 -u $USER &
TOP_PID=$!


#################################
echo "Starting clickhouse server"
CLICKHOUSE_ROOT='/projects/caps/uiucsn/ztf_dr/clickhouse'
export PATH="$CLICKHOUSE_ROOT/bin:$PATH"

$CLICKHOUSE_ROOT/bin/clickhouse-server -C $CLICKHOUSE_ROOT/etc/config.xml &
CLICKHOUSE_PID=$!
sleep 300


####################
module load python/3
export PYTHONPATH="$HOME/.local/lib/python3.7/site-packages:$PYTHONPATH"

cd "$PROJECT_ROOT"
pip3 install --user -r requirements.txt
python3 -m put_cat_to_ch -vvv -d "$DATA_ROOT" --tmp-dir="$DATA_ROOT/tmp" -e drop htm -a create insert
# python3 -m put_cat_to_ch -vvv -j20 -d "$DATA_ROOT" --dr=4 -a gen-csv


#################################
echo "Stopping clickhouse server"
kill $CLICKHOUSE_PID
timeout 300 tail --pid=$CLICKHOUSE_PID -f /dev/null

echo "Stopping top"
kill $TOP_PID

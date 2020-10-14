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
#SBATCH --time=18:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
##SBATCH --cpus-per-task=1                # Number of cpu cires per task
#SBATCH --job-name=put-ztf-to-ch         # Name of batch job
#SBATCH --partition=ncsa                 # Partition (queue)           
#SBATCH --mail-user=kostya@illinois.edu  # Send email notifications
#SBATCH --mail-type=BEGIN,END            # Type of email notifications to send
#                                                                            
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR


CLICKHOUSE_ROOT='/projects/caps/uiucsn/ztf_dr/clickhouse'
export PATH="$CLICKHOUSE_ROOT/bin:$PATH"

echo "Starting clickhouse server"
$CLICKHOUSE_ROOT/bin/clickhouse-server -C $CLICKHOUSE_ROOT/etc/config.xml &
CLICKHOUSE_PID=$!
sleep 300


module load python/3
export PYTHONPATH="$HOME/.local/lib/python3.7/site-packages:$PYTHONPATH"

PROJECT_ROOT="$HOME/download_cats/"
DATA_ROOT="/projects/caps/uiucsn/ztf_dr/lc_dr3"

cd "$PROJECT_ROOT"
pip3 install --user -r requirements.txt
python3 -m put_cat_to_ch -vvv -d "$DATA_ROOT" -j4 -e fail


echo "Stopping clickhouse server"
kill $CLICKHOUSE_PID


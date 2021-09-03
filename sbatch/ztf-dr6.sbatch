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
#SBATCH --cpus-per-task=20               # Number of cpu cires per task
#SBATCH --job-name=download_catsHTM      # Name of batch job
#SBATCH --partition=caps                 # Partition (queue)           
#SBATCH --mail-user=kostya@illinois.edu  # Send email notifications
#SBATCH --mail-type=BEGIN,END            # Type of email notifications to send
#                                                                            
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

module load python/3
export PYTHONPATH="$HOME/.local/lib/python3.7/site-packages:$PYTHONPATH"

CLICKHOUSE_ROOT='/projects/caps/uiucsn/ztf_dr/clickhouse'
export PATH="$CLICKHOUSE_ROOT/bin:$PATH"

PROJECT_DIR="$HOME/download_cats/"
DR="6"
DEST="/projects/caps/uiucsn/ztf_dr/lc_dr$DR"

HOST="ccc0240"

cd $PROJECT_DIR
# python3 -m download_cats -vvv -d "$DEST" -j12 ztf --dr $DR
python3 -m put_cat_to_ch -vvv -d "$DEST" --host $HOST ztf --dr "$DR" -j20 -a obs

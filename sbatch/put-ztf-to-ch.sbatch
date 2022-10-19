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
#SBATCH --job-name=put-ztf-to-ch         # Name of batch job
#SBATCH --partition=caps                 # Partition (queue)           
#SBATCH --mail-user=kostya@illinois.edu  # Send email notifications
#SBATCH --mail-type=BEGIN,END            # Type of email notifications to send
#                                                                            
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

DR="13"
DATA_ROOT="/projects/caps/uiucsn/ztf_dr/lc_dr$DR"
HOST="ccc0288"


CLICKHOUSE_ROOT='/projects/caps/uiucsn/ztf_dr/clickhouse'
export PATH="$CLICKHOUSE_ROOT/bin:$PATH"


###############
top -b -d 60 -u $USER &
TOP_PID=$1

####################
module load anaconda/2022-May/3
conda activate /projects/caps/uiucsn/github.com/uiucsn/download_cats/conda-env
export PYTHONPATH=/projects/caps/uiucsn/github.com/uiucsn/download_cats/conda-env/lib/python3.10/site-packages

python3.10 -m put_cat_to_ch -vvv -d "$DATA_ROOT" --host="$HOST" -e drop ztf --dr=$DR -a obs


#################################
echo "Stopping top"
kill $TOP_PID

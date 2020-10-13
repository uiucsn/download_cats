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
#SBATCH --time=10:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
#SBATCH --cpus-per-task=1                # Number of cpu cires per task
#SBATCH --job-name=download_ztf_dr3      # Name of batch job
#SBATCH --partition=ncsa                 # Partition (queue)           
#SBATCH --mail-user=kostya@illinois.edu  # Send email notifications
#SBATCH --mail-type=BEGIN,END            # Type of email notifications to send
#                                                                            
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

module load python/3
export PYTHONPATH="$HOME/.local/lib/python3.7/site-packages:$PYTHONPATH"

PROJECT_ROOT="$HOME/download_cats/"
DEST="/projects/caps/uiucsn/ztf_dr/lc_dr3"

cd "$PROJECT_ROOT"
python3 -m download_cats -vvv -d "$DEST" -j16 ztf

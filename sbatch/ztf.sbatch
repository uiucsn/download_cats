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
#SBATCH --time=24:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
#SBATCH --cpus-per-task=4                # Number of cpu cires per task
#SBATCH --job-name=download_catsHTM      # Name of batch job
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

PROJECT_DIR="$HOME/download_cats/"
DEST="/projects/caps/uiucsn/ztf_dr/lc_dr4"

cd $PROJECT_DIR
python3 -m download_cats -vvv -d "$DEST" -j8 ztf --dr 4

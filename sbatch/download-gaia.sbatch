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

module load anaconda/2022-May/3
conda activate /projects/caps/uiucsn/github.com/uiucsn/download_cats/conda-env
export PYTHONPATH=/projects/caps/uiucsn/github.com/uiucsn/download_cats/conda-env/lib/python3.10/site-packages

DEST="/projects/caps/uiucsn/gdr3-gaia_source/"

python3.10 -m download_cats -vvv -d "$DEST" -j12 gaia --dr=dr3

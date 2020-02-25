#! /bin/bash

NEVENTS=$1
SEED=$2
RUNCARD=$3

echo "Running SHERPA in WD $PWD ..."
export ALIBUILD_WORK_DIR=/global/cfs/projectdirs/alice/mfasel/alice/sw
export PATH=/usr/bin:$PATH
eval `/usr/bin/alienv --no-refresh printenv SHERPA/latest`
Sherpa -e $NEVENTS -R $SEED -f $RUNCARD

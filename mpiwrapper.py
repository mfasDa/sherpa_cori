#! /usr/bin/env python
from mpi4py import MPI
import logging
import os
import sys
import subprocess

def run_slot(slot, globalslot, nevents, runscript, runcard):
    os.chdir("%04d" %slot)
    logging.info("starting sherpa simulation for slot %d (global %d)", slot, globalslot)
    logging.info("Running job for slot %d in workdir %s", slot, os.getcwd())
    content = os.listdir(os.getcwd())
    if not len(content):
        logging.info("Working directory %s empty ...", os.getcwd())
    else:
        contentstring = ""
        for f in content:
            if len(contentstring):
                contentstring += ", "
            contentstring += f
        logging.info("Content of working directory %s: %s", os.getcwd(), contentstring)
    cmd = "shifter ./%s %d %d %s &> %s/run_sherpa.log" %(runscript, nevents, globalslot, runcard, os.getcwd())
    logging.info("running simulation command \"%s\"" %cmd)
    subprocess.call(cmd, shell = True) 
    content = os.listdir(os.getcwd())
    if not len(content):
        logging.info("Working directory %s empty after job execution ...", os.getcwd())
    else:
        contentstring = ""
        for f in content:
            if len(contentstring):
                contentstring += ", "
            contentstring += f
        logging.info("Content of working directory after job execution %s: %s", os.getcwd(), contentstring)

if __name__ == "__main__":
    nslots = int(sys.argv[1])
    minslot = int(sys.argv[2])
    nevents = int(sys.argv[3])
    runcard = sys.argv[4]
    runscript = sys.argv[5]
    if MPI.COMM_WORLD.Get_rank() >= nslots:
        sys.exit(0)     # More CPUs than jobs due to whole node allocation
    logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)
    logging.info("Starting worker %d ...", MPI.COMM_WORLD.Get_rank())
    slotID = MPI.COMM_WORLD.Get_rank()
    globalslotID = slotID + minslot
    run_slot(slotID, globalslotID, nevents, runscript, runcard)
    logging.info("Worker %d done ...", MPI.COMM_WORLD.Get_rank())

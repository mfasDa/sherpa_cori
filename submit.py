#! /usr/bin/env python3
import argparse
import os
import subprocess
import sys

sourcedir = os.path.abspath(os.path.dirname(sys.argv[0]))

def create_jobscript(workdir, outputdir, runcard, nslotsall, nslotsmaster, minslot, nevents, qos, platform, timelimit,  usebb):
    scratchsize = {"hasswell": 100, "knl" : 300}
    jobscriptname = os.path.join(workdir, "jobscript.sh")
    with open(jobscriptname, "w") as scriptwriter:
        scriptwriter.write("#! /bin/bash\n")
        scriptwriter.write("#SBATCH --qos=%s\n" %qos)
        scriptwriter.write("#SBATCH --time=%s\n" %timelimit)
        scriptwriter.write("#SBATCH --nodes=1\n")
        scriptwriter.write("#SBATCH --constraint=%s\n" %platform)
        scriptwriter.write("#SBATCH --tasks-per-node=%s\n" %nslotsall)
        scriptwriter.write("#SBATCH -o %s\n" %(os.path.join(workdir, "joboutput.log")))
        scriptwriter.write("#SBATCH --image=docker:mfasel/cc7-alice:latest\n")
        scriptwriter.write("#SBATCH --license=cscratch1,cfs\n") 
        if usebb:
            scriptwriter.write("#DW jobdw capacity=%dGB access_mode=striped type=scratch\n" %scratchsize[platform])
            for slot in range(0, nslotsmaster):
                outputslot = minslot+slot
                workoutputfile = "$DW_JOB_STRIPED/%04d.zip" %outputslot
                outputfile = os.path.join(outputdir, "%04d.zip" %outputslot)
                scriptwriter.write("#DW stage_out source=%s destination=%s type=file\n" %(workoutputfile, outputfile))
        scriptwriter.write("module load cray-python/2.7.15.7\n")  # python with mpi, needed for srun
        if usebb:
            scriptwriter.write("$WD=$DW_JOB_STRIPED\n")
        else:
            scriptwriter.write("WD=$SCRATCH/spool/slurm_$SLURM_JOBID\n")
            scriptwriter.write("if [ ! -d $WD ]; then mkdir -p $WD; fi\n")
        scriptwriter.write("cd $WD\n")
        scriptwriter.write("echo \"Using WOKING directory $PWD ... \"\n")
        scriptwriter.write("cp %s %s\n" %(os.path.join(sourcedir, "mpiwrapper.py"), "mpiwrapper.py"))
        for slot in range(0, nslotsmaster):
            scriptwriter.write("mkdir %04d\n" %slot)
            scriptwriter.write("cp %s %04d/%s\n" %(os.path.join(sourcedir, "run.sh"), slot, "run.sh")) 
            scriptwriter.write("cp %s %04d/%s\n" %(runcard, slot, os.path.basename(runcard)))
        scriptwriter.write("srun -n %d python mpiwrapper.py %d %d %d %s run.sh\n" %(nslotsall, nslotsmaster, minslot, nevents, os.path.basename(runcard)))
        scriptwriter.write("cd $WD\n")
        scriptwriter.write("ls -l\n")
        for slot in range(0, nslotsmaster):
            outputslot = minslot+slot
            if outputslot != slot:
                scriptwriter.write("mv %04d %04d\n" %(slot, outputslot))
            scriptwriter.write("zip -r %04d.zip %04d/\n" %(outputslot, outputslot))
            if not usebb:
                scriptwriter.write("cp %04d.zip %s/%04d.zip\n" %(outputslot, outputdir, outputslot))
        scriptwriter.write("ls -l\n")
        if not usebb:
            scriptwriter.write("cd $SCRATCH\n")
            scriptwriter.write("rm -rf $WD\n")
        scriptwriter.write("echo \"Done ... \"\n")
        scriptwriter.close()
    return jobscriptname


def main(outputdir, runcard, njobs, nevents, qos, platform, timelimit, usebb):
    nslots = {"haswell": 32, "knl" : 68}
    nmaster = int(int(njobs)/int(nslots[platform]))
    jobslast = int(njobs) % int(nslots[platform])
    minslot = 0
    if jobslast > 0:
        nmaster += 1
    for imaster in range(0, nmaster):
        jobworkdir = os.path.join(outputdir, "master%d" %imaster)
        if not os.path.exists(jobworkdir):
            os.makedirs(jobworkdir, 0o755)
        nslotsmaster = njobs - minslot
        if nslotsmaster > nslots[platform]:
            nslotsmaster = nslots[platform]
        jobscript = create_jobscript(jobworkdir, outputdir, runcard, nslots[platform], nslotsmaster, minslot, nevents, qos, platform, timelimit, usebb)
        subprocess.call(["sbatch", jobscript])
        minslot += nslotsmaster

if __name__ == "__main__":
    platforms = ["haswell", "knl"]
    parser = argparse.ArgumentParser("submit.py", description = "Submitter for SHERPA jobs\n")
    parser.add_argument("-o", "--outputdir", type = str, required = True, help = "Output directory")
    parser.add_argument("-r", "--runcard", type = str, required = True, help = "Runcard")
    parser.add_argument("-n", "--numberofevents", type = int, default = 1000, help = "Number of events per job (default: 1000)")
    parser.add_argument("-j", "--jobs", type = int, default = 100, help = "Total number of jobs (default: 100)")
    parser.add_argument("-p", "--platform", metavar="PLATFORM", type = str, default = "haswell", help = "Cori platform (knl or haswell, default: haswell)")
    parser.add_argument("-q", "--qos", type = str, default = "regular", help = "Queue (default: regular)")
    parser.add_argument("-b", "--burstbuffer", action = "store_true", help = "Use burst buffer")
    parser.add_argument("-t", "--timelimit", type = str, default = "5:00:00", help = "Time limit")
    args = parser.parse_args()
    if not args.platform in platforms:
        print("Platform %s unsupported, select either \"haswell\" or \"knl\"" %platform)
        sys.exit(1)
    main(args.outputdir, args.runcard, args.jobs, args.numberofevents, args.qos, args.platform, args.timelimit, args.burstbuffer)

from batchq.core import batch
from batchq.queues.nohup import NoHUP, NoHUPSSH
from batchq.pipelines.shell.bash import BashTerminal
from batchq.pipelines.shell.ssh import SSHTerminal
from batchq.pipelines.shell.sftp import SFTPTerminal
from batchq.pipelines.shell.utils import FileCommander
from batchq.shortcuts.shell import home_create_dir, send_command


class PBSQSub(NoHUPSSH):
    __descriptive_name__ = "Remote PBS"

    _1 = batch.WildCard()
    _rev = batch.WildCard(reverse=True)
    _last = batch.WildCard( select = 0, reverse = True)

    ## TESTED AND WORKING
    pbs_status = batch.Function(NoHUP.pid,verbose=True,cache=5).Qcontroller("terminal") \
        .Qjoin("qstat ",_last," | awk '{ if($1 ~ /^",_last,"./) {printf $5}}'").send_command(_1)

    log = batch.Function(NoHUP.create_workdir,verbose=True, enduser = True) \
        .Qcall(NoHUP.identifier_filename).Qjoin(_1,".log").Qstore("log_file") \
        .isfile(_1).Qdon(2).Qstr("").Qreturn() \
        .Qget("log_file") \
        .Qjoin("cat ",_1).Qcontroller("terminal") \
        .send_command(_1)

    failed = batch.Function(NoHUP.lazy_finished,verbose=True, enduser=True, cache=5, type=bool).Qcontroller("terminal") \
        .Qdon(2).Qbool(False).Qreturn() \
        .Qcall(log).Qcontains("Successfully completed.",_1) \
        .Qdon(3).Qbool(True).Qreturn() \
        .Qcall(pbs_status).Qequal(_1,"F")

    
    ## TESTED AND WORKING
    finished = batch.Function(NoHUP.lazy_finished,verbose=True, enduser=True, cache=5, type=bool).Qcontroller("terminal") \
        .Qdo(2).Qbool(True).Qreturn() \
        .Qcall(log).Qcontains("Successfully completed.",_1) \
        .Qdo(2).Qbool(True).Qreturn() \
        .Qcall(pbs_status).Qequal(_1,"C")

    ## TESTED AND WORKING
    running = batch.Function(NoHUP.lazy_finished,verbose=True, enduser=True, cache=5, type=bool) \
        .Qdo(2).Qbool(False).Qreturn() \
        .Qcall(pbs_status).Qequal(_1,"R")

    ## TESTED AND WORKING
    pending = batch.Function(NoHUP.lazy_finished,verbose=True, enduser=True, cache=5, type=bool).Qcontroller("terminal") \
        .Qdo(2).Qbool(False).Qreturn() \
        .Qcall(NoHUP.lazy_running).Qdo(2).Qbool(False).Qreturn() \
        .Qcall(pbs_status).Qequal(_1,"Q")

    prepare_submission = batch.Function(verbose=True) \
         .Qstr(NoHUP.threads).Qjoin("export  OMP_NUM_THREADS=",_1).send_command(_1) \
         .Qset("command_prepend","").Qbool(NoHUP.mpi).Qdo(3).Qstr(NoHUP.processes).Qjoin("mpirun -np ", _1 , " ").Qstore("command_prepend") \
         .Qset("qsub_params","") # empty for now

#        .Qjoin("(touch ",_last, ".submitted ; bsub -oo ", _last, ".log ", _rev," \"touch ",_last,".running ; ", _rev , NoHUP.command, " 1> ",_rev,".running 2> ",_last,".error ; echo \\$? > ",_last,".finished \" |  awk '{ if(match($0,/([0-9]+)/)) { printf substr($0, RSTART,RLENGTH) } }' > ",_last,".pid )") \
    startjob = batch.Function(NoHUP.create_workdir,verbose=True) \
        .Qcall(prepare_submission) \
        .send_command(NoHUP.prior) \
        .Qget("qsub_params") \
        .Qget("command_prepend") \
        .Qcall(NoHUP.identifier_filename, 1) \
        .Qjoin("(touch ",_last, ".submitted ; qsub -o ", _last, ".running -e ", _last, ".error ",
               NoHUP.command, " |  awk '{ if(match($0,/([0-9]+)/)) { printf substr($0, RSTART,RLENGTH) } }' > ", _last,".pid )") \
        .send_command(_1) \
        .Qclear_cache()

    ## TODO: write this function
    cancel = batch.Function().Qstr("TODO: This function needs to be implemented")

from batchq.core.library import Library
from batchq.queues.nohup import NoHUP,NoHUPSSH
from batchq.queues.lsf import LSFBSub
from batchq.queues.pbs import PBSQSub
from batchq.queues.functions import create_configuration,list,help, server, client

# LocalShell = NoHUP

Library.queues.register("nohup",NoHUP)
Library.queues.register("ssh-nohup",NoHUPSSH)
Library.queues.register("lsf",LSFBSub)
Library.queues.register("pbs",PBSQSub)
Library.functions.register("configuration",create_configuration)
Library.functions.register("list",list)
Library.functions.register("help",help)

Library.functions.register("server",server)
Library.functions.register("client",client)


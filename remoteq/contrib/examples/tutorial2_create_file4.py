from tutorial2_create_file3 import CreateFileShort
from remoteq.core import batch
from remoteq.pipelines.shell.ssh import SSHTerminal
import getpass

class CreateFileSSH(CreateFileShort):
    server = batch.Property()
    username = batch.Property()
    password = batch.Property()
    terminal = batch.Controller(SSHTerminal, server, username, password)

user = raw_input("Username:")
pasw = getpass.getpass()
instance = CreateFileSSH("Documents/DEMO_SSH", "echo Hello SSH > hello.txt", "localhost",user,pasw)
instance.create_file()

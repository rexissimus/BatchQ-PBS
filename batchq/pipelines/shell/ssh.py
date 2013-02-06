####################################################################################
# Copyright (C) 2011-2012
# Troels F. Roennow, ETH Zurich
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
####################################################################################

from batchq.core.communication import BasePipe
from batchq.pipelines.shell.bash import BashTerminal
from batchq.core.process import Process
from batchq.core.utils import which 
import posixpath
import re
import time 

class BaseSecureTerminalLoginError(StandardError):
    """
    This exception is thrown by secure terminals whenever login attempts
    has failed or when acceptence of fingerprints is required but not
    allowed. 
    """
    pass

class BaseSecureTerminal(BasePipe):

    def __init__(self, server, username, password, port = 22, accept_figerprint = False, command = "ssh", port_option = "-p %d", expect_token = "#-->", submit_token="\n", login_expect = None):

        self.connect(server, username, password, port, accept_figerprint, command, port_option, expect_token, submit_token, login_expect)

    def disconnect(self):
        if self._pipe.isalive():
            try:
                self.set_timeout(0)
                self.send_command("exit")
            except:
                pass
        if self._pipe.isalive():
            self._pipe.kill()

    def connect(self, server, username, password, port = 22, accept_figerprint = False, command = "ssh", port_option = "-p %d", expect_token = "#-->", submit_token="\n", login_expect = None):

        pop = port_option % int(port)
        cmd = which(command)
        pipe = Process(cmd,[pop, "%s@%s" % (username, server)], terminal_required = True)


        super(BaseSecureTerminal, self).__init__(pipe,expect_token, submit_token, initiate_pipe = False)       

        self.set_timeout(10) 

        if login_expect:
            self.push_expect(re.compile(r"(password:|Password:|\(yes/no\)\?|\$|sftp\>|%s)"%login_expect))

        else:
            self.push_expect(re.compile(r"(password:|Password:|\(yes/no\)\?|\$|sftp\>)"))

        try:
            out = self.expect()
        except:
            raise BaseSecureTerminalLoginError(self.buffer)

#        print "END OF EXPECT", out
        newfigerprint = "(yes/no)" in out

        if not accept_figerprint and newfigerprint:
            raise BaseSecureTerminalLoginError("Accept figerprint required, but not allowed by user")
        elif newfigerprint:
            self.send_command("yes")
        

        if "Password" in out or "password" in out:
            out = self.send_command(password, False)

            if "Password:" in out or "password" in out:
                print self.buffer
                raise BaseSecureTerminalLoginError("Wrong username or password.")

        self._path = posixpath
        self.pop_expect()
        self.set_timeout(40) 
        self.initiate_pipe()


    def connection_lost(self,timeout = 10):
        teststring = "hello world"
        deleteit = chr(127)*len(teststring)
        self._pipe.write(teststring)
        end_time = time.time()+timeout

        success = self._pipe.isalive()
        if success:
            output = self.consume_output()
            while output.strip() != teststring:
                output += self.consume_output()
            
                if end_time < time.time():
                    success = False
                    break
        if success:
            self._pipe.write(deleteit)
            self.consume_output(wait_for_some_output = True)
        return not success
            


class SSHTerminal(BaseSecureTerminal, BashTerminal):
    """
    The object will be initiated as first as a BaseSecureTerminal and
    secondly as a BashTerminal. The order of the inheritance is important
    as the constructor which is called first will open the pipe
    (i.e. either SSH or Bash). Moreover, the SSHTerminal inherits
    all the functionality of the bash terminal (which makes sense as it
    just is a remote bash terminal after successful login).

    Following code gives an example of how the SSH Terminal may be used
    to pull out a list of files and directories:

    .. code-block:: python

       import getpass
       server = raw_input("Server:")
       if server == "": server = "localhost"
       username = ""
       while username == "":  username = raw_input("Username:")
       pas = getpass.getpass()
       x = SSHTerminal(server, username, pas)
   
       files, dirs = x.list(recursively = False)
       pwd = x.pwd()
 
       print "On", server, "is following files "
       for file in files:
          print "     -", file

       print "and following directories"
       for d in dirs:
          print "     -", d,"."

       print "in", pwd
    """

    def __init__(self, server, username, password, port = 22, accept_figerprint = False, login_expect = None):

        super(SSHTerminal,self).__init__(server, username, password, port, accept_figerprint, "ssh", login_expect = login_expect)


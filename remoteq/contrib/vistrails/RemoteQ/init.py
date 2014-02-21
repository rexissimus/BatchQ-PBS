###############################################################################
##
## Copyright (C) 2011-2014, NYU-Poly.
## Copyright (C) 2006-2011, University of Utah. 
## All rights reserved.
## Contact: contact@vistrails.org
##
## This file is part of VisTrails.
##
## "Redistribution and use in source and binary forms, with or without 
## modification, are permitted provided that the following conditions are met:
##
##  - Redistributions of source code must retain the above copyright notice, 
##    this list of conditions and the following disclaimer.
##  - Redistributions in binary form must reproduce the above copyright 
##    notice, this list of conditions and the following disclaimer in the 
##    documentation and/or other materials provided with the distribution.
##  - Neither the name of the University of Utah nor the names of its 
##    contributors may be used to endorse or promote products derived from 
##    this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
## THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
## PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR 
## CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
## EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
## PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
## OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
## WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
## OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
## ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."
##
###############################################################################
from vistrails.core.modules.config import ModuleSettings
from vistrails.core.modules.module_registry import get_module_registry
from vistrails.core.modules.vistrails_module import Module, ModuleError, ModuleSuspended
from vistrails.core.system import current_user
from vistrails.core.interpreter.job import JobMixin, JobMonitor

from remoteq.pipelines.shell import FileCommander as BQMachine
from remoteq.core.stack import select_machine, end_machine, use_machine, \
                                                                current_machine
from remoteq.batch.commandline import PBS, PBSScript
from remoteq.batch.directories import CreateDirectory
from remoteq.batch.files import TransferFiles
from remoteq.pipelines.shell.ssh import SSHTerminal

import hashlib

class Machine(Module, BQMachine):
    _input_ports = [('server', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('port', '(edu.utah.sci.vistrails.basic:Integer)', True),
                    ('username', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('password', '(edu.utah.sci.vistrails.basic:String)', True),
                   ]
    
    def compute(self):
        server = self.getInputFromPort('server') \
              if self.hasInputFromPort('server') else 'localhost'
        port = self.getInputFromPort('port') \
            if self.hasInputFromPort('port') else 22
        username = self.getInputFromPort('username') \
                if self.hasInputFromPort('username') else current_user()
        password = self.getInputFromPort('password') \
                if self.hasInputFromPort('password') else ''
        self.machine = Machine.create_machine(server, username, password, port)
        self.setResult("value", self)

    @staticmethod
    def create_machine(server, username, password, port):
        machine = BQMachine(server, username, password, port)
        # force creation of server-side help files
        select_machine(machine)
        end_machine()
        return machine

    @property
    def remote(self):
        return self.machine.remote

    @property
    def local(self):
        return self.machine.local

Machine._output_ports = [('value', Machine)]

class RQModule(Module):
    _settings = ModuleSettings(abstract=True)
    # a tuple (server, port, username, machine)
    default_machine = None

    def get_machine(self):
        if self.hasInputFromPort('machine'):
            return machine
        return self.get_default_machine()

    def get_default_machine(self):
        server = username = port = password = ''
        if configuration.check('server'):
            server = configuration.server
        if not server:
            raise ModuleError(self, 'No Machine specified. Either add a '
                                    'default machine, or a Machine module.')
        if configuration.check('username'):
            username = configuration.username
        if not username:
            username = current_user()
        if configuration.check('port') is not None:
            port = configuration.port
        # check if it exists or have changed
        if RQModule.default_machine and \
           (server, port, username) == RQModule.default_machine[:3]:
            return RQModule.default_machine[3]
        if configuration.check('password'):
            text = 'Enter password for server %s' % configuration.server
            from PyQt4 import QtGui
            (password, ok) = QtGui.QInputDialog.getText(None, text, text,
                                                  QtGui.QLineEdit.Password)
            if not ok:
                raise ModuleError(self, "Canceled password")
        machine = Machine()
        machine.machine = Machine.create_machine(server,
                                                 username,
                                                 password,
                                                 port)
        self.annotate({'RemoteQ-server':server,
                       'RemoteQ-username':username,
                       'RemoteQ-port':port})
        RQModule.default_machine = (server, port, username, machine)
        return machine

class RunCommand(RQModule):
    _input_ports = [('machine', Machine),
                    ('command', '(edu.utah.sci.vistrails.basic:String)', True),
                   ]
    
    _output_ports = [('machine', Machine),
                     ('output', '(edu.utah.sci.vistrails.basic:String)'),
                    ]
    
    def compute(self):
        machine = self.get_machine().machine
        if not self.hasInputFromPort('command'):
            raise ModuleError(self, "No command specified")
        command = self.getInputFromPort('command').strip()

        jm = JobMonitor.getInstance()
        cache = jm.getCache(self.signature)
        if cache:
            result = cache['result']
        else:
            ## This indicates that the coming commands submitted on the machine
            # trick to select machine without initializing every time
            use_machine(machine)
            m = current_machine()
            result = m.remote.send_command(command)
            end_machine()
            cache = jm.setCache(self.signature, {'result':result})
        self.setResult("output", result)
        self.setResult("machine", self.getInputFromPort('machine'))

class PBSJob(RQModule):
    _input_ports = [('machine', Machine),
                    ('command', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('working_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('input_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('processes', '(edu.utah.sci.vistrails.basic:Integer)', True),
                    ('time', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('mpi', '(edu.utah.sci.vistrails.basic:Boolean)', True),
                    ('threads', '(edu.utah.sci.vistrails.basic:Integer)', True),
                    ('memory', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('diskspace', '(edu.utah.sci.vistrails.basic:String)', True),
                   ]
    
    _output_ports = [('stdout', '(edu.utah.sci.vistrails.basic:String)'),
                     ('stderr', '(edu.utah.sci.vistrails.basic:String)'),
                     ('file_list', '(edu.utah.sci.vistrails.basic:List)'),
                    ]
    
    def compute(self):
        self.is_cacheable = lambda *args, **kwargs: False
        machine = self.get_machine().machine
        if not self.hasInputFromPort('command'):
            raise ModuleError(self, "No command specified")
        command = self.getInputFromPort('command').strip()
        working_directory = self.getInputFromPort('working_directory') \
              if self.hasInputFromPort('working_directory') else '.'
        if not self.hasInputFromPort('input_directory'):
            raise ModuleError(self, "No input directory specified")
        input_directory = self.getInputFromPort('input_directory').strip()
        additional_arguments = {'processes': 1, 'time': -1, 'mpi': False,
                                'threads': 1, 'memory':-1, 'diskspace': -1}
        for k in additional_arguments:
            if self.hasInputFromPort(k):
                additional_arguments[k] = self.getInputFromPort(k)
        ## This indicates that the coming commands submitted on the machine
        # trick to select machine without initializing every time

        use_machine(machine)
        cdir = CreateDirectory("remote", working_directory)
        trans = TransferFiles("remote", input_directory, working_directory,
                              dependencies = [cdir])
        job = PBS("remote", command, working_directory, dependencies = [trans],
                  **additional_arguments)
        job.run()
        try:
            ret = job._ret
            if ret:
                job_id = int(ret)
        except ValueError:
            end_machine()
            raise ModuleError(self, "Error submitting job: %s" % ret)
        finished = job.finished()
        job_info = job.get_job_info()
        if job_info:
            self.annotate({'job_info': job.get_job_info()})
        if not finished:
            status = job.status()
            # try to get more detailed information about the job
            # this only seems to work on some versions of torque
            if job_info:
                comment = [line for line in job_info.split('\n') if line.startswith('comment =')]
                if comment:
                    status += ': ' + comment[10:]
            end_machine()
            raise ModuleSuspended(self, '%s' % status, queue=job)
        self.is_cacheable = lambda *args, **kwargs: True
        # copies the created files to the client
        get_result = TransferFiles("local", input_directory, working_directory,
                              dependencies = [cdir])
        get_result.run()
        ## Popping from the machine stack                                                                                                                                     
        end_machine()
        self.setResult("stdout", job.standard_output())
        self.setResult("stderr", job.standard_error())
        files = machine.local.send_command("ls -l %s" % input_directory)
        self.setResult("file_list",
                       [f.split(' ')[-1] for f in files.split('\n')[1:]])

class RunPBSScript(JobMixin,RQModule):
    _input_ports = [('machine', Machine),
                    ('command', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('working_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('input_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('processes', '(edu.utah.sci.vistrails.basic:Integer)', True),
                    ('time', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('mpi', '(edu.utah.sci.vistrails.basic:Boolean)', True),
                    ('threads', '(edu.utah.sci.vistrails.basic:Integer)', True),
                    ('memory', '(edu.utah.sci.vistrails.basic:String)', True),
                    ('diskspace', '(edu.utah.sci.vistrails.basic:String)', True),
                   ]
    
    _output_ports = [('stdout', '(edu.utah.sci.vistrails.basic:String)'),
                     ('stderr', '(edu.utah.sci.vistrails.basic:String)'),
                    ]
    
    def getId(self, params):
        return hashlib.md5(params['input_directory'] +
                           params['command'] +
                           params['working_directory']).hexdigest()
        
    def readInputs(self):
        self.job = None
        d = {}
        machine = self.get_machine().machine
        if not self.hasInputFromPort('command'):
            raise ModuleError(self, "No command specified")
        d['command'] = self.getInputFromPort('command').strip()
        d['working_directory'] = self.getInputFromPort('working_directory') \
              if self.hasInputFromPort('working_directory') else '.'
        if not self.hasInputFromPort('input_directory'):
            raise ModuleError(self, "No input directory specified")
        d['input_directory'] = self.getInputFromPort('input_directory').strip()
        d['additional_arguments'] = {'processes': 1, 'time': -1, 'mpi': False,
                                'threads': 1, 'memory':-1, 'diskspace': -1}
        for k in d['additional_arguments']:
            if self.hasInputFromPort(k):
                d['additional_arguments'][k] = self.getInputFromPort(k)
        return d

    def startJob(self, params):
        work_dir = params['working_directory']
        use_machine(self.machine)
        self.cdir = CreateDirectory("remote", work_dir)
        trans = TransferFiles("remote", params['input_directory'], work_dir,
                              dependencies = [self.cdir])
        self.job = PBSScript("remote", params['command'], work_dir,
                      dependencies = [trans], **params['additional_arguments'])
        self.job.run()
        try:
            ret = self.job._ret
            if ret:
                job_id = int(ret)
        except ValueError:
            end_machine()
            raise ModuleError(self, "Error submitting job: %s" % ret)
        return params
        
    def getMonitor(self, params):
        if not self.job:
            self.startJob(params)
        return self.job

    def finishJob(self, params):
        job_info = self.job.get_job_info()
        if job_info:
            self.annotate({'job_info': job_info})
        # copies the created files to the client
        get_result = TransferFiles("local", params['input_directory'],
                                   params['working_directory'],
                                   dependencies = [self.cdir])
        get_result.run()
        end_machine()
        stdout = self.job.standard_output()
        stderr = self.job.standard_error()
        return {'stdout':stdout, 'stderr':stderr}

    def setResults(self, params):
        self.setResult('stdout', params['stdout'])
        self.setResult('stderr', params['stderr'])

class SyncDirectories(RQModule):
    _input_ports = [('machine', Machine),
                    ('local_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('remote_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('to_local', '(edu.utah.sci.vistrails.basic:Boolean)'),
                   ]
    
    _output_ports = [('machine', Machine),
                    ]
    
    def compute(self):
        self.is_cacheable = lambda *args, **kwargs: False
        machine = self.get_machine().machine
        if not self.hasInputFromPort('local_directory'):
            raise ModuleError(self, "No local directory specified")
        local_directory = self.getInputFromPort('local_directory').strip()
        if not self.hasInputFromPort('remote_directory'):
            raise ModuleError(self, "No remote directory specified")
        remote_directory = self.getInputFromPort('remote_directory').strip()
        whereto = 'remote'
        if self.hasInputFromPort('to_local') and self.getInputFromPort('to_local'):
            whereto = 'local'


        jm = JobMonitor.getInstance()
        cache = jm.getCache(self.signature)
        if not cache:
            ## This indicates that the coming commands submitted on the machine
            # trick to select machine without initializing every time

            use_machine(machine)
            to_dir = local_directory if whereto=='local' else remote_directory
            cdir = CreateDirectory(whereto, to_dir)
            job = TransferFiles(whereto, local_directory, remote_directory,
                              dependencies = [cdir])
            job.run()
            end_machine()
            cache = jm.setCache(self.signature, {'result':''})

        self.setResult("machine", machine)

class CopyFile(RQModule):
    _input_ports = [('machine', Machine),
                    ('local_file', '(edu.utah.sci.vistrails.basic:String)'),
                    ('remote_file', '(edu.utah.sci.vistrails.basic:String)'),
                    ('to_local', '(edu.utah.sci.vistrails.basic:Boolean)'),
                   ]
    
    _output_ports = [('machine', Machine),
                    ('output', '(edu.utah.sci.vistrails.basic:String)'),
                    ]
    
    def compute(self):
        machine = self.get_machine().machine
        if not self.hasInputFromPort('local_file'):
            raise ModuleError(self, "No local file specified")
        local_file = self.getInputFromPort('local_file').strip()
        if not self.hasInputFromPort('remote_file'):
            raise ModuleError(self, "No remote file specified")
        remote_file = self.getInputFromPort('remote_file').strip()
        whereto = 'remote'
        if self.hasInputFromPort('to_local') and self.getInputFromPort('to_local'):
            whereto = 'local'

        jm = JobMonitor.getInstance()
        cache = jm.getCache(self.signature)
        if cache:
            result = cache['result']
        else:
            ## This indicates that the coming commands submitted on the machine
            # trick to select machine without initializing every time
            command = machine.getfile if whereto=='local' else machine.sendfile
            result = command(local_file, remote_file)
            cache = jm.setCache(self.signature, {'result':result})

        self.setResult("machine", self.getInputFromPort('machine'))
        self.setResult("output", result)


def initialize():
    global _modules
    _modules = [Machine, RQModule, PBSJob, RunPBSScript, RunCommand,
                SyncDirectories, CopyFile]
    import base
    import hdfs
    import streaming
    _modules.extend(base.register())
    _modules.extend(hdfs.register())
    _modules.extend(streaming.register())

_modules = []

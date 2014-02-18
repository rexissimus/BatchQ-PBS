from vistrails.core.modules.vistrails_module import Module, ModuleError, ModuleSuspended
from vistrails.core.system import current_user
from vistrails.core.interpreter.job import JobMixin, JobMonitor
from batchq.pipelines.shell import FileCommander as BQMachine
from batchq.core.stack import select_machine, end_machine, use_machine, \
                                                                current_machine
from batchq.batch.commandline import PBS, PBSScript
from batchq.batch.directories import CreateDirectory
from batchq.batch.files import TransferFiles
from batchq.pipelines.shell.ssh import SSHTerminal

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
        self.machine = BQMachine(server, username, password, port)
        # force creation of server-side help files
        select_machine(self.machine)
        end_machine()
        self.setResult("value", self)

    @property
    def remote(self):
        return self.machine.remote

    @property
    def local(self):
        return self.machine.local

Machine._output_ports = [('value', Machine)]

    
class RunCommand(Module):
    _input_ports = [('machine', Machine),
                    ('command', '(edu.utah.sci.vistrails.basic:String)', True),
                   ]
    
    _output_ports = [('machine', Machine),
                     ('output', '(edu.utah.sci.vistrails.basic:String)'),
                    ]
    
    def compute(self):
        if not self.hasInputFromPort('machine'):
            raise ModuleError(self, "No machine specified")
        if not self.hasInputFromPort('command'):
            raise ModuleError(self, "No command specified")
        command = self.getInputFromPort('command').strip()
        machine = self.getInputFromPort('machine').machine

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

class PBSJob(Module):
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
        if not self.hasInputFromPort('machine'):
            raise ModuleError(self, "No machine specified")
        machine = self.getInputFromPort('machine').machine
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

class RunPBSScript(JobMixin,Module):
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
        if not self.hasInputFromPort('machine'):
            raise ModuleError(self, "No machine specified")
        self.machine = self.getInputFromPort('machine').machine
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

class SyncDirectories(Module):
    _input_ports = [('machine', Machine),
                    ('local_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('remote_directory', '(edu.utah.sci.vistrails.basic:String)'),
                    ('to_local', '(edu.utah.sci.vistrails.basic:Boolean)'),
                   ]
    
    _output_ports = [('machine', Machine),
                    ]
    
    def compute(self):
        self.is_cacheable = lambda *args, **kwargs: False
        if not self.hasInputFromPort('machine'):
            raise ModuleError(self, "No machine specified")
        machine = self.getInputFromPort('machine').machine
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

class CopyFile(Module):
    _input_ports = [('machine', Machine),
                    ('local_file', '(edu.utah.sci.vistrails.basic:String)'),
                    ('remote_file', '(edu.utah.sci.vistrails.basic:String)'),
                    ('to_local', '(edu.utah.sci.vistrails.basic:Boolean)'),
                   ]
    
    _output_ports = [('machine', Machine),
                    ('output', '(edu.utah.sci.vistrails.basic:String)'),
                    ]
    
    def compute(self):
        if not self.hasInputFromPort('machine'):
            raise ModuleError(self, "No machine specified")
        machine = self.getInputFromPort('machine').machine
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

_modules = [Machine, PBSJob, RunPBSScript, RunCommand, SyncDirectories, CopyFile]

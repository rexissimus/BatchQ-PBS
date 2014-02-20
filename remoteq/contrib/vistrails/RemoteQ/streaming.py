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
""" Wrapper for Hadoop Streaming to use with Python mapper/reducer,
cache files, etc...  """

import copy
from vistrails.core.interpreter.job import JobMixin, JobMonitor
from vistrails.core.modules.basic_modules import File, String
from vistrails.core.modules.config import IPort, OPort
from vistrails.core.modules.vistrails_module import NotCacheable, ModuleError
from base import HadoopBaseModule
from remoteq.core.stack import select_machine, end_machine, use_machine, \
                                                                current_machine
from remoteq.batch.commandline import Subshell
import os.path
################################################################################
class HadoopStreaming(JobMixin,HadoopBaseModule):
    """
    The class for executing MapReduce using Hadoop Streaming with
    customized Python Mapper/Reducer/Combiner
    
    """
    _input_ports = [IPort('Mapper',       File),
                    IPort('Reducer',      File),
                    IPort('Combiner',     File),
                    IPort('Workdir',      String),
                    IPort('Identifier',   String),
                    IPort('Input',        String),
                    IPort('Output',       String),
                    IPort('CacheFile',    String),
                    IPort('CacheArchive', String),
                    IPort('Environment',  String),
                    IPort('Machine', '(org.vistrails.vistrails.remoteq:Machine)')]

    _output_ports = [OPort('Machine', '(org.vistrails.vistrails.remoteq:Machine)'),
                     OPort('Output', String)]

    def __init__(self):
        HadoopBaseModule.__init__(self)
        self.job = None
        self.job_machine = None

    def readInputs(self):
        p = {}
        self.localMapper = self.forceGetInputFromPort('Mapper')
        self.localReducer = self.forceGetInputFromPort('Reducer')
        self.localCombiner = self.forceGetInputFromPort('Combiner')
        p['workdir'] = self.forceGetInputFromPort('Workdir')
        if p['workdir']==None:
            p['workdir'] = ".vistrails-hadoop"
        p['job_identifier'] = self.forceGetInputFromPort('Identifier')
        if p['job_identifier'] == None:
            raise ModuleError(self, 'Job Identifier is required')
        p['input'] = self.forceGetInputFromPort('Input')
        p['output'] = self.forceGetInputFromPort('Output')
        if p['input']==None or p['output']==None:
            raise ModuleError(self, 'Input and Output are required')
        p['files'] = self.forceGetInputListFromPort('CacheFile')
        p['cacheArchives'] = self.forceGetInputListFromPort('CacheArchive')
        p['envVars'] = self.forceGetInputListFromPort('Environment') 
        self.job_machine = self.getInputFromPort('Machine')
        return p

    def getId(self, p):
        return p['job_identifier']

    def createJob(self, p):
        use_machine(self.job_machine.machine)
        self.job = Subshell("remote", command="%s",
                            working_directory=p['workdir'],
                            identifier=p['job_identifier'])

    def startJob(self, p):
        if not self.job_machine.remote.isdir(p['workdir']):
            self.job_machine.remote.mkdir(p['workdir'])
        self.createJob(p)
        self.job.reset()

        # Now generate the command line
        config = self.get_hadoop_config(self.job_machine)
        command = 'jar %s' % config['streaming.jar']
        generics = ''
        arguments = ''

        arguments += ' -input %s -output %s' % (p['input'], p['output'])
        
        if self.localMapper!=None:
            tempfile = self.job_machine.remote.send_command('mktemp').strip()
            result = self.job_machine.machine.sendfile(self.localMapper.name,
                                                       tempfile)
            mapperFileName = os.path.split(tempfile)[1]
            p['files'].append(tempfile)
            arguments += ' -mapper %s' % mapperFileName
        else:
            arguments += ' -mapper org.apache.hadoop.mapred.lib.IdentityMapper'

        if self.localCombiner!=None:
            tempfile = self.job_machine.remote.send_command('mktemp').strip()
            result = self.job_machine.machine.sendfile(self.localCombiner.name,
                                                       tempfile)
            combinerFileName = os.path.split(tempfile)[1]
            p['files'].append(tempfile)
            arguments += ' -combiner %s' % combinerFileName

        if self.localReducer!=None:
            tempfile = self.job_machine.remote.send_command('mktemp').strip()
            result = self.job_machine.machine.sendfile(self.localReducer.name,
                                                       tempfile)
            reducerFileName = os.path.split(tempfile)[1]
            p['files'].append(tempfile)
            arguments += ' -reducer %s' % reducerFileName
        else:
            arguments += ' -numReduceTasks 0'

        for var in p['envVars']:
            arguments += ' -cmdenv ' + var

        for cacheArchive in p['cacheArchives']:
            arguments += ' -cacheArchive %s' % cacheArchive

        # files is a generic command and needs to be first
        if p['files']:
            generics += ' -files ' + ','.join(p['files'])

        arguments = command + generics + arguments
        result = self.call_hadoop(arguments, p['workdir'],
                                  p['job_identifier'], self.job_machine)
        return p

    def getMonitor(self, p):
        if not self.job:
            self.createJob(p)
        return self.job

    def finishJob(self, p):
        r = {}
        r['output'] = p['output']
        r['workdir'] = p['workdir']
        r['job_identifier'] = p['job_identifier']
        
        if self.job.failed():
            error = self.job.standard_error()
            raise ModuleError(self, error)
        return r

    def setResults(self, p):
        self.setResult('Output', p['output'])
        self.setResult('Machine', self.job_machine)

    def call_hadoop(self, arguments, workdir, identifier, machine):
        config = self.get_hadoop_config(machine)
        argList = [config['hadoop']]
        if type(arguments) in [str, unicode]:
            argList += arguments.split(' ')
        elif type(arguments)==list:
            argList += arguments
        else:
            raise ModuleError(self, 'Invalid argument types to hadoop')
        self.job.command = self.job.command % " ".join(argList)
        self.job.run()

################################################################################
class URICreator(NotCacheable,HadoopBaseModule):
    """
    The class for caching HDFS file onto the TaskNode local drive
    
    """
    _input_ports = [IPort('HDFS File/URI', String),
                    IPort('Symlink',       String),
                    IPort('Machine', '(org.vistrails.vistrails.remoteq:Machine)')]

    _output_ports = [OPort('Machine', '(org.vistrails.vistrails.remoteq:Machine)'),
                     OPort('URI', String)]

    def compute(self):
        machine = self.forceGetInputFromPort('Machine')
        uri = self.forceGetInputFromPort('HDFS File/URI')
        symlink = self.forceGetInputFromPort('Symlink')
        if uri==None or symlink==None:
            raise ModuleError(self, "Missing 'HDFS File/URI' or 'Symlink' values")
        jm = JobMonitor.getInstance()
        id = 'URICreator' + uri + symlink
        job = jm.getCache(id)
        if not job:
            if '://' not in uri:
                prefix = self.get_hadoop_config(machine)['fs.defaultFS']
                uri = prefix + uri
            uri += '#' + symlink
            jm.setCache(id, {}, name='URICreator(%s)'%uri)
        self.setResult('URI', uri)
        self.setResult('Machine', machine)
       

################################################################################
def register():
    return [URICreator, HadoopStreaming]

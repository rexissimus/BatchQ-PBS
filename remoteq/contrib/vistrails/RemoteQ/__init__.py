version = '0.2'

identifier = 'org.vistrails.vistrails.remoteq'
name = 'RemoteQ'

old_identifiers = ['org.vistrails.pbs']

def package_requirements():
    import core.requirements
    if not core.requirements.python_module_exists('remoteq'):
        raise core.requirements.MissingRequirement('remoteq')


version = '0.1'
identifier = 'org.vistrails.pbs'
name = 'Remote PBS'

def package_requirements():
    import core.requirements
    if not core.requirements.python_module_exists('batchq'):
        raise core.requirements.MissingRequirement('batchq')


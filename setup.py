from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
buildOptions = dict(
    packages=["os", "yaml", "boto", "numpy", "scipy", "matplotlib", "io",
              "zipfile", "bz2", "math", "datetime", "dateutil", "time", "h5py"],
    excludes=[]
)

base = 'Console'

executables = [
    Executable('SidServerDataProcessor.py', base=base)
]

setup(name='pySidServerDataProcessor',
      version='0.1.0.0',
      description='SID Server Data Processor',
      options=dict(build_exe=buildOptions),
      executables=executables)
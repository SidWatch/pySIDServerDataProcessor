from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
buildOptions = dict(
    packages=["os", "yaml", "boto", "numpy", "scipy", "matplotlib", "io", "zlib",
              "zipfile", "bz2", "math", "datetime", "dateutil", "time", "h5py"],
    include_files=['SIDServer/', 'ServiceHandler'],
    excludes=[]
)

base = 'Console'

executables = [
    Executable('SidServerDataProcessor.py', base='Win32Service', targetName='SidServerDataProcessorService.exe')
]

setup(name='pySidServerDataProcessorService',
      version='0.1.0.0',
      description='SID Server Data Processor Service',
      options=dict(build_exe=buildOptions),
      executables=executables)


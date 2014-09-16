from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
options = {'build_exe': {
    'packages': ['os', 'yaml', 'boto', 'numpy', 'scipy', 'matplotlib', 'io', 'zlib',
                 'zipfile', 'bz2', 'math', 'datetime', 'dateutil', 'time', 'h5py'],
    'include_files': ['SIDServer/'],
    'excludes': [],
    "build_exe": '../build/Output/Win32/'
}
}

executable_to_build = [
    Executable('SidServerDataProcessor.py',
               base='console',
               targetDir='../build/Output/Win32/')
]

setup(name='pySidServerDataProcessor',
      version='0.1.0.0',
      description='SID Server Data Processor',
      options=options,
      executables=executable_to_build
)

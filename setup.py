import setuptools

try:
    from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
    from distutils.command.build_py import build_py


setuptools.setup(
    name='couch-chain',
    version='0.1',
    description='A tool for processing couchdb _changes.',
    url='',
    author='Kris Bandurski',
    author_email='krzysztof.bandurski@gmail.com',
    license='MIT',
    packages=setuptools.find_packages(),
    zip_safe=False,
    install_requires=[
        'boto',
        'futures',
        'elasticsearch',
        'futures',
        'gnureadline',
        'ipython',
        'mock',
        'pycouchdb',
        'requests',
        'urllib3',
        'redis',
    ],
    dependency_links=[
        (
            'git+https://github.com/krisb78/py-couchdb.git'
            '@4e1b81e83582e04eb5300649ac54fdc798a9044a#egg=pycouchdb'
        ),
    ],
    cmdclass={'build_py': build_py},
)

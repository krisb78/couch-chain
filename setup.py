from setuptools import setup

setup(
    name='couch-chain',
    version='0.1',
    description='A tool for processing couchdb _changes.',
    url='',
    author='Kris Bandurski',
    author_email='krzysztof.bandurski@gmail.com',
    license='MIT',
    packages=['cchain'],
    zip_safe=False,
    install_requires=[
        'elasticsearch',
        'gnureadline',
        'ipython',
        'pycouchdb',
        'requests',
        'urllib3',
        'wsgiref',
        'redis',
    ],
    dependency_links=[
        (
            '-e git+https://github.com/krisb78/py-couchdb.git'
            '@73531531f0f939a7cce90d0eddea9c843b43aff5#egg=pycouchdb'
        ),
    ]
)

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
        'boto',
        'futures',
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
            '@36e117fa58324400f5e99a6e9fa3abec2362f607#egg=pycouchdb'
        ),
    ]
)

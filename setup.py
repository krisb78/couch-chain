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
    ]
)

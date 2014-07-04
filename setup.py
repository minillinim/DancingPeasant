from distutils.core import setup

setup(
    name='DancingPeasant',
    version='0.0.2',
    author='Michael Imelfort',
    author_email='mike@mikeimelfort.com',
    packages=['dancingPeasant'],
    scripts=['bin/dancingPeasant'],
    url='http://pypi.python.org/pypi/DancingPeasant/',
    license='GPLv3',
    description='Utilities that allow a batch of CSV files to be passed as a single SQLITE db',
    long_description=open('README.md').read(),
    install_requires=[],
)


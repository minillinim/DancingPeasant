from distutils.core import setup

setup(
    name='DancingPeasant',
    version='0.0.1',
    author='Michael Imelfort',
    author_email='mike@mikeimelfort.com',
    packages=['dancingpeasant'],
    scripts=['bin/DancingPeasant'],
    url='http://pypi.python.org/pypi/DancingPeasant/',
    license='GPLv3',
    description='DancingPeasant',
    long_description=open('README.md').read(),
    install_requires=[],
)


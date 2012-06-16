from setuptools import setup,find_packages

setup(
    name='triv.io.datasources',
    version = '0.1',
    description='Third party datasources for htttp://triv.io',
    author='Scott Robertson',
    author_email='srobertson@codeit.com',
    packages = find_packages(),
    install_requires = ['boto']

)
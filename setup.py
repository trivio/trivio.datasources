from setuptools import setup,find_packages

setup(
  name='triv.io.datasources',
  version = '0.1',
  description='Third party datasources for htttp://triv.io',
  author='Scott Robertson',
  author_email='scott@triv.io',
  packages = find_packages(),

  dependency_links = [
    'https://github.com/trivio/Hadoop/tarball/master#egg=Hadoop-0.1'
  ],
  install_requires = [
    'boto',
    'Hadoop',
    'warc',
    'python-dateutil==1.5',
    'MySQL-python'
  ]

)
from distutils.core import setup

setup(
    name = 'db_to_pandas',
    version = '0.1',
    description = 'Database operation with Pandas',
    author = 'Ryan Brunt, Greg Romrell',
    author_email = 'grromrell@gmail.com',
    packages = ['db_to_pandas'],
    package_dir = {'db_to_pandas' : 'db_to_pandas'}
)

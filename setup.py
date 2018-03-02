from distutils.core import setup

setup(
    name = 'db_pandas',
    version = '0.4',
    description = 'Database operation with Pandas',
    author = 'Ryan Brunt, Greg Romrell',
    author_email = 'grromrell@gmail.com',
    packages = ['db_pandas'],
    package_dir={'db_pandas':'db_pandas'},
    install_requires = [
        'pandas',
        'sqlalchemy'
        ]
)

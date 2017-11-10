from distutils.core import setup

setup(
    name = 'db_to_pandas',
    version = '0.4',
    description = 'Database operation with Pandas',
    author = 'Ryan Brunt, Greg Romrell',
    author_email = 'grromrell@gmail.com',
    py_modules = ['db_to_pandas'],
    install_requires = [
        'pandas',
        'sqlalchemy'
        ]
)

from setuptools import setup, find_packages


# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
   name='phouse',
   version='1.4',
   long_description=long_description,
   long_description_content_type='text/markdown',
   description='work with clickhouse',
   author='Nader Bedretdinov',
   author_email='php-job@mail.ru',
   packages=find_packages()
)

 


from distutils.core import setup
from setuptools import Extension, find_packages

setup(
    name='atlas-metadata-extractor',
    packages=find_packages(),
    version='0.0.3',
    license='MIT',
    description='This project is used for extract metadata from data source',
    # Give a short description about your library
    author='pongthepv',
    author_email='mr.pongthep@gmail.com',
    url='https://github.com/pongthep/atlas-metadata-extractor',
    download_url='https://github.com/pongthep/atlas-metadata-extractor/archive/v0.0.1.tar.gz',
    # I explain this later on
    keywords=['apache atlas', 'atlas', 'extractor'],
    install_requires=[
        'psycopg2-binary',
        'requests',
        'mysql-connector-python',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
)

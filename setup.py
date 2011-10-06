from setuptools import setup, find_packages
import os

version = '0.1'

setup(
    name='memcachelock',
    version=version,
    description="Python wrapper to use memcache as a lock server",
    keywords='Memcache Lock',
    author='Vincent Pelletier',
    author_email='plr.vincent@gmail.com',
    license='GPL',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'setuptools',
        'python-memcached',
    ],
)


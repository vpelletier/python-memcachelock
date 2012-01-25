from setuptools import setup

setup(
    name='memcachelock',
    description="Use memcache as a lock server",
    keywords='memcache lock',
    version='0.2',
    author='Vincent Pelletier',
    author_email='plr.vincent@gmail.com',
    url='http://github.com/vpelletier/python-memcachelock',
    license='GPL',
    platforms=['any'],
    include_package_data=True,
    zip_safe=True,
    py_modules=['memcachelock'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
    ],
    install_requires=[
        'setuptools',
        'python-memcached',
    ],
)


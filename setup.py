# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = 'viruzzz-kun'
__version__ = '0.2'


if __name__ == '__main__':
    setup(
        name="bouser_ezekiel",
        version=__version__,
        description="Object locking service for Bouser",
        long_description='',
        author=__author__,
        author_email="viruzzz.soft@gmail.com",
        license='ISC',
        url="http://github.com/hitsl/bouser_ezekiel",
        packages=["bouser_ezekiel", ],
        zip_safe=False,
        package_data={},
        install_requires=[
            'bouser',
            'autobahn',
        ],
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Environment :: Plugins",
            "Programming Language :: Python",
        ])

from setuptools import setup
from Cython.Build import cythonize

setup(
    name='Pi Billion',
    ext_modules=cythonize("*.pyx", compiler_directives={
                          'language_level': "3"}),
    zip_safe=False,
)

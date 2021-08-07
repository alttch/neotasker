__version__ = '0.0.38'

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='neotasker',
    version=__version__,
    author='Altertech',
    author_email='div@altertech.com',
    description=
    'Lightweight thread and asyncio task library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/alttch/neotasker',
    packages=setuptools.find_packages(),
    license='MIT',
    install_requires=['aiosched'],
    classifiers=(
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries',
    ),
)

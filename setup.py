from setuptools import setup

setup(
    name='download_cats',
    version='0.1.0',
    url='https://github.com/uiucsn/download_cats',
    license='',
    author='Konstantin Malanchev',
    author_email='kostya@illinois.edu',
    description='Download astronomical catalogues',
    packages=['download_cats'],
    entry_points={'console_scripts': [
        'download-cats = download_cats.__main__:main',
    ]}
)

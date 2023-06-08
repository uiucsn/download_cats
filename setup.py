from setuptools import setup, find_packages

setup(
    name='download_cats',
    version='0.1.0',
    url='https://github.com/uiucsn/download_cats',
    license='',
    author='Konstantin Malanchev',
    author_email='kostya@illinois.edu',
    description='Download astronomical catalogues',
    packages=find_packages(),
    entry_points={'console_scripts': [
        'download-cats = download_cats.__main__:main',
        'put-cat-to-ch = put_cat_to_ch.__main__:main',
    ]},
    include_package_data=True,
)

import setuptools

with open('README.md') as readme:
    long_desc = readme.read()

setuptools.setup(
    name='pillar-mongodb-wrapper',
    version='0.0.1',
    author="PillarGG",
    author_email='chandler@pillar.gg',
    description='Our internal MongoDB wrapper.',
    long_description=long_desc,
    long_description_content_type="text/markdown",
    url='https://github.com/pillargg/pillar-mongodb-wrapper',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pymongo>=3.11.3',
        'moviepy>=1.0.3'
    ]
)

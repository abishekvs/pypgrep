import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypgrep",
    version="1.0.1",
    author="Abishek V S",
    author_email="abishekvs@gmail.com/pypa/example-project",
    description="Python PostgreSQL Logical Replication Utility",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/abishekvs/pypgrep",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
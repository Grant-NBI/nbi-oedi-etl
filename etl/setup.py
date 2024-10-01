# pylint: disable=import-error
# pyright: reportMissingImports=false
from setuptools import setup
import tomli # this is what became tomllib in version 3.11> of python. U will run this setup in python 3.9 as Glue pythonshell is 3.9

# Load the pyproject.toml file using tomli (read mode in binary)
with open("pyproject.toml", "rb") as f:
    pyproject_data = tomli.load(f)

# Extract the version from the pyproject.toml file
version = pyproject_data["tool"]["poetry"]["version"]

setup(
    name="oedi_etl",
    version=version,
    packages=["oedi_etl"]
)


import pathlib
import setuptools
from setuptools import setup

# This directory
path = pathlib.Path(__file__).parent

readme_content = (path / "README.md").read_text()

setup(name="rdfframes",
      version="0.9.1",
      description="Exposes RDF datasets from sparql endpoints for machine learning models in convenient formats like "
                  "pandas dataframe",
      long_description=readme_content,
      long_description_content_type="text/markdown",
      url="https://github.com/qcri/RDFframes",
      author="Aisha Mohamed, Zoi Kaoudi, Ghadeer Abuoda, Abdurrahman Ghanem",
      author_email="ahmohamed@qf.org.qa, zkaoudi@hbku.edu.qa, gabuoda@hbku.edu.qa, abghanem@hbku.edu.qa",
      classifiers=[
            "Programming Language :: Python :: 3.6", "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
      ],
      packages=setuptools.find_packages(),
      include_package_data=True,
      entry_points={},
      )

import pathlib
import setuptools
from setuptools import setup

# This directory
path = pathlib.Path(__file__).parent

readme_content = (path / "README.md").read_text()

setup(name="rdfframes",
      version="0.9.2",
      description="Exposes RDF datasets from sparql endpoints for machine learning models in convenient formats like "
                  "pandas dataframe",
      long_description=readme_content,
      long_description_content_type="text/markdown",
      url="https://github.com/qcri/RDFframes",
      author="Aisha Mohamed, Ghadeer Abuoda, Zoi Kaoudi, Abdurrahman Ghanem, Ashraf Aboulnaga",
      author_email="ahmohamed@qf.org.qa, gabuoda@hbku.edu.qa, zkaoudi@hbku.edu.qa, abghanem@hbku.edu.qa, aaboulnaga@hbku.edu.qa",
      license="MIT",
      classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6"],
      packages=setuptools.find_packages(),
      include_package_data=True,
      entry_points={},
      )

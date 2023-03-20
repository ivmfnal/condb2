import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open(os.path.join("condb", "version.py"), "r").read(), g)
    return g["Version"]


setup(
    name = "condb",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("Conditions Database (ConDB)"),
    license = "BSD 3-clause",
    keywords = "database, web service, conditions database",
    packages=['condb', 'condb.ui', 'condb.ui.cli'],
    zip_safe = False,
    install_requires=["psycopg2"],
    classifiers=[],
    entry_points = {
            "console_scripts": [
                "condb = condb.ui.condb_ui:main",
            ]
        }
)
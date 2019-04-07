import io
import os.path
import re
from setuptools import setup, find_packages


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names), encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r'^__version__ = [\'"]([^\'"]*)[\'"]', version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


pkg = "textio-parallel"
setup(
    name=pkg,
    version=find_version(pkg.replace("-", "_"), "__init__.py"),
    description="Parallelization Library",
    url="https://github.com/textioHQ/{}/".format(pkg),
    author="Ely Paysinger",
    author_email="ely@textio.com",
    packages=find_packages(),
    include_package_data=True,  # Use MANIFEST.in for package data
    install_requires=[],
)

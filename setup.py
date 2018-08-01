from setuptools import setup, find_packages


setup(
    name="distributed_systems_talk",
    version="0.5",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    extras_require={"testing": ["pytest", "pytest-mock"]},
)

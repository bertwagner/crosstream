import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="streaming_Data_joiner",
    version="0.0.1",
    author="Bert Wagner",
    package_dir={"": "streaming_data_joiner"},
    packages=setuptools.find_packages(where="streaming_data_joiner"),
    python_requires=">=3.6",
)
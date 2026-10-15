from setuptools import find_packages, setup

setup(
    name="source_instagram_organic",
    version="0.1.0",
    description="Conector Airbyte para conteúdo orgânico de contas do Instagram",
    author="Binder",
    packages=find_packages(exclude=("tests",)),
    install_requires=["airbyte-cdk>=7,<8"],
    package_data={"": ["*.json", "schemas/*.json"]},
    entry_points={"console_scripts": ["source-instagram-organic=source_instagram_organic.run:run"]},
)

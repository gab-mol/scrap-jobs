from setuptools import setup, find_packages

setup(
    name="jobnlp",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "beautifulsoup4",
        "jsonlines"
    ],
    entry_points={
        "console_scripts": [
            "fetch_raw=jobnlp.pipeline.fetch_raw:main",
            "clean_text=jobnlp.pipeline.clean_text:main",
            "nlp_extract=jobnlp.pipeline.nlp_extract:main",
            "entity_count=jobnlp.pipeline.entity_count:main"
        ]
    },
)
from setuptools import setup, find_packages

setup(
    name="olympia-restaurants",
    version="0.1.0",
    description="Aggregator for Olympia, WA restaurant data",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    install_requires=[
        "requests",
        "python-dotenv",
        "pandas",
        "overpy",
        "geocoder",
        "rapidfuzz",
        "shapely",
        "pyyaml",
        "XlsxWriter",
        "tqdm",
    ],
    entry_points={
        "console_scripts": [
            "refresh-restaurants=restaurants.refresh_restaurants:main",
            "toast-leads=restaurants.toast_leads:main",
            "restaurants-gui=restaurants.gui:main",
        ]
    },
)

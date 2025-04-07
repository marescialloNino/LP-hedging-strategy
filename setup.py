from setuptools import setup, find_packages

setup(
    name="lp_hedging_strategy",
    version="0.1.0",
    description="A package for LP hedging strategy monitoring and rebalancing",
    packages=find_packages(where="python"),  # automatically find packages under python/
    package_dir={"": "python"},  # tells setuptools that packages are under the src directory
    include_package_data=True,
    install_requires=[
        "ccxt>=1.0.0",          #
        "pandas>=1.0.0",
        "numpy>=1.0.0",
        "python-dotenv",
        "numba",
        
    ],
    entry_points={
        # Optionally define command line scripts if needed:
        # "console_scripts": [
        #     "lp-workflow=hedging_strategy.some_module:main",
        # ],
    },
)
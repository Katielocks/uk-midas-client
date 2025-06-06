import setuptools
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setuptools.setup(
    name="midas_client",                    
    version="0.3.1",                                                      
    author="Katherine Whitelock",
    author_email="ktwhitelock@outlook.com",
    description="A Python client for UK Met Office MIDAS via CEDA Archives",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/katielocks/uk_midas_client",
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires=">=3.10",                   
    install_requires=[
        "numpy>=1.21",   
        "pandas>=1.3",            
        "scikit-learn>=1.0",               
        "requests>=2.26",                      
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  
        "Operating System :: OS Independent",
    ],
)

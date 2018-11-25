import setuptools

# import ``__version__`` from code base
exec(open('lambdo/version.py').read())

setuptools.setup(
    name='lambdo',
    version=__version__,

    # descriptive metadata for upload to PyPI
    description='Feature engineering and machine learning: together at last!',
    author='Alexandr Savinov',
    author_email='savinov@conceptoriented.org',
    license='MIT License',
    keywords = "feature engineering machine learning data science analytics data mining forecasting time series",
    url='https://github.com/asavinov/lambdo',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers ",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
    ],

    test_suite='nose.collector',
    tests_require=['nose'],

    # dependencies
    install_requires=[
        'numpy>=1.15.0',
        'pandas>=0.23.0',
        'scipy>=1.1.0',
        'scikit-learn>=0.20.0',
        'statsmodels>=0.9.0',
        'patsy>=0.5.0',
    ],
    zip_safe=True,

    # package content (what to include)
    packages=setuptools.find_packages(),

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.md', '*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        #'hello': ['*.msg'],
    },

    # It will generate lambdo.exe and lambdo-script.py in the Scripts folder of Python
    entry_points={
        'console_scripts': [
            'lambdo=lambdo.main:main'
        ],
    },
    # The files will be copied to Scripts folder of Python
    scripts=[
        #'scripts/lambdo.bat',
        #'scripts/lambdo',
        #'scripts/lambdo.py',
    ],
)

Getting Started
===============

Installation::

    pip install arbalest

This will install Arbalest and any dependencies. However for Windows it may be
necessary to install `psycopg2`, a PostgreSQL database driver manually.

64 bit Python installation::

    pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win64-py27#egg=psycopg2

32 bit Python installation::

    pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win32-py27#egg=psycopg2

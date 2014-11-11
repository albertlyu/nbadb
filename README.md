# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

### Requirements

This project was built with Python 2.7.6, PostgreSQL 9.3.4. That does not mean it won't work in Python 3 or PostgreSQL 9.4, as I haven't tested that yet. It should work on both Windows and Unix operating systems. I think it's best that you create your nbadb within a virtual environment for easy replication.

In your nbadb folder, start a ```virtualenv``` instance (see the [virtualenv docs](http://virtualenv.readthedocs.org/en/latest/virtualenv.html) for more information) and install the required modules:

```
$ virtualenv ENV
$ \path\to\ENV\Scripts\activate # For Windows machines
$ pip install -r requirements.txt
```

If you are on a Windows machine and are unable to install ```psycopg2``` with the message 'error: Unable to find vcvarsall.bat,' you will need to install ```psycopg2``` directly as this is a known issue with installing ```psycopg2``` on Windows. To do so, run the following:

```
$ easy_install http://stickpeople.com/projects/python/win-psycopg/2.5.3/psycopg2-2.5.3.win32-py2.6-pg9.3.4-release.exe
```

For more details, see the [following link](http://stackoverflow.com/questions/5382801/where-can-i-download-binary-eggs-with-psycopg2-for-windows/5383266#5383266).

### Configuration

Update your config.ini file with your PostgreSQL credentials. You will need to create a new database called 'nbadb,' which you can do so by using PostgreSQL's ```createdb``` wrapper statement in command line or executing a ```CREATE DATABASE``` statement in a psql interpreter.

## ETL Details

### Staging Layer

nbadb starts by loading raw data from the source *as-is* and dumping them into staging tables. The source includes data from scoreboards, box scores, play-by-play logs, and shot chart detail. There are also data on players, including player profile information, player shot logs, and player rebound logs.

To load data from scoreboards, box scores, play-by-play logs, and shot charts into staging tables:
```
$ python load_staging.py 2014-10-28 2014-11-10 # start_date to end_date
```

Or simply running ```python load_staging.py``` will bring your staging layer up-to-date.

To load player and roster information, player shot logs, and player rebound logs:
```
$ python update_players.py
```

To drop all staging tables (including the player tables), simply run the drop_staging.py script:
```
$ python drop_staging.py
```

### Reporting Layer

TBD

## Credits
- Data courtesy of NBA.com
# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

### Requirements

This project was built with Python 2.7.5, PostgreSQL 9.4.0. That does not mean it won't work in Python 3 or PostgreSQL 9.4, as I haven't tested that yet. It should work on both Windows and Unix operating systems. I think it's best that you create your nbadb within a virtual environment for easy replication.

In your nbadb folder, start a ```virtualenv``` instance (see the [virtualenv docs](http://virtualenv.readthedocs.org/en/latest/virtualenv.html) for more information) and install the required modules:

```
$ virtualenv ENV
$ source ENV/bin/activate # For Unix machines
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

To load data from the entire 2013-14 season into staging tables:
```
$ python load_staging.py 2013-10-29 2014-04-16 # start to end of 2013-14 regular season
$ python update_players.py 2013-14 # loads data for 2013-14 season only
```

To load data from the beginning of the 2014-15 season until yesterday's games:
```
$ python load_staging.py # if no argument, start date set at '2014-10-28'
$ python update_players.py # if no argument, loads data for 2014-15 season only
```

To drop all staging tables (including the player tables), simply run the drop_staging.py script:
```
$ python drop_staging.py
```

### Reporting Layer

TBD

## Credits
- Data courtesy of NBA.com
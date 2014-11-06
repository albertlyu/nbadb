# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

### Requirements

This project was built with Python 2.7.6, PostgreSQL 9.3.4. That does not mean it won't work in Python 3 or PostgreSQL 9.4, as I haven't tested that yet. It should work on both Windows and Unix operating systems.

Details on Python module and package requirements will be provided later...

### Configuration

Update your config.ini file with your PostgreSQL credentials. You will need to create a new database called 'nbadb,' which you can do so by using PostgreSQL's ```createdb``` wrapper statement in command line or executing a CREATE DATABASE statement in a psql interpreter.

### Staging Layer (Work in Progress)

nbadb starts by loading raw data from the source *as-is* and dumping them into staging tables. The source includes data from scoreboards, box scores, play-by-play logs, and shot chart detail. There are also data on players, including player profile information, player shot logs, and player rebound logs.

To load data from scoreboards, box scores, play-by-play logs, and shot charts into staging tables:
```
python load_staging.py 2014-10-28 2014-11-04 # start_date to end_date
```

Or simply running ```python load_staging.py``` will bring your staging layer up-to-date.

To load player and roster information, player shot logs, and player rebound logs:
```
python update_players.py
```

To drop all staging tables (including the player tables), simply run the drop_staging.py script:
```
python drop_staging.py
```

### Reporting Layer

TBD

## Credits
- Data courtesy of NBA.com
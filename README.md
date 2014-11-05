# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

# Staging Layer

nbadb starts by loading raw data from the source *as-is* and dumping them into staging tables. The source data includes data from scoreboards, box scores, play-by-play logs, and shot chart detail. There are also data on players and rosters, including player profile information.

To load data from scoreboards, box scores, play-by-play logs, and shot charts into staging tables:
```
python load_staging.py 2014-10-28 2014-11-04 # start_date to end_date
```

Or simply running ```python load_staging.py``` will bring your staging layer up-to-date.

To load player and roster information:
```
python update_players.py
```

To drop all staging tables (including the player tables), simply run the drop_staging.py script:
```
python drop_staging.py
```

# Reporting Layer

TBD

## Credits
- Data courtesy of NBA.com
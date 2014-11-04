# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

To load staging tables from scoreboards, box scores, play-by-plays, and shot charts:
```
python load_staging.py 2014-10-28 2014-11-01 # start_date to end_date
```

To load player and roster information:
```
python update_players.py
```

To drop all staging tables, simply run the drop_staging.py script:
```
python drop_staging.py
```

## Credits
- Data courtesy of NBA.com
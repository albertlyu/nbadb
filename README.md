# nbadb

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

To load staging tables:
```
python load_staging.py 2014-10-28 2014-11-01 # start_date to end_date
```

To drop all staging tables, simply run the drop_staging.py script:
```
python drop_staging.py
```

## Credits
- Data courtesy of NBA.com
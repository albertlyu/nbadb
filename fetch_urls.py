#! /usr/bin/python
import urllib

NBA_BASE_URL = "http://stats.nba.com/stats/"

def validate_url(url):
  """
  Validate URL and return True except HTTPError or URLError.
  """
  try:
    urllib.urlopen(url)
  except urllib.error.HTTPError as e:
    print("Error code:", e.code)
    print(url)
  except urllib.error.URLError as e:
    print("We failed to reach a server. Reason: ", e.reason)
    print(url)
  else:
    return True

def fetch_scoreboard_urls(dates):
  """
  Fetch scoreboard URLs by gameDate given a list of datetime objects.
  """
  urls = []
  scoreboard_params = "scoreboardV2?DayOffset=0&LeagueID=00&gameDate="
  for date in dates:
    YYYY = str(date.year)
    MM = str(date.month)
    DD = str(date.day)
    scoreboard_url = NBA_BASE_URL + scoreboard_params + "%2F".join((MM,DD,YYYY)) 
    urls.append(scoreboard_url)
  return(urls)

def fetch_urls(ids,resource,params):
  """
  Fetch resource URLs given identifier, resource name, and parameters.
  """
  urls = []
  for id in ids:
    url = NBA_BASE_URL + resource + params + str(id)
    urls.append(url)
  return(urls)
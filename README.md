# GCEventScheduler
Google Calendar Event Scheduler

Built using python 3.6.5

## Installation requirements
use requirements.txt

`pip install -r requirements.txt`

## You need stuff from Google api
Follow python quickstart
https://developers.google.com/calendar/quickstart/python
but download the 'credentials' file as client_secret.json

Then you need to activate the calendar API.

Here are instructions:

https://developers.google.com/calendar/auth



## Running instruction

`python eventsheduler.py`

it is hardcoded to read from events.csv

Column headings aer:

Project Name,Event name,Earliest Date,Latest Date,Hours,Predecessor Event,Gap to predecessor Event (days),Attendees

###############################################################################################33
# Google calendar event scheduler
# v0.2
#  + variable names refactored
#  + added github repo: 
# v0.1
#


#from __future__ import print_function
#import apiclient.discovery
#import apiclient.service
#from apiclient.service import service

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import datetime, json, csv
import rfc3339
import iso8601
from pytz import timezone

###############################################################################################33
# Reading data from CSV file
#
settings = json.load(open('settings.json'))
events = []

with open(settings['csv'], newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        events.append(row)

def findNextEvent(event):
    ret = None
    for e in events:
        if (e[settings['PROJECT_NAME']] == event[settings['PROJECT_NAME']]) and (e[settings['PREV_EVENT']] == event[settings['EVENT_NAME']]):
            ret = e
    return ret


###############################################################################################33
# Build ordered event list
#
scheduled_events = {}
time_max = "00000000"
time_min = "99999999"
for event in events:
    current_project = event[settings['PROJECT_NAME']]
    if current_project in scheduled_events:
        continue
    else:
        first_event = None
        for event_tmp in events:
            if (event_tmp[settings['START_DATE']] < time_min):
                time_min = event_tmp[settings['START_DATE']]
            if (event_tmp[settings['END_DATE']] > time_min):
                time_max = event_tmp[settings['END_DATE']]
            if (event_tmp[settings['PROJECT_NAME']] == current_project) and (event_tmp[settings['PREV_EVENT']] == ""):
                first_event = event_tmp
        if first_event == None:
            print("Error! Initial event not found!!!")
        else:
            scheduled_events[current_project] = [first_event]
            next_event = findNextEvent(first_event)
            while next_event != None:
                scheduled_events[current_project].append(next_event)
                next_event = findNextEvent(next_event)

###############################################################################################33
# Setup the Calendar API
#

SCOPES = 'https://www.googleapis.com/auth/calendar'
store = file.Storage('saved_token.json')
creds = store.get()
if not creds or creds.invalid:
    # client_secret comes from Google developer console
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('calendar', 'v3', http=creds.authorize(Http()))

global_min_date = datetime.datetime(int(time_min[0:4]), int(time_min[4:6]), int(time_min[6:8]))
global_max_date = datetime.datetime(int(time_max[0:4]), int(time_max[4:6]), int(time_max[6:8]))

print("Get calendars:")
page_token = None
while True:
    calendar_list = service.calendarList().list(pageToken=page_token).execute()
    for calendar in calendar_list['items']:
        print("id: {} summary: {} kind: {} etag: {}".format(calendar['id'],calendar['summary'],calendar['kind'],calendar['etag']))
    page_token = calendar_list.get('nextPageToken')
    if not page_token:
        break


events_result = service.events().list(calendarId=settings["calendar_id"],
                                      timeMin=global_min_date.isoformat() + 'Z', timeMax=global_max_date.isoformat() + 'Z',
                                      maxResults=100, singleEvents=True,
                                      orderBy='startTime').execute()
calendar_events = events_result.get('items', [])

time_zone = events_result['timeZone']
print("TIMEZONE: {}".format(time_zone))

###############################################################################################33
# Main loop
#
last_event_date = None

for project in scheduled_events:
    print("Start handling project:{}".format(project))
    for event in scheduled_events[project]:
        print("===\nevent:{}\nglobal min: {} max: {}===\n".format(event, global_min_date, global_max_date))

# update event list to include newly added events
        events_result = service.events().list(calendarId=settings["calendar_id"],
                                              timeMin=global_min_date.isoformat() + 'Z', timeMax=global_max_date.isoformat() + 'Z',
                                              maxResults=100, singleEvents=True,
                                              orderBy='startTime').execute()
        calendar_events = events_result.get('items', [])

        event_earliest_day = event[settings['START_DATE']]
        event_latest_day = event[settings['END_DATE']]
        event_gap = int(event[settings['GAP']]) if event[settings['GAP']] != "" else 0
        print("event gap: {}".format(event[settings['GAP']]))

        event_earliest_date = datetime.datetime(int(event_earliest_day[0:4]), int(event_earliest_day[4:6]), int(event_earliest_day[6:8]))
        event_latest_date = datetime.datetime(int(event_latest_day[0:4]), int(event_latest_day[4:6]), int(event_latest_day[6:8]))

###############################################################################################33
# Loop inside event Start/End dates
#
        new_event_reached = False
        while event_earliest_date < event_latest_date and not new_event_reached:
            if event_earliest_date < global_min_date:
                event_earliest_date = global_min_date
            if last_event_date != None and event_gap > 0:
                print("Gap detected! Time shifted from: {}".format(event_earliest_date))
                event_earliest_date = last_event_date + datetime.timedelta(days=event_gap)
                print("To: {}".format(event_earliest_date))
            week_day = str(event_earliest_date.weekday())
            print("event earliest date: {} latest date: {}".format(event_earliest_date, event_latest_date))
            print("week day:{}\nsettings:{}".format(week_day, settings['weekly_schedule']))
            if week_day in settings['weekly_schedule'] and len(settings['weekly_schedule'][week_day]) > 0:
                m = settings['weekly_schedule'][week_day]['start'].split(":")
                m = int(m[0]) * 60 + int(m[1])
                print("Minutes start: {}".format(m))
                event_min_date = event_earliest_date + datetime.timedelta(minutes=m)
                event_start_date = event_min_date
                event_end_date = event_start_date + datetime.timedelta(hours=int(event[settings['HOURS']]))
                m = settings['weekly_schedule'][week_day]['end'].split(":")
                m = int(m[0]) * 60 + int(m[1])
                print("Minutes end: {}".format(m))
                event_max_date = event_earliest_date + datetime.timedelta(minutes=m)

                print("event dates => min: {} start: {} end: {} max: {}".format(event_min_date, event_start_date, event_end_date, event_max_date))
                print("start checking calendar events...\n")

                while event_end_date < event_max_date:
                    all_events_old = True
                    for calendar_event in calendar_events:
#                        print("EVENT: {}".format(calendar_event))
                        if ('dateTime' not in calendar_event['start']):
                            continue
                        calendar_event_start_date = iso8601.parse_date(calendar_event['start']['dateTime']).replace(tzinfo=None)
                        calendar_event_end_date = iso8601.parse_date(calendar_event['end']['dateTime']).replace(tzinfo=None)  
                        print("{} => start: {} end: {}\n---".format(calendar_event['summary'], calendar_event_start_date, calendar_event_end_date))
#                        if calendar_event_end_date < event_earliest_date:
                        if calendar_event_end_date < event_min_date:
                            print("old event {} < {}".format(calendar_event_end_date, event_earliest_date))
                            if all_events_old and calendar_event != calendar_events[-1]:
                                continue
                            else:
                                print("all events was OLD!!!")
                        else:
                            all_events_old = False
                        if (event_min_date <= event_start_date < event_max_date) and (event_min_date < event_end_date <= event_max_date):
#                            if (event_start_date < calendar_event_start_date and event_end_date <= calendar_event_start_date) or (event_start_date >= calendar_event_end_date and event_end_date >= calendar_event_end_date):
                            if (event_end_date <= calendar_event_start_date) or (event_start_date >= calendar_event_end_date):
                                new_event = {
                                  'summary': "{}-{}".format(event[settings['PROJECT_NAME']], event[settings['EVENT_NAME']]),
                                  'start': {
                                    'dateTime': rfc3339.rfc3339(timezone(time_zone).localize(event_start_date)),
                                  },
                                  'end': {
                                    'dateTime': rfc3339.rfc3339(timezone(time_zone).localize(event_end_date)),
                                  },
                                }
                                print("New Event generation: {} => s: {} e: {}".format(new_event["summary"], event_start_date, event_end_date))
                                new_event = service.events().insert(calendarId=settings["calendar_id"], body=new_event).execute()
                                print('Event created: %s' % (new_event.get('htmlLink')))
#                                event_start_date = event_end_date

# shift global min date to latest event
#                                global_min_date = last_event_date
#                                global_min_date = event_end_date
# exit from event loop
                                event_earliest_date = event_end_date
# exit from day loop
                                event_end_date = event_max_date
                                new_event_reached = True
                                last_event_date = event_start_date.replace(hour=0, minute=0)
                                global_min_date = last_event_date
                                print("event dates => min: {} start: {} end: {} max: {}".format(event_min_date, event_start_date, event_end_date, event_max_date))
                                break
#                            continue
                            else:
                                print("scheduled event start time overlaps with existing event")
                                if calendar_event_start_date.day > event_start_date.day:
                                    event_start_date = event_max_date
                                    print("calendar_event_start_date.day > event_start_date.day: jump to next day")
                                    break
                                event_start_date = calendar_event_end_date
                                event_end_date = event_start_date + datetime.timedelta(hours=int(event[settings['HOURS']]))
                                if event_end_date > event_max_date:
                                    event_start_date = event_max_date
                                    global_min_date = global_min_date + datetime.timedelta(days=1)
                                    print("no more space to fit scheduled event wihin currend day: jump to next day: {}".format(global_min_date))
                                    break
                        else:
                            print("current dates out of day range! {0} <= {2} < {1} || {0} < {3} <= {1}".format(
                              event_min_date,event_max_date,event_start_date,event_end_date
                            ))
            else:
                print("Non working day!!! Skipped")
#            global_min_date = global_min_date + datetime.timedelta(days=1)
            event_earliest_date = event_earliest_date + datetime.timedelta(days=1)
            
        if not calendar_events:
            print('No upcoming events found.')

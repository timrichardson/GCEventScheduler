###############################################################################################33
# Google calendar event scheduler
# v0.2
#  + variable names refactored
#  + added github repo: 
# v0.1
#

# this was not written by me.

#import apiclient.discovery
#import apiclient.service
#from apiclient.service import service

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import datetime, json, csv
import rfc3339
import iso8601
from pytz import timezone,UTC
import pytest
import logging
from collections import OrderedDict
import networkx as nx
from dateutil import parser
from sortedcontainers import SortedList,SortedKeyList

from typing import List,Optional,Dict,Tuple,NamedTuple,NewType,Iterable

GoogleEvent = NewType('GoogleEvent',OrderedDict)  #alias, promote to new type after refactoring
ProjectName = str
RequestedEvent = NewType('RequestedEvent',OrderedDict)


class CalendarGap(NamedTuple):
    gap_start_datetime: datetime.datetime
    gap_end_datetime: datetime.datetime
    gap_duration: datetime.timedelta

class GapRequest(NamedTuple):
    minimum_start_date: datetime.datetime
    maximum_end_date: datetime.datetime
    gap_duration_minutes: int #minutes


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



###############################################################################################33
# Reading data from CSV file
#

global_min_date = datetime.datetime(2018,1,1)
global_max_date = datetime.datetime(2018,12,31)


def findNextEvent(settings,event:OrderedDict,events:List[OrderedDict])->Optional[OrderedDict]:
    """ events is a list of events in order. Return the event after e from the same project, or None"""
    ret = None
    for e in events:
        if (e[settings['PROJECT_NAME']] == event[settings['PROJECT_NAME']]) and (e[settings['PREV_EVENT']] == event[settings['EVENT_NAME']]):
            ret = e
    return ret


###############################################################################################33
# Build ordered event list
#
def build_ordered_events_per_project(settings:dict, events:List[RequestedEvent])->Dict[ProjectName, List[RequestedEvent]]:
    """ return a dict keyed by project name, and containing a list of events in chronological order"""


    # scheduled events is a dict keyed by project. Each value is a list of events, but in tuple of (index,event)
    # the index is an easy way to refer to the event; the index is stored in a graph
    # we assume the event names are unique
    requested_events_by_project = {} #type: Dict[ProjectName,List[Tuple[int,RequestedEvent]]]
    ordered_events = {} #type: Dict[ProjectName,List[RequestedEvent]]
    indexed_scheduled_events = {} #type: Dict[int,RequestedEvent]
    current_project_column_name = settings['PROJECT_NAME']
    predecessor_event_column = settings['PREV_EVENT']
    event_name_column = settings['EVENT_NAME']
    project_roots = {}  #this records the index of the root event for the project, although we don't use it at the moment
    unique_event_name_check = {}

    # pass one, split the events into their projects, and index them for easy reference later
    for index,event in enumerate(events):
        event_project = event[current_project_column_name]  #type: ProjectName
        if event_project not in requested_events_by_project:
            requested_events_by_project[event_project] = []
            unique_event_name_check[event_project] = set()
        #rewrite event name

        if len(event[predecessor_event_column]) == 0:
            event[predecessor_event_column] = f"__root__"
            if event_project in project_roots:
                raise RuntimeError(f"Project {event_project} has more than one root event (that is, an even with no predecessor)")
            else:
                project_roots[event_project] = index
        #check that event names are unique

        this_event_name = event[event_name_column]
        if this_event_name in unique_event_name_check[event_project]:
            raise RuntimeError(f"Event {this_event_name} is duplicated in project: {event_project}")
        else:
            unique_event_name_check[event_project].add(this_event_name)

        requested_events_by_project[event_project].append([index,event])
        indexed_scheduled_events[index] = event

    #pass two: per project, add the events to a directed graph
    # and then traverse it

    for project in requested_events_by_project:
        directed_graph = nx.DiGraph()
        for (index,event) in requested_events_by_project[project]:
            #adding edges will add a node if necessary
            this_event_name = event[event_name_column]  #uniqify event name
            predecessor_event_name = event[predecessor_event_column]
            directed_graph.add_edge(predecessor_event_name,this_event_name)
            directed_graph.nodes[this_event_name]['index'] = index

        print (nx.dfs_successors(directed_graph))
        #now, build a list of events in the right order
        ordered_events[project] = []
        ordered_indexes = []
        for parent_node,successor_nodes in nx.dfs_successors(directed_graph).items():
            #traversal data is a list of nodes
            for node_name in successor_nodes:
                index = directed_graph.nodes[node_name]['index']
                if not index in ordered_indexes:
                    ordered_indexes.append(index)
                    ordered_events[project].append(indexed_scheduled_events[index])

    return ordered_events

###############################################################################################33
# Setup the Calendar API
#
def setup_calendar_API(settings:dict):
    global time_zone
    SCOPES = 'https://www.googleapis.com/auth/calendar'
    store = file.Storage('saved_token.json')
    creds = store.get()
    if not creds or creds.invalid:
        # client_secret comes from Google developer console
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('calendar', 'v3', http=creds.authorize(Http()))



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
    return service

###############################################################################################33
# Main loop
#

def read_existing_events(settings,calendar_service)->List:
    events_result = calendar_service.events().list(calendarId=settings["calendar_id"],
                                                   timeMin=global_min_date.isoformat() + 'Z',
                                                   timeMax=global_max_date.isoformat() + 'Z',
                                                   maxResults=500, singleEvents=True,
                                                   orderBy='startTime').execute()
    existing_calendar_events = events_result.get('items', [])

    return []





def build_calendar_gaps(existing_calendar_events:List[GoogleEvent])->List[CalendarGap]:
    """ Make list of gaps in google events. May need to add a gap to extend from the last gap in the list to the end of the time under consideration"""
    """ eliminate gaps which are on weekends or outside working hours, which could mean altering start date and end dates."""
    list_of_gaps = []
    events_with_start_times = [e for e in existing_calendar_events if 'dateTime' in e['start']]
    sorted_existing_events = sorted(events_with_start_times, key=lambda ev: parser.parse(ev['start']['dateTime']))
    #need to check what to do about all-day busy events
    nbr_events = len(sorted_existing_events)
    list_of_gaps = [] #type: List[CalendarGap]
    for counter,existing_calendar_event in enumerate(sorted_existing_events):
        if counter != nbr_events - 1: #there will be no further gap when we get to the last event
            next_event = sorted_existing_events[counter+1]
            this_event_end_datetime = parser.parse(existing_calendar_event['end']['dateTime']) #type: datetime.datetime
            next_event_start_datetime = parser.parse(next_event['start']['dateTime']) #type: datetime.datetime
            if this_event_end_datetime < next_event_start_datetime:
                new_gap = CalendarGap(gap_start_datetime=this_event_end_datetime,
                                      gap_end_datetime=next_event_start_datetime,
                                      gap_duration=next_event_start_datetime - this_event_end_datetime) # we have a gap
                list_of_gaps.append(new_gap)

    return list_of_gaps


def find_candidate_gaps(gaps:List[CalendarGap], gap_request:GapRequest)->List[CalendarGap]:
    """
    gap_request ={'minimum_start...}
    Returns a subset of the original gaps, but not copies.

    one list, it is sorted by start date.

    Find all gaps with a start date is is after the required minimum start date, call this sublist 1. O(log2(n))
    now sort sublist1 by gap end date O(N log N)
    search the second list on the end date criteria: sublist2
    now sort sublist2 by gap duration: sublist 3
    search sublist3 to eliminate gaps which aren't big enough, and sort by start date into sublist4.
    Now take the first 1.


    """
    def key_fun_startdate(cg:CalendarGap):
        return cg.gap_start_datetime


    def key_fun_enddate(cg: CalendarGap):
        return cg.gap_end_datetime


    gaps_sorted_by_start = SortedKeyList(key = key_fun_startdate)  #type: SortedKeyList[CalendarGap]
    gaps_sorted_by_start.update(gaps)
    index_start_date = gaps_sorted_by_start.bisect_key_left(gap_request.minimum_start_date)
    remaining_gaps_sorted_by_end = SortedKeyList(key=key_fun_enddate) #type: SortedKeyList[CalendarGap]
    remaining_gaps_sorted_by_end.update(gaps[index_start_date:])
    index_end_date = remaining_gaps_sorted_by_end.bisect_key_left(gap_request.maximum_end_date)

    gaps_of_sufficient_duration = [g for g in remaining_gaps_sorted_by_end[:index_end_date] if g.gap_duration.seconds / 60 >= gap_request.gap_duration_minutes]
    return gaps_of_sufficient_duration


def update_gap(list_of_gaps:List[CalendarGap], original_gap:CalendarGap, new_startdatetime:datetime.datetime, new_enddatetime:datetime.datetime):
    # whoops, Named tuples can't be modified!
    """ modifies the collection of CalendarGaps in place. Use if after part of the original_gap is used for an event"""
    revised_gap = CalendarGap(gap_start_datetime = new_startdatetime,
                              gap_end_datetime=new_enddatetime,
                              gap_duration=(new_enddatetime - new_startdatetime)
                              )
    list_of_gaps.remove(original_gap)
    list_of_gaps.append(revised_gap)



def read_google_calendar_events(calendar_service,settings:dict)->List[GoogleEvent]:
    events_result = calendar_service.events().list(calendarId=settings["calendar_id"],
                                                   timeMin=global_min_date.isoformat() + 'Z',
                                                   timeMax=global_max_date.isoformat() + 'Z',
                                                   maxResults=500, singleEvents=True,
                                                   orderBy='startTime').execute()
    existing_calendar_events = events_result.get('items', []) #type: List[GoogleEvent]
    return existing_calendar_events


def add_events_to_calendar_v2(events_to_schedule:Dict[ProjectName, List[RequestedEvent]],calender_service,gaps:List[CalendarGap],settings:dict):
    # events are scheduled as early as possible. This should eliminate dependency loops I hope
    # gaps is modified
    projects_found = events_to_schedule.keys()
    localtimezone = timezone(settings['TIMEZONE'])
    for project,events in events_to_schedule.items():
        events_by_name = {event['Event name']:event for event in events}
        for event in events:
            earliest_datetime = localtimezone.localize(parser.parse(event['Earliest Date']))
            predecessor_event_name = event.get('Predecessor Event')
            if predecessor_event_name:
                predecessor_finish = events_by_name.get(predecessor_event_name,{}).get('end_datetime')
                if predecessor_finish:
                    try:
                        gap_in_days = int(event['Gap to predecessor Event (days)'])
                    except ValueError:
                        gap_in_days = 0
                    earliest_datetime_predecessor_rule = predecessor_finish + datetime.timedelta(days=gap_in_days)
                    earliest_datetime = max(earliest_datetime,earliest_datetime_predecessor_rule)
            latest_datetime = localtimezone.localize(parser.parse(event['Latest Date']))
            event_duration_minutes = int(float(event['Hours']) * 60)

            candidate_gaps = find_candidate_gaps(gaps=gaps, gap_request=GapRequest(
                minimum_start_date=earliest_datetime,
                maximum_end_date=latest_datetime,
                gap_duration_minutes=event_duration_minutes))
            if len(candidate_gaps) > 0:
                chosen_gap=candidate_gaps[0]
                events_by_name[event['Event name']]['end_datetime'] = chosen_gap.gap_end_datetime
                #add event to calendar
                insert_success = insert_event_to_google_calendar(calendar_service=calender_service, event_name=event['Event name'],
                                                                 event_start_datetime=chosen_gap.gap_start_datetime,
                                                                 event_duration_minutes=event_duration_minutes)
                if insert_success:
                   update_gap(list_of_gaps=gaps,
                              original_gap=chosen_gap,new_startdatetime=chosen_gap.gap_start_datetime + datetime.timedelta(minutes=event_duration_minutes),
                              new_enddatetime=chosen_gap.gap_end_datetime)

                print(f"Added to calendar: Event: {event} using gap: {chosen_gap}")
            else:
                print(f"No gap available for event: {event}")
                break



def insert_event_to_google_calendar(calendar_service,event_name:str,event_start_datetime, event_duration_minutes)->bool:
    return True




def add_events_to_calendar(settings:dict,calendar_service,scheduled_events:dict):
    global global_min_date,global_max_date  #global is only needed if values are changing
    last_event_date = None
    event_gap = 0
    event_earliest_date = None
    event_latest_date = None
    events_result = calendar_service.events().list(calendarId=settings["calendar_id"],
                                                   timeMin=global_min_date.isoformat() + 'Z',
                                                   timeMax=global_max_date.isoformat() + 'Z',
                                                   maxResults=500, singleEvents=True,
                                                   orderBy='startTime').execute()
    existing_calendar_events = events_result.get('items', [])
    for project in scheduled_events:
        print("Start handling project:{}".format(project))
        for event in scheduled_events[project]:
            print("===\nevent:{}\nglobal min: {} max: {}===\n".format(event, global_min_date, global_max_date))

    # update event list to include newly added events

            event_earliest_day = event[settings['START_DATE']] #this is modified as needed in the loop due to gap settings
            event_latest_day = event[settings['END_DATE']]
            event_gap = int(event[settings['GAP']]) if event[settings['GAP']] != "" else 0
            print("event gap: {}".format(event[settings['GAP']]))

            event_earliest_date = datetime.datetime(int(event_earliest_day[0:4]), int(event_earliest_day[4:6]), int(event_earliest_day[6:8]))
            event_latest_date = datetime.datetime(int(event_latest_day[0:4]), int(event_latest_day[4:6]), int(event_latest_day[6:8]))

            new_event_reached = False
            while event_earliest_date < event_latest_date and not new_event_reached:
                event_earliest_date = max(event_earliest_date,global_min_date)
                if last_event_date and event_gap > 0:
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
                        #we have to place this event. Iterate over existing events to find room
                        for existing_calendar_event in existing_calendar_events:
                            #igno
    #                        print("EVENT: {}".format(calendar_event))
                            if ('dateTime' not in existing_calendar_event['start']):
                                continue

                            existing_calendar_event_start_date = iso8601.parse_date(existing_calendar_event['start']['dateTime']).replace(tzinfo=None)
                            existing_calendar_event_end_date = iso8601.parse_date(existing_calendar_event['end']['dateTime']).replace(tzinfo=None)
                            logger.debug("{} => start: {} end: {}\n---".format(existing_calendar_event['summary'], existing_calendar_event_start_date, existing_calendar_event_end_date))

    #                        if calendar_event_end_date < event_earliest_date:
                            if existing_calendar_event_end_date < event_min_date:
                                logger.debug("old event {} < {}".format(existing_calendar_event_end_date, event_earliest_date)) #an irrelevant event
                                #
                                # if all_events_old and existing_calendar_event != existing_calendar_events[-1]:
                                #     continue
                                # else:
                                #     logger.debug("all events was OLD!!!")
                            else:
                                all_events_old = False
                            if (event_min_date <= event_start_date < event_max_date) and (event_min_date < event_end_date <= event_max_date):
    #                            if (event_start_date < calendar_event_start_date and event_end_date <= calendar_event_start_date) or (event_start_date >= calendar_event_end_date and event_end_date >= calendar_event_end_date):
                                if (event_end_date <= existing_calendar_event_start_date) or (event_start_date >= existing_calendar_event_end_date):
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
                                    new_event = calendar_service.events().insert(calendarId=settings["calendar_id"], body=new_event).execute()
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
                                    if existing_calendar_event_start_date.day > event_start_date.day:
                                        event_start_date = event_max_date
                                        print("calendar_event_start_date.day > event_start_date.day: jump to next day")
                                        break
                                    event_start_date = existing_calendar_event_end_date
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

                    # if we end the loop if may be that we couldn't create the event
                else:
                    print("Non working day!!! Skipped")
    #            global_min_date = global_min_date + datetime.timedelta(days=1)
                event_earliest_date = event_earliest_date + datetime.timedelta(days=1)

        if not existing_calendar_events:
            print('No upcoming events found.')


def read_settings()->dict:
    """ settings is a global which I don't like"""

    settings = json.load(open('settings.json'))
    settings['global_min_date'] = datetime.datetime(2018, 1, 1).astimezone(UTC)
    settings['global_max_date'] = datetime.datetime(2019, 1, 1).astimezone(UTC)
    settings['time_zone'] = None

    return settings


def read_events(settings:dict)->List[RequestedEvent]:
    events = []
    with open(settings['csv'], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            events.append(row)
    return events


def main():
    settings = json.load(open('settings.json'))
    events = [] #type: List[RequestedEvent]
    with open(settings['csv'], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            events.append(row)

    ordered_events_per_project = build_ordered_events_per_project(settings=settings, events=events)
    service=setup_calendar_API(settings=settings)
    add_events_to_calendar(settings=settings,calendar_service=service,scheduled_events=ordered_events_per_project)

if __name__ == '__main__':
    main()


def test_read_settings():
    global settings, events
    settings = json.load(open('settings.json'))
    assert settings['csv'] == 'events.csv'


def test_read_events():
    settings = read_settings()
    events = read_events(settings=settings)
    assert len(events) > 0


def test_scheduled_events():
    settings = read_settings()
    events = read_events(settings=settings)
    ordered_scheduled_events = build_ordered_events_per_project(settings=settings, events=events)
    assert ordered_scheduled_events


def test_build_gaps():
    settings = read_settings()
    calender_service = setup_calendar_API(settings=settings)
    google_events = read_google_calendar_events(calendar_service=calender_service)
    gaps = build_calendar_gaps(existing_calendar_events=google_events)
    assert gaps


def test_find_gap():
    settings = read_settings()
    calender_service = setup_calendar_API(settings=settings)
    google_events = read_google_calendar_events(calendar_service=calender_service)
    gaps = build_calendar_gaps(existing_calendar_events=google_events)
    events = read_events(settings=settings)
    ordered_scheduled_events = build_ordered_events_per_project(settings=settings, events=events)
    candidate_gaps = find_candidate_gaps(gaps=gaps, gap_request=GapRequest(minimum_start_date=datetime.datetime(2018, 8, 1).astimezone(UTC),
                                                          maximum_end_date=datetime.datetime(2018,8,31).astimezone(UTC),
                                                          gap_duration_minutes=30))

    print (candidate_gaps)


def test_fill_calendar():
    settings = read_settings()
    calender_service = setup_calendar_API(settings=settings)
    google_events = read_google_calendar_events(calendar_service=calender_service,settings=settings)
    gaps = build_calendar_gaps(existing_calendar_events=google_events)
    events = read_events(settings=settings)
    ordered_scheduled_events = build_ordered_events_per_project(settings=settings, events=events)
    add_events_to_calendar_v2(events_to_schedule = ordered_scheduled_events,
        calender_service=calender_service, gaps=gaps,settings=settings)












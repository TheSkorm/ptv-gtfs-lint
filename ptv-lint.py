#designed for python 2.7

# pip install patool pyunpack

import requests
from pyunpack import Archive
import zipfile
import csv
import StringIO

SOURCE="http://data.ptv.vic.gov.au/downloads/gtfs.zip"
PATH="gtfs.zip"
UPDATE_DATA=False
# 4:"MetroBus",8:"NightRider"
TRUSTED_DATA={2:"MetroTrain",3:"MetroTram"}

if UPDATE_DATA:
    r = requests.get(SOURCE, stream=True)
    if r.status_code == 200:
        with open(PATH, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
Archive(PATH).extractall('temp/')

routeNames={}
routeCompress={}
writableStops=[]
stopCounter={}
stoplatln={}
stopCompress={}
def merge_data(header,file_name):
    data = []
    skippable_trips = list(set(stopCounter.keys()) - set(writableStops))
    for type in TRUSTED_DATA:
        Archive('temp/' + str(type) + "/google_transit.zip").extractall('extracted/'+TRUSTED_DATA[type])
        with open('extracted/'+TRUSTED_DATA[type]+'/'+file_name) as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                output = StringIO.StringIO()
                writer = csv.writer(output)
                if file_name == "stop_times.txt": #this is to do the stop id replacement - see below
                    if(row[3] in stopCompress):
                        row[3] = stopCompress[row[3]]
                if TRUSTED_DATA[type] == "MetroTrain" and file_name == "routes.txt":
                    if(row[3] not in routeNames): #this is to merge route names
                        routeNames[row[3]]=row[0]
                        row[2]=""
                        writer.writerow(row)
                        data.append(output.getvalue().rstrip('\r\n'))
                    else:
                        routeCompress[row[0]]=routeNames[row[3]]
                elif file_name == "stops.txt":
                    latlng=row[2] + ":" + row[3]
                    if latlng not in stoplatln: #this is to remove stops that have the same latlng as other stops
                        writer.writerow(row)
                        data.append(output.getvalue().rstrip('\r\n'))
                        stoplatln[latlng] = row[0]
                    else:
                        stopCompress[row[0]] = stoplatln[latlng]
                elif TRUSTED_DATA[type] == "MetroTrain" and file_name == "trips.txt":
                    if row[2] not in skippable_trips: #if trips have no stops we don't write them down
                        if(row[0] in routeCompress):
                            row[0] = routeCompress[row[0]]
                        writer.writerow(row)
                        data.append(output.getvalue().rstrip('\r\n'))
                elif TRUSTED_DATA[type] == "MetroTrain" and file_name == "stop_times.txt":
                    # Caulfield Railway Station hack
                    if( row[3] == "19943" and not("PKM" in row[0] or "CRB" in row[0] or "FKN" in row[0])):
                    #    print "Skipping this stop because it looks like it's invalid " + ",".join(row)
                        pass
                    #only write trips that stop more than once
                    elif (row[0] in stopCounter):
                        writer.writerow(row)
                        data.append(output.getvalue().rstrip('\r\n'))
                        data.append(stopCounter[row[0]])
                        writableStops.append(row[0])
                    elif (row[0] in writableStops):
                        writer.writerow(row)
                        data.append(output.getvalue().rstrip('\r\n'))
                    else:
                        writer.writerow(row)
                        stopCounter[row[0]]=output.getvalue().rstrip('\r\n')
                else:
                    writer.writerow(row)
                    data.append(output.getvalue().rstrip('\r\n'))
    data=sorted(set(data))
    #readd header
    data.insert(0,header)

    f = open('merged/'+file_name,'wb')
    for line in data:
        f.write("%s\n" % line)
    f.close()

merge_data("service_id,date,exception_type", "calendar_dates.txt")
merge_data("service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date", "calendar.txt")
merge_data("route_id,agency_id,route_short_name,route_long_name,route_type","routes.txt")
merge_data("shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled","shapes.txt")
#stops need to be before trips to compress stops with same latlng
merge_data("stop_id,stop_name,stop_lat,stop_lon","stops.txt")
merge_data("trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled","stop_times.txt")
#trips need to be done last for the mapping
merge_data("route_id,service_id,trip_id,shape_id,trip_headsign,direction_id","trips.txt")

zf = zipfile.ZipFile('output/gtfs.zip', mode='w')
zf.write('merged/agency.txt','agency.txt')
zf.write('merged/calendar.txt','calendar.txt')
zf.write('merged/calendar_dates.txt','calendar_dates.txt')
zf.write('merged/routes.txt','routes.txt')
zf.write('merged/shapes.txt','shapes.txt')
zf.write('merged/stop_times.txt','stop_times.txt')
zf.write('merged/stops.txt','stops.txt')
zf.write('merged/trips.txt','trips.txt')
zf.close()

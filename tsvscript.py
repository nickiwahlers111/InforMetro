#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []

with open('outfile.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:len(data)-1]:
        catch = line.strip('\n')
        row = catch.split("\t") 
        
        # skip the rows where speed is missing
        # print(row)
        x = row[0]
        y = row[1]
        speed = row[2]
        if speed is None or speed == "":
            continue
     
        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(float(speed)))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)
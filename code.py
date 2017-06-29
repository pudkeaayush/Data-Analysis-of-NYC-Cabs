#RAJU KHANAL 110849511
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
import operator
from geopy.geocoders import Nominatim
import csv
import reverse_geocoder as rg
import re

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Word Count") \
                    .getOrCreate()

def getlocation(x):
    point_coord = (float(x[1]) , float(x[0]))
    this_location = rg.search(point_coord, mode = 1)
    print(this_location)
    if ( this_location[0]['admin2'] == 'Queens County'):
        return 'Queens'
    else :
        return this_location[0]['name']

def getlocation2(x):
    point_coord_pickup = (float(x[1]) , float(x[0]))
    point_coord_drop = (float(x[3]) , float(x[2]))
    pickup_location = rg.search(point_coord_pickup, mode = 1)
    drop_location = rg.search(point_coord_drop , mode = 1)
    if ( pickup_location[0]['admin2'] == 'Queens County'):
        pickup_name = 'Queens'
    else :
        pickup_name = pickup_location[0]['name']
    
    if ( drop_location[0]['admin2'] == 'Queens County'):
        drop_name = 'Queens'
    else :
        drop_name = drop_location[0]['name']
    names = (pickup_name , drop_name)
    return names

def getlocation_drop(x):
    point_coord = (x[3] , x[2])
    this_location = rg.search(point_coord, mode = 1)
    if ( this_location[0]['admin2'] == 'Queens County'):
        return ('Queens',x[0],x[1])
    else :
        return (this_location[0]['name'],x[0],x[1])



if __name__ == "__main__":
    sc = spark.sparkContext
    
    #To get the tip data
    text_file1 = sc.textFile("yellow_tripdata_2016-06.csv")\
                .map( lambda lines: lines.split(","))\
                .map( lambda lines : (lines[4], lines[15]))\
                .filter( lambda lines: not (lines[0] == 'trip_distance' or lines[1] == 'trip_distance'))\
                .map( lambda lines : (float(lines[0]), float(lines[1]))).collect()
     
    file = open("June_2016_Tip","w")
    file.write("Total Distance travelled is : ")
    sum_distance = sum(x[0] for x in text_file1)
    file.write(str(sum_distance))
    file.write("\n")
    sum_tip = sum(x[1] for x in text_file1)
    file.write("Total tip amount is : ")
    file.write(str(sum_tip))
    file.write("\n")
    file.write("Tip amount per mile is : ")
    file.write(str(float(sum_tip / sum_distance))) 
     
    #Get total count for different boroughs
    text_file2 = sc.textFile("yellow_tripdata_2015-12.csv")\
                .map( lambda lines: lines.split(","))\
                .map( lambda lines : (lines[9], lines[10]))\
                .map( lambda lines : (lines[0] , lines[1]))\
                .filter( lambda lines: not lines[0].startswith('drop'))\
                .map( lambda x : getlocation(x))\
                .filter( lambda x : x =='Queens' or x == 'Manhattan' or x == 'Staten Island' or x == 'Brooklyn' or x =='The Bronx')\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a + b).collect()
         
    file = open("Count_total_2015_12","w")
    file.write(str(text_file2))
    
    #Get disputed count for different boroughs   
    text_file3 = sc.textFile("yellow_tripdata_2015-12.csv")\
                .map( lambda lines: lines.split(","))\
                .map( lambda lines : (lines[9], lines[10], lines[11]))\
                .filter( lambda lines: not (lines[2] == 'payment_type'))\
                .filter( lambda lines : (int(lines[2]) == 4) )\
                .map( lambda lines : (lines[0] , lines[1]))\
                .filter( lambda lines: not lines[0].startswith('drop'))\
                .map( lambda x : getlocation(x))\
                .filter( lambda x : x =='Queens' or x == 'Manhattan' or x == 'Staten Island' or x == 'Brooklyn' or x =='The Bronx')\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a + b).collect()
         
    file = open("Count_for_disputed_payments_2015_12.txt","w")
    file.write(str(text_file3))
 
    #Get Frequent Itemsets
    text_file3 = sc.textFile("yellow_tripdata_2015-12.csv")\
                .map( lambda lines: lines.split(","))\
                .filter( lambda lines : not lines[0].startswith('[a-zA-Z]'))\
                .map( lambda lines : (lines[5], lines[6], lines[9] , lines[10] , lines[11]))\
                .filter( lambda lines : not (lines[0] == ('pickup_longitude') or lines[1] == ('pickup_longitude')))\
                .map( lambda x : getlocation(x))\
                .filter( lambda x : (x[0] =='Queens' or x[0] == 'Manhattan' or x[0] == 'Staten Island' or x[0] == 'Brooklyn' or x[0] =='The Bronx')\
                         and (x[1] =='Queens' or x[1] == 'Manhattan' or x[1] == 'Staten Island' or x[1] == 'Brooklyn' or x[1] =='The Bronx'))\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a + b).collect()
    file = open("Frequent_Itemset_2015_12.txt","w")
    file.write(str(text_file3))
    
    text_file4 = sc.textFile("yellow_tripdata_2015-12.csv")\
                .map( lambda lines: lines.split(","))\
                .map( lambda lines : (lines[4], lines[15],lines[9],lines[10]))\
                .filter( lambda lines: not (lines[0] == 'trip_distance' or lines[1] == 'trip_distance'))\
                .map( lambda lines : (float(lines[0]), float(lines[1]),lines[2],lines[3]))\
                .filter( lambda x: x[1] > 150)\
                .map( lambda x : getlocation_drop(x))\
                .filter( lambda x : x[0] =='Queens' or x[0] == 'Manhattan' or x[0] == 'Staten Island' or x[0] == 'Brooklyn' or x[0] =='The Bronx')\
                .map(lambda x : (x[0], (x[1],x[2])))\
                .groupByKey().collect()
    
    print(text_file4)
    my_dict1 = {}
    for i in range(len(text_file4)):
        for j in range(len(text_file4[i][1])):
            sum_tip = sum(float(x[1]) for x in text_file4[i][1])
            sum_distance = sum(float(x[0]) for x in text_file4[i][1])
            my_dict1[text_file4[i][0]] = sum_tip / sum_distance
    print("My dict is", my_dict1.items())
    file = open("raju.txt", "w")
    file.write(str(my_dict1.items()))
    

    

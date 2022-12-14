#Lucy Wang (lcw2nkz)
#Anna Dandridge (amd6wqk)

import os
import datetime
import pymongo
import pprint
import pandas as pd
import csv
import json
from operator import itemgetter

host_name = "localhost"
port = "27017"

atlas_cluster_name = "sandbox"
atlas_default_dbname = "local"

conn_str = {
    "local" : f"mongodb://{host_name}:{port}/",
#    "atlas" : f"mongodb+srv://{atlas_user_name}:{atlas_password}@{atlas_cluster_name}.zibbf.mongodb.net/{atlas_default_dbname}"
}

client = pymongo.MongoClient(conn_str["local"])
#print(client)

print(f"Local Connection String: {conn_str['local']}")
#print(f"Atlas Connection String: {conn_str['atlas']}")

db_name = "netflix"
db = client[db_name]

df = pd.read_csv('Best Movie by Year Netflix.csv')

for i in range(df.shape[0]):
        netflix_info = {
                        "title": df["TITLE"].to_list()[i],
                        "release_year": df["RELEASE_YEAR"].to_list()[i],
                }
        
        best_movie_by_year = db.best_movie_by_year
        best_movie_by_year_id = best_movie_by_year.insert_one(netflix_info).inserted_id




df2 = pd.read_csv('Best Movies Netflix.csv')

for i in range(df2.shape[0]):
        netflix_info2 = {
                "title": df2["TITLE"].to_list()[i],
                "release_year": df2["RELEASE_YEAR"].to_list()[i],
                "score": df2["SCORE"].to_list()[i],
                "number_of_votes": df2["NUMBER_OF_VOTES"].to_list()[i], 
                "duration": df2["DURATION"].to_list()[i], 
                "main_genre": df2["MAIN_GENRE"].to_list()[i], 
        }

        best_movies_netflix = db.best_movies_netflix
        best_movies_netflix_id = best_movies_netflix.insert_one(netflix_info2).inserted_id
#pprint.pprint(best_movies_netflix.find_one({}))





df3 = pd.read_csv('Best Show by Year Netflix.csv')

for i in range(df3.shape[0]):
        netflix_info3 = {
                "title": df3["TITLE"].to_list()[i],
                "release_year": df3["RELEASE_YEAR"].to_list()[i],
                "main_genre": df3["MAIN_GENRE"].to_list()[i], 
        }

        best_show_by_year = db.best_show_by_year
        best_show_by_year_id = best_show_by_year.insert_one(netflix_info3).inserted_id
#pprint.pprint(best_movies_netflix.find_one({}))




df4 = pd.read_csv('Best Shows Netflix.csv')

for i in range(df4.shape[0]):
        netflix_info4 = {
                "title": df4["TITLE"].to_list()[i],  
                "main_genre": df4["MAIN_GENRE"].to_list()[i], 
        }

        best_shows_netflix = db.best_shows_netflix
        best_shows_netflix_id = best_shows_netflix.insert_one(netflix_info4).inserted_id
        #pprint.pprint(best_movies_netflix.find_one({}))




df6 = pd.read_csv('raw_titles.csv')

for i in range(df6.shape[0]):
        netflix_info6 = {
                "title": df6["title"].to_list()[i],
                "age_certification": df6["age_certification"].to_list()[i],
                "production_countries": df6["production_countries"].to_list()[i],
        }

        raw_titles = db.raw_titles
        raw_titles_id = raw_titles.insert_one(netflix_info6).inserted_id


#pprint.pprint(best_movie_by_year.count_documents( {} ))
#pprint.pprint(raw_titles.count_documents( {} ))

#for m in  best_movie_by_year.find({"score" : 8.2}):
#    pprint.pprint(m)

#pprint.pprint(best_movie_by_year.find_one( {"score" : 8.2} ))

#choices.sort(key=itemgetter('choice'), reverse=True)
#print(db.best_movie_by_year.find().sort("release_year",-1).pretty()) #// for MAX


#for m in best_movie_by_year.find().sort("score"):
#   pprint.pprint(m["score"])







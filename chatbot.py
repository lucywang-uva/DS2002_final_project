#Lucy Wang (lcw2nkz)
#Anna Dandridge (amd6wqk)

import nltk 
import discord
nltk.download('punkt')
from nltk import word_tokenize,sent_tokenize
from nltk.stem.lancaster import LancasterStemmer
stemmer = LancasterStemmer()
#read more on the steamer https://towardsdatascience.com/stemming-lemmatization-what-ba782b7c0bd8
import numpy as np 
import tflearn
import tensorflow as tf
import random
import json
import pickle
import os
import datetime
import pymongo
import pprint
import pandas as pd
import csv
import json





#LINK TO DB
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
















with open("intents.json") as file:
    data = json.load(file)

try:
    with open("data.pickle","rb") as f:
        words, labels, training, output = pickle.load(f)

except:
    words = []
    labels = []
    docs_x = []
    docs_y = []
    for intent in data["intents"]:
        for pattern in intent["patterns"]:
            wrds = nltk.word_tokenize(pattern)
            words.extend(wrds)
            docs_x.append(wrds)
            docs_y.append(intent["tag"])
            
        if intent["tag"] not in labels:
            labels.append(intent["tag"])


    words = [stemmer.stem(w.lower()) for w in words if w != "?"]
    words = sorted(list(set(words)))
    labels = sorted(labels)

    training = []
    output = []
    out_empty = [0 for _ in range(len(labels))]

    for x, doc in enumerate(docs_x):
        bag = []

        wrds = [stemmer.stem(w.lower()) for w in doc]

        for w in words:
            if w in wrds:
               bag.append(1)
            else:
              bag.append(0)
    
        output_row = out_empty[:]
        output_row[labels.index(docs_y[x])] = 1
        
        training.append(bag)
        output.append(output_row)

    training = np.array(training)
    output = np.array(output)
    
    with open("data.pickle","wb") as f:
        pickle.dump((words, labels, training, output), f)



net = tflearn.input_data(shape=[None, len(training[0])])
net = tflearn.fully_connected(net, 8)
net = tflearn.fully_connected(net, 8)
net = tflearn.fully_connected(net, len(output[0]), activation="softmax")
net = tflearn.regression(net)

model = tflearn.DNN(net)
model.fit(training, output, n_epoch=1000, batch_size=8, show_metric=True)
model.save("model.tflearn")

try:
    model.load("model.tflearn")
except:
    model.fit(training, output, n_epoch=1000, batch_size=8, show_metric=True)
    model.save("model.tflearn")


def bag_of_words(s, words):
    bag = [0 for _ in range(len(words))]

    s_words = nltk.word_tokenize(s)
    s_words = [stemmer.stem(word.lower()) for word in s_words]

    for se in s_words:
        for i, w in enumerate(words):
            if w == se:
                bag[i] = 1
    
    return np.array(bag)

def list_questions():
    print("These are the questions you can ask about:")

    print("(1) 2008 most popular movie")
    print("(2) Number of drama shows")
    print("(3) Votes of highest scored movie")
    print("(4) Country of top movie in 2015")
    print("(5) Oldest Netflix TV show")
    print("(6) Highest scored movie")
    print("(7) Most popular movie genre")
    print("(8) Duration of top scored movie?")
    print("(9) Best TV show from 2020")
    print("(10) Least popular movie age certification")

def chat():
    print("Ask a question! (type 'quit' to stop or 'help' for a list of topics)")

    while True:

        inp = input("You: ")

        if inp.lower() == "quit":
            break
            

        result = model.predict([bag_of_words(inp, words)])[0]
        result_index = np.argmax(result)
        tag = labels[result_index]

        if result[result_index] > 0.7:
            for tg in data["intents"]:
                if tag == "question 1":
                    response = db.best_movie_by_year.find_one( {"release_year" : 2008})['title']
                    break

                if tag == "question 2":
                    count_drama = db.best_shows_netflix.aggregate([{"$group" : {'_id':"$main_genre", 'count':{'$sum':1}}}])
                    count_drama_list = list(count_drama)
                    response = [obj for obj in count_drama_list if obj['_id']=='drama'][0]['count']
                    break 

                if tag == "question 3":
                    for m in db.best_movies_netflix.find().sort("score", -1):
                        response = m['number_of_votes']
                        break

                if tag == "question 4":
                    top_movie = db.best_movie_by_year.find_one( {"release_year" : 2015})['title']
                    response = db.raw_titles.find_one( {"title" : top_movie})['production_countries']
                    break

                if tag == "question 5":
                    for m in db.best_show_by_year.find().sort("release_year", 1):
                        response = m['title']
                        break
                
                if tag == "question 6":
                    for m in db.best_movies_netflix.find().sort("score", -1):
                        response = m['title']
                        break

                if tag == "question 7":
                    ans = ""
                    count = 0
                    movie_genre = db.best_movies_netflix.aggregate([{"$group" : {'_id':"$main_genre", 'count':{'$sum':1}}}])
                    movie_genre_list = list(movie_genre)
                    for obj in movie_genre_list:
                        if obj['count'] > count:
                            ans = obj['_id']
                            count = obj['count']
                    response = ans
                    break

                if tag == "question 8":
                    for m in db.best_movies_netflix.find().sort("score", -1):
                        response = m['duration']
                        break

                if tag == "question 9":
                    response = db.best_show_by_year.find_one({"release_year" : 2020})['title']
                    break

                if tag == "question 10":
                    for m in db.best_movies_netflix.find().sort("score", 1):
                        lowest_movie = (m['title'])
                        break
                    response = db.raw_titles.find_one( {"title" : lowest_movie})['age_certification']
                    break

            print(response)
        
        elif inp.lower() == "help":
            list_questions()


        else:
            print("I didnt get that. Please try another question.")
            list_questions()

list_questions()
chat()
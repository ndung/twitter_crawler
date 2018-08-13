from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from datetime import datetime
import os
import thread
import threading
import sys, getopt
import json
import requests
import bson
import sched, time
import pymongo
import tweepy
from pymongo import MongoClient

thread_dict = {}
auth_dict = {}
thread_auth_pair = {}
current_users = ""
current_keywords = ""

## connect to mongodb database ##
client = pymongo.MongoClient('192.168.46.3', 27017)
db = client.nlp_twitter    

class Main:

    def start(self, thread_id, consumer_key, consumer_secret, access_token, access_secret, keywords, users):    
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        
        table = "pilpres2019"

        if table not in db.collection_names():
            db[table].create_index([("id", pymongo.DESCENDING)], unique=True)

        try:
            self.stream_listener = StreamListener(thread_id, table)
            stream = Stream(auth, self.stream_listener)
            if keywords != '' and users != '':
                stream.filter(follow=[users], track=[keywords])
            elif keywords != '':
                stream.filter(track=[keywords])
            elif users != '':
                stream.filter(follow=[users])
            else:
                stream.sample()
        except Exception as e:
            print("Error Occured " + str(e))
            
    def stop(self):
        self.stream_listener.stop()

class StreamingThread (threading.Thread):
    
    def __init__(self, thread_id, consumer_key, consumer_secret, access_token, access_secret, keywords, users):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.keywords = keywords
        self.users = users
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.is_running = True
        thread_dict[self.thread_id] = self

    def run(self):
        print("Starting " + self.thread_id)
        self.main = Main()
        self.main.start(self.thread_id, self.consumer_key, self.consumer_secret, self.access_token, self.access_secret, self.keywords, self.users)        
        self.is_running = True
        
    def stop(self):        
        #del thread_dict[self.thread_id]
        if (self.is_running == True):
            print("Stopping " + self.thread_id)
            self.main.stop()
            self.is_running = False
        
class StreamListener:
    counter = 0
    keep_alive = True

    def __init__(self, thread_id, table):
        self.thread_id = thread_id     
        self.table = db[table]
        self.run = True

    def stop(self):
        self.run = False

    def on_data(self, data):

        try:            
            tweet = json.loads(data)
        except requests.exceptions.ReadTimeout:
            print("ReadTimeout Occured")
            return

        # print(tweet)
        if "text" in tweet:
            try:
                tweet['timestamp_ms'] = bson.Int64(tweet['timestamp_ms'])               
                self.table.insert_one(tweet)                
            except pymongo.errors.DuplicateKeyError:
                pass
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(message)

        return self.run

    def on_connect(self):
        self.counter = 0
        self.start_time = datetime.now()

    def on_error(self, status):
        print("on_error:"+str(status)+"=> on thread:"+self.thread_id)
        return False

    def on_exception(self, status):
        print("on_exception:"+str(status)+"=> on thread:"+self.thread_id)
        return False

    def on_timeout(self):
        print("on_timeout=> on thread:"+self.thread_id)

def main(argv):    
    run()

def getUsers():
    auths = db.config_auth.find_one()
    auth = OAuthHandler(auths['consumer_key'], auths['consumer_secret'])
    auth.set_access_token(auths['access_token'], auths['access_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify =True)

    for u in tweepy.Cursor(api.friends, screen_name="daftarverified").items():
        try:    
            data = "{\"user_id\":\""+u.id_str+"\", \"keyword\":\"@"+u.screen_name+"\", \"name\":\""+u.name+"\", \"active\":\"Y\"}"
            user = json.loads(str(data))
            db['config_stream'].insert_one(user)
        except Exception as ex:
            print("Error Occured " + str(e) +", screen_name:"+u.screen_name)
            pass

def run():
    ## check if all threads are still alive, if not remove the thread from the thread_dict ##
    for k,t in thread_dict.items():
        if (t.is_alive()==False):
            print("thread "+k+" is not alive")
            del thread_dict[k]
            auth_key = thread_auth_pair[k]
            del thread_auth_pair[k]
            current_usage = auth_dict[auth_key]
            auth_dict[auth_key] = current_usage-1
    
    print(str(thread_dict))
    print(str(auth_dict))

    ## get all twitter authentication configurations ##
    auths = db.config_auth
    for auth_config in auths.find({}):
        if (auth_dict.has_key(auth_config['user'])==False):
            auth_dict[auth_config['user']]=0

    users = ""
    keywords = ""

    ## get all streaming configurations ##
    for stream_config in db.config_pilpres.find({}):        
        if (stream_config.has_key('user_id') and (stream_config['active']=="Y")):
            users = users+stream_config['user_id']+","
        elif (stream_config.has_key('keyword') and (stream_config['active']=="Y")):
            keywords = keywords+stream_config['keyword']+","

    if (users.endswith(',')):
        users = users[:-1]

    if (keywords.endswith(',')):
        keywords = keywords[:-1]

    print("users:"+users)
    print("keywords:"+keywords)
    thread_id = "crawler"

    global current_users
    global current_keywords
    ## if the streaming has not been started yet, run a thread to stream ##
    if (thread_dict.has_key(thread_id)==False):
        ## get a twitter authentication with the most minimum usage ##
        auth_key = min(auth_dict.items(), key=lambda x: x[1]) 
        auth = auths.find_one({'user' : auth_key[0]})
        auth_dict[auth_key[0]]=auth_key[1]+1
        t = StreamingThread(thread_id, auth['consumer_key'], auth['consumer_secret'], auth['access_token'], auth['access_secret'], keywords, users)
        t.start()
        thread_auth_pair[thread_id] = auth_key[0]        
        current_users = users
        current_keywords = keywords
    ## stop streaming if the active flag = No ##
    elif (thread_dict.has_key(thread_id)==True and (current_users!=users or current_keywords!=keywords)):
        t = thread_dict[thread_id]
        t.stop()      
    ## run a timer to check all configurations for every minute ##
    threading.Timer(1.0, run).start()

if __name__ == '__main__':    
    main(sys.argv[1:])
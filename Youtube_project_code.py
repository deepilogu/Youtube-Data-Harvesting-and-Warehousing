'''Description: ==>This code will connect with Google Youtube API and extract all the required
                details for One Youtube Channel ID at a time.
                ==>Further, It will store all the collected details of the channel in JSON file
                and move the same to MongoDB. 
                ==>Once stored in MongoDB it will be fetched and store in MySQL in structured format
                ==> It has few set of MySQL queries to analysis data
                ==> The input and output are taken and displayed in the web application using streamlit library '''

#----------------------------------------------------- Libraries ---------------------------------------------------
#Libraries for Google API Connection
import googleapiclient.discovery
import googleapiclient.errors

#Library for mongodb connection
from pymongo import MongoClient

#Library for Mysql Connection - mysql.connector, sqlalchemy
import mysql.connector
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

#Library for Streamit
import streamlit as st

# Other required Libraries
import pandas as pd
import json
from datetime import timedelta,datetime,time
import re


#------------------------------------------------- Yoututbe API connection---------------------------------------

api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey="YOUR_API_KEY")

#---------------------------------------------- MongoDB Connection ----------------------------------------------

def mongodb_con():
    client = MongoClient('mongodb://localhost:27017/')
    db = client["youtube_data"]
    return db


#-------------------------------------------- Extracting Channel details-----------------------------------------

def get_channel_details(youtube, channel_id):
        channel_details_request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
        channel_details_response = channel_details_request.execute()
        # return "Hi"
        channel = {
        'channel_id' : channel_details_response['items'][0]['id'],
        'channel_title' : channel_details_response['items'][0]['snippet']['title'],
        'channel_description' :channel_details_response['items'][0]['snippet']['description'],
        'channel_views' : channel_details_response['items'][0]['statistics']['viewCount'],
        'channel_video_count' : channel_details_response['items'][0]['statistics']['videoCount'],
        'channel_subscriber_count': channel_details_response['items'][0]['statistics']['subscriberCount'],
        'channel_playlist_id' : channel_details_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        overall_playlist_id = channel_details_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel, overall_playlist_id


#-------------------------------------------- Extracting Playlist details-----------------------------------------

def get_playlist_ids(youtube, channel_id):
    playlist_details_request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=25)
    playlist_ids = []
    playlist_details_response = playlist_details_request.execute()
    for i in range(0,len(playlist_details_response['items'])):
        x = playlist_details_response['items'][i]['id']
        playlist_ids.append(x)
    return playlist_ids


#-------------------------------------------- Extracting Videos details-----------------------------------------

def get_video_ids(youtube, overall_playlist_id):
    playlist_request = youtube.playlistItems().list(
    part="contentDetails",
    playlistId=overall_playlist_id,
    maxResults=10  # You can adjust this to get more results per request
    )
    playlist_response = playlist_request.execute()
    video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]
    video_details_request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=",".join(video_ids)
        )    
    video_details_response = video_details_request.execute()
    return video_ids,video_details_response


#-------------------------------------------- Extracting Comments details-----------------------------------------

def get_comments(youtube, video_ids):
    comments_by_video = {}
    for video_id in video_ids:
        comment_request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=10  # You can adjust this to get more comments per request
        )
        comment_response = comment_request.execute()
        video_comments = []
        for item in comment_response["items"]:
            x = {"comment_id":item["id"],
                "video_id":item["snippet"]["videoId"],
                "comment_text" : item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "comment_author":item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "comment_publishedat":item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]}
            video_comments.append(x)
        comments_by_video[video_id] = video_comments
    return comments_by_video


#-------------------------------------------- Duration convertion -----------------------------------------

def duration_convertion(duration_str):
    pattern = r'PT(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?'

    match = re.match(pattern, duration_str)

    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0

        duration = time(hours, minutes, seconds)
    return str(duration)


#-------------------------------------------- Convert data to JSON -----------------------------------------

def convert_to_json(channel_details,video_details_response,comment_details):
    video_details_1 = []
    for i in video_details_response["items"]:
        video_comments = comment_details.get(i["id"], [])  # Get comments for this video, default to an empty list if no comments
        x = {
            "video_id": i["id"],
            "video_name": i["snippet"]["title"],
            "video_description": i["snippet"]["description"],
            "tags": i.get('snippet', {}).get('tags', 'No Tags Found'),
            "publishedat": i["snippet"]["publishedAt"],
            "view_count": i["statistics"]["viewCount"],
            "like_count": i.get("statistics", {}).get('likeCount','No likes'),
            "fav_count": i["statistics"]["favoriteCount"],
            "comment_count": i["statistics"]["commentCount"],
            "duration": duration_convertion(duration_str=i["contentDetails"]["duration"]),
            "thumbnail": i["snippet"]["thumbnails"]["default"]["url"],
            "caption_status": "Avaiable" if i["contentDetails"]["caption"] else "Not Available",
            "comments":video_comments # Add comments for this video
        }
        video_details_1.append(x)
    
    channels = {"channel_details":channel_details,
            "videos":video_details_1}
    channels["_id"] = channels["channel_details"]["channel_id"]
    with open(channel_details["channel_title"], 'w') as json_file:
        json.dump(channels,json_file, indent=4)
    

#-------------------------------------------- Store the JSON file to MongoDB -----------------------------------------

def store_in_mongoDB( db,title, channel_id):
    # print(title)
    if title in db.list_collection_names():
                db[title].drop()
    collection = db[title]
    try:
        with open(title, 'r') as json_file:
            data = json.load(json_file)
            collection.insert_one(data)
            collection.replace_one({'_id': channel_id}, data, upsert=True)
    except:
        return False
    else:
        return True

#-------------------------------------------- Time convertor -----------------------------------------

def time_convertor(publishedat):
    published_at_datetime = datetime.fromisoformat(publishedat)
    formatted_published_at = published_at_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_published_at


#-------------------------------------------- MySQL Connection -----------------------------------------

def connect_to_mysql():
    try:
        conn= mysql.connector.connect(
                            host = "localhost",
                            username = "YOUR_USERNAME",
                            password = "YOUR_PASSWORD"
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data")
    cursor.close()
    engine = create_engine("mysql+mysqlconnector://root:mysql12345;@localhost:3306/youtube_data")
    return conn, engine


#-------------------------------------------- MySQL Table Creation -----------------------------------------

def mysql_table_creation():

    base = declarative_base()
    class channeldet(base):
        __tablename__ = "channel"

        channel_id = Column(String(255), primary_key=True)
        channel_name = Column(String(255))
        channel_description = Column(Text)
        channel_views = Column(Integer)
        video_count = Column(Integer)
        subscriber_count = Column(Integer)
        playlist_id = Column(String(255))

    class playlistdet(base):
        __tablename__ = "playlist"

        playlist_id = Column(String(255), primary_key=True)
        channel_id = Column(String(255), ForeignKey('channel.channel_id'))

    class videosdet(base):
        __tablename__ = "videos"
        video_id = Column(String(255), primary_key=True)
        channel_id = Column(String(255), ForeignKey('channel.channel_id'))
        video_name = Column(String(255))
        video_description = Column(Text)
        publishedat = Column(DateTime)
        view_count = Column(Integer)
        like_count = Column(Integer)
        fav_count = Column(Integer)
        comment_count = Column(Integer)
        duration = Column(String(255))
        thumbnail = Column(Text)
        caption_status = Column(String(255))

    class commentsdet(base):
        __tablename__ = "comments"
        comment_id = Column(String(255), primary_key=True)
        video_id =  Column(String(255), ForeignKey('videos.video_id'))
        comment_text= Column(Text)
        comment_author = Column(String(255))
        comment_publishedat = Column(DateTime)    
    return base


#-------------------------------------------- Data fetching in MySQL ------------------------------------------------

def fetch_from_mongodb(engine, id):
    collection_names = db.list_collection_names()
    for collection_name in collection_names:
        collection = db[collection_name]
        document = collection.find_one({"_id": id})
        if document is not None:
            output = collection.find({}) 
            break
    for i in output:
        document = i
    # print(document)
    channel_details_extract = {
        "channel_id" :  document["channel_details"]["channel_id"],
        "channel_name" : document["channel_details"]["channel_title"],
        "channel_description" : document["channel_details"]["channel_description"],
        "channel_views": document["channel_details"]["channel_views"],
        "video_count" : document["channel_details"]["channel_video_count"],
        "subscriber_count": document["channel_details"]["channel_subscriber_count"],
        "playlist_id": document["channel_details"]["channel_playlist_id"]}
    channel_dataframe = pd.DataFrame.from_dict(channel_details_extract, orient='index').T


    playlist_details_extract ={
        "channel_id" :  document["channel_details"]["channel_id"],
        "playlist_id": document["channel_details"]["channel_playlist_id"]
    }
    playlist_dataframe = pd.DataFrame.from_dict(playlist_details_extract, orient="index").T

    video_details_extract =[]
    comment_details_extract = []
    for i in range(len(document["videos"])):
        x = {
            "video_id": document["videos"][i]["video_id"],
            "channel_id":document["channel_details"]["channel_id"],
            "video_name": document["videos"][i]["video_name"],
            "video_description": document["videos"][i]["video_description"],
            "publishedat": time_convertor(document["videos"][i]["publishedat"]),
            "view_count": document["videos"][i]["view_count"],
            "like_count": document["videos"][i]["like_count"],
            "fav_count": document["videos"][i]["fav_count"],
            "comment_count": document["videos"][i]["comment_count"],
            "duration": document["videos"][i]["duration"],
            "thumbnail": document["videos"][i]["thumbnail"],
            "caption_status": document["videos"][i]["caption_status"]
            }
        video_details_extract.append(x)
        for j in range(len(document["videos"][i]["comments"])):
            y = {
                "comment_id":document["videos"][i]["comments"][j]["comment_id"],
                "video_id":document["videos"][i]["comments"][j]["video_id"],
                "comment_text" : document["videos"][i]["comments"][j]["comment_text"],
                "comment_author": document["videos"][i]["comments"][j]["comment_author"],
                "comment_publishedat":time_convertor(document["videos"][i]["comments"][j]["comment_publishedat"])    
                }
            comment_details_extract.append(y)
        
    video_dataframe = pd.DataFrame(video_details_extract)
    comment_dataframe = pd.DataFrame(comment_details_extract)
    return channel_dataframe, playlist_dataframe, video_dataframe, comment_dataframe


#-------------------------------------------- Function to Queries the Data --------------------------------------------

def query1(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("select v.video_name AS `VIDEO NAME`, ch.channel_name AS `CHANNEL NAME` from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id")
        q1_rows = cursor.fetchall()
        cursor.close()
        return q1_rows

def query2(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("Select channel_name, video_count from channel WHERE video_count = (SELECT MAX(video_count) from channel)")
        q2_rows = cursor.fetchall()
        cursor.close()
        return q2_rows

def query3(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT  ch.channel_name,v.video_name, v.view_count from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id ORDER BY v.view_count DESC LIMIT 10;")
        q3_rows = cursor.fetchall()
        cursor.close()
        return q3_rows

def query4(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT  ch.channel_name,v.video_name ,v.comment_count from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id;")
        q4_rows = cursor.fetchall()
        cursor.close()
        return q4_rows

def query5(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT ch.channel_name, v.video_name, v.like_count FROM videos AS v INNER JOIN channel AS ch  WHERE v.channel_id = ch.channel_id AND v.like_count = (SELECT MAX(like_count) FROM videos); ")
        q5_rows = cursor.fetchall()
        cursor.close()
        return q5_rows

def query6(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT video_name, like_count FROM videos ORDER BY like_count;")
        q6_rows = cursor.fetchall()
        cursor.close()
        return q6_rows

def query7(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT channel_name, channel_views from channel ORDER BY channel_views DESC;")
        q7_rows = cursor.fetchall()
        cursor.close()
        return q7_rows

def query8(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute('''SELECT ch.channel_name, v.video_name,DATE_FORMAT(publishedat, '%Y') as publish_year
                            FROM videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id and 
                            DATE_FORMAT(publishedat, '%Y') = '2023';''')
        q8_rows = cursor.fetchall()
        cursor.close()
        return q8_rows

def query9(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT ch.channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v.duration)))), '%H:%i:%s') AS average_duration from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id GROUP BY ch.channel_name;")
        q9_rows = cursor.fetchall()
        cursor.close()
        return q9_rows

def query10(conn):
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("USE youtube_data")
        cursor.execute("SELECT ch.channel_name, v.video_name, v.comment_count  from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id and v.comment_count = (SELECT MAX(comment_count) from videos)")
        q10_rows = cursor.fetchall()
        cursor.close()
        return q10_rows


#--------------------------------------------------- **** Beginning of the Streamlit part **** ----------------------------------------------------


st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
st.divider()

container1_state = st.session_state.get('container1_state', True)
container2_state = st.session_state.get('container2_state', False)
container3_state = st.session_state.get('container3_state', False)
channel_name= st.session_state.get('channel_name', None)

#------------------------------------------- Getting Channel Id from User -------------------------------------------------
if container1_state:
    with st.container():
        st.write(":blue[By clicking on the Extract Data will collect data from yoututbe API and store in MongoDB]")
        channel_id = st.text_input('Enter the Channel ID', placeholder = "UCLvydIgNyNSI-eMy8-gw-Ng")
        col1, col2 = st.columns(2)
        with col1:
            extract = st.button(
                label = "Extract Data", 
                help = "On clicking on this button will extract data from youtube API and store in MongoDB")
        with col2:
            next1 = st.button(label = "Next", help="Move to next step")
            #----------- To display number channel present in MongoBD ------------------------------------------------
            db = mongodb_con()
            collections = db.list_collection_names()
            num_collections = len(collections)
            st.write(":violet[Number channel present in mongoDB:]", num_collections)
    #-------------------------------------------- Data extraction and storing in MongoDB Part ---------------------------------------------    
    
        if extract:
            channel_details, overall_playlist_id = get_channel_details(youtube, channel_id)
            st.session_state.channel_name = channel_details["channel_title"]
            playlist_ids = get_playlist_ids(youtube, channel_id)
            video_ids,video_details_response = get_video_ids(youtube, overall_playlist_id)
            comment_details = get_comments(youtube, video_ids)
            json_data = convert_to_json(channel_details,video_details_response,comment_details)
            db = mongodb_con()
            mongodb = store_in_mongoDB(db, channel_details["channel_title"],channel_id)
            st.success('Data extracted from YouTube API and stored in MongoDB successfully', icon="✅")
            

    #------------------------------------------- Move to Next part of the project --------------------------------------       
    if next1:
            st.session_state.container1_state = False
            st.session_state.container2_state = True
            st.session_state.container3_state = False
            st.experimental_rerun()

#------------------------ Getting channel ID as input for moving to MySQL ----------------------------------------------

if container2_state:
    with st.container():
        db = mongodb_con()
        conn, engine = connect_to_mysql()
        connection = engine.connect()
        st.write(":blue[This part migrate the channel data from MongoDB to SQL]")
        collections = db.list_collection_names()
        num_collections = len(collections)
        channel_id_name  = []
        for collection_name in collections:
            collection = db[collection_name]
            documents = collection.find({}, {'_id': 1})

            print(f'Collection: {collection_name}')
            for document in documents:
                print(document["_id"])
                channel_id_name.append(document["_id"])
        chnanel_id_option = st.selectbox("Choose Channel Id", channel_id_name)
        #------------------ To display the channel Name to the user -----------------
        for collection_name in collections:
            
            collection = db[collection_name]
            document = collection.find_one({"_id": chnanel_id_option})
            if document:
                cur_channel_name = document["channel_details"]["channel_title"]
        st.write(":violet[Here is the selected Channel Name:]",cur_channel_name)


        col1, col2, col3= st.columns(3)
        with col1:
            prev1 = st.button(label="Go to Previous")
        with col2:
            move_to_sql = st.button(
                            label= "Move to SQL",
                            help = "On clicking this button will migrate the data of the selected channel from MongoDB to SQL"
            )
        with col3:
            next2 = st.button(label = "Next", help="Move to next step")
        
        if move_to_sql:
            Base = mysql_table_creation()
            Base.metadata.create_all(engine)
            channel_dataframe, playlist_dataframe, video_dataframe, comment_dataframe = fetch_from_mongodb(engine, chnanel_id_option)
            # st.dataframe(video_dataframe)
            cursor = conn.cursor()
            cursor.execute("USE youtube_data")
            existing_query = f"SELECT channel_id FROM channel WHERE channel_id = %s"
            values = (chnanel_id_option,)
            cursor.execute(existing_query,values)
            result = cursor.fetchone()
            if result:
                st.error("Channel details already prseent in database")
            else:
                channel_dataframe.to_sql('channel', engine, if_exists='append', index=False)
                playlist_dataframe.to_sql('playlist', engine, if_exists='append',index=False)
                video_dataframe.to_sql('videos', engine, if_exists='append', index=False)
                comment_dataframe.to_sql('comments', engine, if_exists='append', index=False)
                st.success('Data extracted from MongoDB and stored in MySQL successfully', icon="✅")

        if next2:
            st.session_state.container1_state = False
            st.session_state.container2_state = False
            st.session_state.container3_state = True
            st.experimental_rerun()
        
        if prev1:
            st.session_state.container1_state = True
            st.session_state.container2_state = False
            st.session_state.container3_state = False
            st.experimental_rerun()
        
#----------------------------------------------- Getting Query as Input --------------------------------------------------
if container3_state:
    with st.container():
        conn, engine = connect_to_mysql()
        query_option = st.selectbox("Choose one of the query", 
                        ('1. What are the names of all the videos and their corresponding channels?',
                    '2. Which channels have the most number of videos, and how many videos do they have?',
                    '3. What are the top 10 most viewed videos and their respective channels?',
                    '4. How many comments were made on each video, and what are their corresponding video names?',
                    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                    '8. What are the names of all the channels that have published videos in the year 2023?',
                    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = "sql_queries")
        col1, col2 = st.columns(2)
        with col1:
            prev2 = st.button(label="Go to Previous")
        with col2:
            query_button = st.button(label="Show Data", help="On clicking on this button will query the data from mysql and show here")                
        
        if prev2:
            st.session_state.container1_state = False
            st.session_state.container2_state = True
            st.session_state.container3_state = False
            st.experimental_rerun()
        if query_button:
            if query_option == "1. What are the names of all the videos and their corresponding channels?":
                result = query1(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Video Name'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "2. Which channels have the most number of videos, and how many videos do they have?":
                result = query2(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Video Count'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)


            if query_option == "3. What are the top 10 most viewed videos and their respective channels?":
                result = query3(conn)
                df = pd.DataFrame(result, columns=['Channel Name','Video Name', 'View Count'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "4. How many comments were made on each video, and what are their corresponding video names?":
                result = query4(conn)
                df = pd.DataFrame(result, columns=['Channel Name','Video Name', 'Comment Count'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
                result = query5(conn)
                df = pd.DataFrame(result, columns=['Channel Name','Video Name', 'Like Count'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                result = query6(conn)
                df = pd.DataFrame(result, columns=['Video Name', 'Like Count'])
                df.index = range(1, len(df) + 1)
                st.write("Disclaimer: Dislike cannot be fetch from youtube API")
                st.dataframe(df)
            if query_option == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                result = query7(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Total Views'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "8. What are the names of all the channels that have published videos in the year 2023?":
                result = query8(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Video Name', 'Published Year'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                result = query9(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Average Duration'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            if query_option == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
                result = query10(conn)
                df = pd.DataFrame(result, columns=['Channel Name', 'Video Name', 'Comment Count'])
                df.index = range(1, len(df) + 1)
                st.dataframe(df)
            
#--------------------------------------- **** End OF Code **** -------------------------------------------------------


#pip install google-api-python-client
import streamlit as st
import time
from datetime import datetime
from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
import pymongo
import psycopg2
import isodate
from isodate import *
st.set_page_config(layout="wide")

###############
page_bg_img ="""
<style>
[data-testid="stAppViewContainer"]{
       background: #6fbfb2;
       background: -webkit-linear-gradient(0deg, #6fbfb2 0%, #89ec93 100%);
       background: linear-gradient(0deg, #6fbfb2 0%, #89ec93 100%);
        
}
</style>
"""
sidepage_bg_img ="""
<style>
[data-testid="stSidebar"][aria-expanded="true"]{
        background: #6fbfb2;
         
</style>
"""

sidepadding_style ="""
<style>
[data-testid="stSidebar"][aria-expanded="true"]{
        padding-top:2rem;
        padding:0
        
</style>
"""


pd.set_option('display.max_columns', None)

st.write('<style>div.block-container{padding-top:2rem;}</style>', unsafe_allow_html=True)
padding_top = 0
st.markdown(page_bg_img,unsafe_allow_html=True)
st.markdown(sidepage_bg_img,unsafe_allow_html=True)
st.markdown(sidepadding_style,unsafe_allow_html=True)
#st.markdown('<style>body{background-color: black;}</style>',unsafe_allow_html=True)

stSidebarContainer = st.sidebar
with st.sidebar:
    
    st.header(':blue[Youtube Data Harvesting & Warehousing]',divider = 'rainbow')
    stdata = st.sidebar.button("Database Collections")
    stid = st.sidebar.text_input("Enter youtube channel Id")
    #stdata = st.sidebar.button("Get Data")
    stmon = st.sidebar.button("Extract and Store data in MongoDB")
    stmigrate = st.sidebar.button("Migrate to SQL")
    st.write('## :blue[Select any question to get Insights]')
    question = st.selectbox('**select questions**',(" ",
                            '1. What are the names of all the videos and their corresponding channels?',
                            '2. Which channels have the most number of videos, and how many videos do they have?',
                            '3. What are the top 10 most viewed videos and their respective channels?',
                            '4. How many comments were made on each video, and what are their corresponding video names?',          
                            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',          
                            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                            '8. What are the names of all the channels that have published videos in the year 2023?',
                            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                            key='collection_question')                              
                      
st.markdown("###### :blue[Domain] : Social Media")
st.markdown("###### :blue[Technologies used] : Python,MongoDB, Youtube Data API, Postgres, Streamlit")
st.markdown("###### :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB , migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")


        

##################

#global cid,channels,playlist_id,cname, video_ids,playid,channel_df,video_df,comment_df,mycol
global cid,channel_data,pl_data,video_data,comment_data,cm
cid = stid
#establish connection from youtube api
def API_connect():
    API_key='AIzaSyDCSF2K4nHZNsIiVLU2RW8l9vguiEJ5_f4'
    youtube = build('youtube', 'v3', developerKey=API_key)
    return youtube

youtube = API_connect()

def check_valid_id(youtube,channel_ids):
    try :
        try:
            channel_request = youtube.channels().list(
                part = 'snippet,statistics,contentDetails',
                id = channel_ids)
            
            channel_response = channel_request.execute()

            if 'items' not in channel_response:
                st.write(f"Invalid channel id: {channel_ids}")
                st.error("Enter the correct 11-digit **channel_id**")
                return None
                    
        except: 
            st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes', icon='🚨')
            st.write("An error occurred:")
            return None
            
    except:
        st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')

 
def get_channel_data(youtube,channel_ids):
    global channels,playlist_id,channel_id
    #channnel_id = channelid
    request = youtube.channels().list(id=channel_ids,part = 'snippet,statistics,contentDetails,status')
    response = request.execute()
    for i in range(len(response['items'])):
      channels = dict(
                  channel_id= response['items'][i]['id'],
                  channel_name= response['items'][i]['snippet']['title'],
                  channel_description= response['items'][i]['snippet']['description'],
                  channel_published_date= response['items'][i]['snippet']['publishedAt'],
                  channel_type= response['items'][i]['kind'],
                  channel_views= response['items'][i]['statistics']['viewCount'],
                  channel_status=response['items'][i]['status']['privacyStatus'],
                  subscriber_count = response['items'][i]['statistics']['subscriberCount'],
                  video_count = response['items'][i]['statistics']['videoCount'],
                  playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']     )
    
    return channels
    

def get_playlist_id(youtube,cid):
    playlistid = []
    request = youtube.channels().list(id=cid,part = 'contentDetails')
    response = request.execute()
    for i in range(len(response['items'])):
          playlistid.append(response['items'][i]['contentDetails']['relatedPlaylists']['uploads'] )
    
    return playlistid 



def get_playlist_data(youtube, p_id):
    global pids
    request = youtube.playlists().list(part='snippet', id=p_id)
    response = request.execute()   
    
    for i in range(len(response['items'])):
        pids = dict(playlist_id=response['items'][i].get('id',0),
                    channel_id=response['items'][i]['snippet']['channelId'],
                    playlist_name=response['items'][i]['snippet']['title'])   
    
    return pids
 
def get_video_ids(youtube, playlist_id):
    global video_id
    video_id = []
    next_page_token = None
    for i in playlist_id:
        request = youtube.playlistItems().list(part='contentDetails',playlistId=playlist_id, maxResults=50,pageToken=next_page_token)
        response = request.execute()
        # Get video IDs
        for item in response['items']:
            video_id.append(item['contentDetails']['videoId'])

        # Check if there are more pages
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return video_id

    

def get_video_data(youtube,video_ids):
    global video_data,videos
    video_data = []
    IsCmtAvail = 'N'
    # Get video details
    for i in video_ids:
            request = youtube.videos().list(part='snippet, statistics, contentDetails',id=i,maxResults = 50)
            response = request.execute()

            for j in range(len(response['items'])):
                videos = dict(
                    video_id = response['items'][j]['id'],
                    video_name = response['items'][j]['snippet']['title'],
                    video_description = response['items'][j]['snippet']['description'],
                    channel_id = response['items'][j]['snippet']['channelId'],
                    tags = response['items'][j]['snippet'].get('tags', []),
                    published_at = response['items'][j]['snippet']['publishedAt'],
                    view_count = response['items'][j]['statistics']['viewCount'],
                    like_count = response['items'][j]['statistics'].get('likeCount', 0),
                    dislike_count = response['items'][j]['statistics'].get('dislikeCount', 0),
                    favorite_count = response['items'][j]['statistics'].get('favoriteCount', 0),
                    comment_count = response['items'][j]['statistics'].get('commentCount', 0),
                    duration = response['items'][j].get('contentDetails', {}).get('duration', 'Not Available'),
                    thumbnail = response['items'][j]['snippet']['thumbnails']['high']['url'],
                    caption_status = response['items'][j].get('contentDetails', {}).get('caption', 'Not Available'),
                    comments = 'Unavailable'
              )
            
            video_data.append(videos)    
              
       
    return video_data

   
def get_comment_data(youtube,video_ids):
    global comment_data,comments,data
    comment_data = []
    for i in video_ids:
        request = youtube.commentThreads().list(part="id,snippet,replies", videoId=i, maxResults=50)
        response = request.execute()
        for k in range(len(response["items"])):
            data = dict(
                comment_id = response["items"][k]["snippet"]["topLevelComment"]["id"],
                comment_text = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                comment_author = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                comment_publishedAt = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                video_id = response["items"][k]['snippet']['topLevelComment']["snippet"]['videoId'])
            comment_data.append(data)

           
    return comment_data



    

if stmon == True: 
    comments = []    
    with st.spinner("Loading..."):
        time.sleep(10)  
    client = pymongo.MongoClient("mongodb+srv://dhivya:Myworldd@cluster0.yjmzisp.mongodb.net/?retryWrites=true&w=majority")
    db = client["Youtube_DB"]
    col = db["Youtube_data"]
    st.success("Database and collection created in mongoDB") 
    c_stats = get_channel_data(youtube,cid) 
    st.success("Channel details retrieved")   
    cname = c_stats['channel_name']
    playid1 = get_playlist_id(youtube,cid)
    pid1 = c_stats["playlist_id"]    
    p_stats = get_playlist_data(youtube,pid1)
    st.success("Playlist  details retrieved") 
    vids = get_video_ids(youtube,pid1)
    vdata = get_video_data(youtube,vids)
    vd = int(vdata[0]["comment_count"])
    st.success("video details retrieved") 
    if  vd > 0:                
         com = get_comment_data(youtube,vids)
    cm = get_comment_data(youtube, vids)      
    st.success("comment details retrieved")   
                    
                     
    # = get_comment_data(youtube,vids)
    data = {    '_id'   :stid ,       
                "Channel_Name": cname,
                'ChannelDetails':c_stats,
                'PlaylistDetails':p_stats,
                'VideoDetails':vdata,
                'CommentDetails': cm
        }
    col.insert_one(data)
    st.success("Data stored in MongoDB successfully")
    

if stmigrate == True:  
        with st.spinner('Data uploading...'):
            time.sleep(5)
      
        client = pymongo.MongoClient("mongodb+srv://dhivya:Myworldd@cluster0.yjmzisp.mongodb.net/?retryWrites=true&w=majority")
    
        db1 = client['Youtube_DB']
        col1 = db1["Youtube_data"]
        document_names = []
        channel_list= []
        play_list = []
        video_list = []
        comment_list = []
        playl_id = []
        
        for i in col1.find():      
            if(i['_id'] == stid):
                channel_list.append(i['ChannelDetails'])
                playl_id.append(i['ChannelDetails']['playlist_id'] )
        channel_df = pd.DataFrame(channel_list) 
        st.write(channel_df) 
 
        
        channel_df["subscriber_count"] = channel_df["subscriber_count"].astype(int)
        #transforming channel views to int
        channel_df["channel_views"] = channel_df["channel_views"].astype(int)
        #transforming video count to int
        channel_df["video_count"] = channel_df["video_count"].astype(int)
        #transforming published date to date time
        channel_df["channel_published_date"] = pd.to_datetime(channel_df["channel_published_date"])
        channel_df.fillna('Data unavailable',inplace=True)

        
        for i in col1.find():      
            if(i['_id'] == stid):
                play_list.append(i['PlaylistDetails'])
        play_df = pd.DataFrame(play_list)                   
        st.write(play_df)      
       

        #col3 = db["VideoDetails"]

        for i in col1.find():      
            if(i['_id'] == stid):                  
                for j in i['VideoDetails']:
                  video_list.append(j)
        video_df = pd.DataFrame(video_list)                   
         

        extracted_col = channel_df["playlist_id"]
        video_df = video_df.join(extracted_col)    
        video_df['playlist_id'] =  video_df ['playlist_id'].fillna(playl_id[0])               
        st.write(video_df)    

        video_df["view_count"] = video_df["view_count"].astype(int)
        #transforming channel views to int
        video_df["like_count"] = video_df["like_count"].astype(int)
        #transforming video count to int
        video_df["comment_count"] = video_df["comment_count"].astype(int)
        #transforming published date to date time
        video_df["published_at"] = pd.to_datetime(video_df["published_at"])
        video_df.fillna('Data unavailable',inplace = True)

        for i in range(len(video_df["duration"])):
            duration = isodate.parse_duration(video_df["duration"].loc[i])
            seconds = duration.total_seconds()
            video_df.loc[i, 'duration'] = int(seconds)

        video_df['duration'] = pd.to_numeric(video_df['duration'])

        for i in col1.find():      
            if(i['_id'] == stid):
                for j in i['CommentDetails']:
                  comment_list.append(j)
        comment_df = pd.DataFrame(comment_list)                   
        st.write(comment_df)
                       
       

        #transforming published date to date time
        comment_df["comment_publishedAt"] = pd.to_datetime(comment_df["comment_publishedAt"])
        comment_df["comment_publishedAt"] = comment_df["comment_publishedAt"].dt.strftime('%Y-%m-%d %H:%M:%S')
        comment_df.fillna('Data unavailable')
        st.write("Data transformation is done")
        #extracted_col = play_df["playlist_id"] 
        #comment_df = comment_df.join(extracted_col)
        #comment_df = play_df['playlist_id'].copy()
       
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        conn = psycopg2.connect(host = "localhost",user = "postgres",password="Disha400",port="5432",database = "youtube_data")
        sqldb=conn.cursor()
        st.write("connection established") 

        


        def sql_create_channel_data():        

            c = "CREATE TABLE if not exists Channel_details (channel_id varchar(255) PRIMARY KEY,channel_name varchar(255),channel_description text,channel_published_Date date,channel_views int,subscriber_count int,video_count int,playlist_id varchar(255))"
            sqldb.execute(c)
            for index, row in channel_df.iterrows():
                query ="insert into Channel_details(channel_id, channel_name,channel_description,channel_published_Date,channel_views,subscriber_count, video_count, playlist_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                sqldb.execute(query, (row.channel_id,row.channel_name,row.channel_description,row.channel_published_date,row.channel_views,row.subscriber_count, row.video_count, row.playlist_id))
            conn.commit()
            st.write("channel created") 
            st.write("data inserted")

        def sql_create_playlist_data():
           
            c = "CREATE TABLE if not exists Playlist_details (playlist_id varchar(250) PRIMARY KEY,channel_id varchar(250),playlist_name varchar(250))"
            sqldb.execute(c)
            for index, row in play_df.iterrows():
                query ="insert into Playlist_details(playlist_id, channel_id, playlist_name) values(%s,%s,%s)"
                sqldb.execute(query, (row.playlist_id,row.channel_id,row.playlist_name))
            conn.commit()
            st.write("playlist created")
        
        st.write("data inserted")
        st.write("sql_create_playlist_data()hannel data and playlist inserted into table")

        def sql_create_video_data():            
            c = "CREATE TABLE if not exists Video_details (video_id varchar(255) PRIMARY KEY,video_name varchar(255), video_description text,published_at date,view_count int,like_count int,comment_count int,channel_id varchar(255),playlist_id varchar(255),duration int)"
           # d = "ALTER TABLE Video_details ADD CONSTRAINT fk_vid FOREIGN KEY (playlist_id) REFERENCES Playlist_Details(playlist_id) MATCH FULL;"
            #e = "alter table video_details add colomn duration varchar(255)"
            sqldb.execute(c)
            #sqldb.execute(e)
            #sqldb.execute(d)
            for index, row in video_df.iterrows():
                
                    query ="insert into Video_details(video_id, video_name, video_description,published_at,view_count,like_count,comment_count,channel_id,playlist_id,duration) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    
                    
                    sqldb.execute(query, (row.video_id,row.video_name,row.video_description,row.published_at,row.view_count,row.like_count,row.comment_count,row.channel_id,row.playlist_id,row.duration))
                    
                
                    
            conn.commit()
            st.write("video table and data created")


        def sql_create_comment_data():          
            c = "CREATE TABLE if not exists Comment_Details(comment_id varchar(255) PRIMARY KEY, video_id varchar(250) ,comment_text text,comment_author varchar(255),comment_publishedAt date)"
           # d = "ALTER TABLE Comment_Details ADD CONSTRAINT fk_comm FOREIGN KEY (video_id) REFERENCES video_details(video_id) MATCH FULL;"
            sqldb.execute(c)            
           # sqldb.execute(d)           
           
            for index, row in comment_df.iterrows():
                query ="insert into Comment_Details(comment_id, comment_text, comment_author,comment_publishedAt,video_id) values(%s,%s,%s,%s,%s)"
                sqldb.execute(query, (row.comment_id,row.comment_text,row.comment_author,row.comment_publishedAt,row.video_id))
            
            conn.commit()
            
            st.write("comment table and data created")  

       
        
        sql_create_channel_data()
        st.success('Sucess!, Channel Table Created')        
        sql_create_playlist_data()
        st.success('Sucess!, Playlist Table Created')
        sql_create_video_data()
        st.success('Sucess!, Video Table Created')        
        sql_create_comment_data()       
        st.success('Sucess!, Comment Table Created')
############
if stdata:
    with st.spinner("Loading..."):
        time.sleep(5)
    st.markdown('####  :red[Data base collections in postgresql database]')
    conn = psycopg2.connect(host = "localhost",user = "postgres",password="Disha400",port="5432",database = "youtube_data")
    sqldb=conn.cursor()
    query = "select channel_name,channel_id from channel_details"  
    #sqldb.execute(query,row.channel_name,row.channel_id)
    sqldb.execute(query)
    results =sqldb.fetchall()
    conn.commit() 
    df = pd.DataFrame(results, columns=['Channel Name', 'Channel ID']).reset_index(drop=True)   
    st.write(df)    

##############################3
#with st.sidebar:
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
conn = psycopg2.connect(host = "localhost",user = "postgres",password="Disha400",port="5432",database = "youtube_data")
sqldb=conn.cursor()


if question == '1. What are the names of all the videos and their corresponding channels?':
    query = "SELECT c.channel_name,v.video_name from channel_details as c join video_details as v on c.playlist_id=v.playlist_id"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=['Channel Name', 'Video Name']).reset_index(drop=True)   
    st.markdown('#####  :red[Names of all the videos and their corresponding channels]')
    st.write(df) 
    
elif question == '2. Which channels have the most number of videos, and how many videos do they have?':

    query = "SELECT channel_Name, video_Count FROM channel_details ORDER BY video_count DESC"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_names", "video_count"])
    st.markdown('#####  :red[Channels with most number of videos]')
    st.write(df)
elif question == '3. What are the top 10 most viewed videos and their respective channels?':

    query = "SELECT A.channel_name,c.video_name, C.view_count FROM public.channel_details A INNER JOIN public.playlist_details B ON A.channel_id = B.channel_id JOIN public.video_details C ON B.playlist_id = C.playlist_id ORDER BY C.view_count DESC LIMIT 10"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_names", "video_count","view_count"])
    st.markdown('#####  :red[Top 10 most viewed videos and their channels]')
    st.write(df)    

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':

    query = "select v.video_name,v.comment_count from video_details as v"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["video_name", "comment_count"])
    st.markdown('#####  :red[Video names and their corresponding comment count]')
    st.write(df)

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':            
    query = 'select c.channel_name,v.video_name,v.like_count from channel_details as c join video_details as v on c.playlist_id=v.playlist_id order by v.like_count desc'
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_name", 'video_names', 'like_counts'])
    st.markdown('#####  :red[Videos with the highest number of likes and their corresponding channels]')
    st.write(df)  

elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query = "SELECT c.video_name, C.like_count FROM public.channel_details A INNER JOIN public.playlist_details B ON A.channel_id = B.channel_id JOIN public.video_details C ON B.playlist_id = C.playlist_id"
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=['channel_name', 'total_number_likes'])
    st.markdown('#####  :red[Total like count of videos]')
    st.write(df)     


elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = 'SELECT channel_name, channel_views FROM channel_details ORDER BY channel_views DESC;'
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=['video_names', 'total_number_views'])
    st.markdown('#####  :red[Video names with total number of views]')
    st.write(df)

elif question == '8. What are the names of all the channels that have published videos in the year 2023?':
    query = "select c.channel_name,v.published_at from channel_details as c join video_details as v on v.playlist_id=c.playlist_id where EXTRACT(YEAR FROM Published_at) = 2023"
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_name", "video_published_at 2023"])
    st.markdown('#####  :red[Videos published in 2023 along with their channel names]')
    st.write(df)

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query = "select c.channel_name,avg(v.duration) from channel_details as c inner join video_details as v on c.channel_id = v.channel_id group by  c.channel_name order by avg(v.duration) desc"
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=['channel_names','average_duration'])
    st.markdown('#####  :red[Average duration of all videos in each channel]')
    st.write(df)

elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = 'select c.channel_name,v.comment_count,v.video_name from channel_details as c inner join video_details as v on c.playlist_id=v.playlist_id order by comment_count desc'
    sqldb.execute(query)
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=['channel_name', 'highest_comment_counts', 'video_name'])
    st.markdown('#####  :red[Videos with highest number of comment count with theit channel names]')
    st.write(df)
    

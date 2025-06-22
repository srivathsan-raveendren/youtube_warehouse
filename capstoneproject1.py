from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import text
from pymongo import MongoClient
import googleapiclient.discovery
from sqlalchemy import BigInteger
import pandas as pd
import streamlit as st
from PIL import Image
import pymongo 


# Define the Base class for declarative table definitions
Base = declarative_base()


# Define SQLAlchemy ORM models for Channel, Video, and Comment
class Channel(Base):
    __tablename__ = 'channel_details'
    title = Column(String(250))
    channel_id = Column(String(100), primary_key=True)
    description = Column(String(1000))
    country = Column(String(10))
    date_of_joining = Column(String(50))
    thumbnail_url = Column(String(1000))
    subscriber_count = Column(BigInteger)
    video_count = Column(BigInteger)
    view_count = Column(BigInteger)
    videos = relationship("Video", back_populates="channel")

class Video(Base):
    __tablename__ = 'video_details'
    video_id = Column(String(100), primary_key=True)
    channel_id = Column(String(100), ForeignKey('channel_details.channel_id'))
    duration = Column(String(20))
    language = Column(String(10))
    description = Column(String(1000))
    title = Column(String(100))
    date_of_publish = Column(String(50)) 
    comment_count = Column(Integer)
    view_count = Column(Integer)
    like_count = Column(Integer)
    channel = relationship("Channel", back_populates="videos")
    comments = relationship("Comment", back_populates="video")

class Comment(Base):
    __tablename__ = 'comment_details'
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(String(100), ForeignKey('channel_details.channel_id'))
    video_id = Column(String(100), ForeignKey('video_details.video_id'))
    author_name = Column(String(100))
    comment = Column(String(1000))
    reply_count = Column(Integer)
    video = relationship("Video", back_populates="comments")


# Set up YouTube API client
API_key = "AIzaSyBtSOdH9Fdc6HJz6gV2NE2yMIc8RW04NZw"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_key)

# # Set up MongoDB client
# client = pymongo.MongoClient(
#     "mongodb+srv://srivathsanraveendren:<db_password>@youtube.y77hecd.mongodb.net/?retryWrites=true&w=majority&appName=Youtube"
# )
# mongo_db = client['youtube_data_db']
# mongo_collection = mongo_db['youtube_data']

# Set up MongoDB client
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['youtube_data']
mongo_collection = mongo_db['channel_data_capstone']

# Connect to SQL database
sql_engine = create_engine('mysql+pymysql://root:Roach%40551@localhost:3306/youtubedata')
Base.metadata.create_all(sql_engine)
Session = sessionmaker(bind=sql_engine)
sql_session = Session()

def ch_details(channel_id): #function that get channel details
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    channel_details = {
        'title': response['items'][0]['snippet']['title'],
        'channel_id': response['items'][0]['id'],
        'description': response['items'][0]['snippet']['description'][:1000],
        'country': response['items'][0]['snippet'].get('country', ''),
        'date_of_joining': response['items'][0]['snippet']['publishedAt'],
        'thumbnail_url': response['items'][0]['snippet']['thumbnails']['default']['url'],
        'subscriber_count': response['items'][0]['statistics']['subscriberCount'],
        'video_count': response['items'][0]['statistics']['videoCount'],
        'view_count': response['items'][0]['statistics']['viewCount']
    }
    return channel_details



def v_details(channel_id):  # ← here is your replaced block
    video_ids = []
    video_details = []
    # Get upload id …
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # build the full list of video_ids
    request = youtube.playlistItems().list(
        part="contentDetails",
        maxResults=50,
        playlistId=upload_id
    )
    while True:
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        if "nextPageToken" not in response:
            break
        request = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId=upload_id,
            pageToken=response["nextPageToken"]
        )

    # now loop through IDs and batch‐fetch details
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        details = {
            'channel_id': channel_id,
            'video_id': video_id,
            'duration': response['items'][0]['contentDetails'].get('duration', ''),
            'language': response['items'][0]['snippet'].get('defaultAudioLanguage', ''),
            'description': response['items'][0]['snippet'].get('description', '')[:1000],
            'title': response['items'][0]['snippet']['localized'].get('title', ''),
            'date_of_publish': response['items'][0]['snippet'].get('publishedAt', ''),
            'comment_count': response['items'][0]['statistics'].get('commentCount', ''),
            'view_count': int(response['items'][0]['statistics'].get('viewCount', 0)),
            'like_count': int(response['items'][0]['statistics'].get('likeCount', 0))
        }
        video_details.append(details)
    return video_details

def c_details(video_ids,channel_id): #function to get 10 comments or less from each video
    comment_details = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=10  # Limit to retrieve latest 10 comments
        )
        response = request.execute()
        comments = response.get("items", [])
        for comment_item in comments:
            cmt_details = {
                'channel_id': channel_id,
                'video_id': video_id,
                'author_name': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment': comment_item['snippet']['topLevelComment']['snippet']['textDisplay'][:1000],
                'reply_count': comment_item['snippet']['totalReplyCount']
            }
            comment_details.append(cmt_details)
    return comment_details

# Main function to scrape data and store in MongoDB
def scrape_and_store(channel_id):
    # Scrape channel details
    channel_info = ch_details(channel_id)

    # Scrape video details
    videos_info = v_details(channel_id)
    video_ids    = [v['video_id'] for v in videos_info]

    # Scrape comment details
    comment_info =  c_details(video_ids, channel_id)

    # Combine data into a single document
    document = {
        "channel": channel_info,
        "videos": videos_info,
        "comments": comment_info
    }

    # Store data in MongoDB
    mongo_collection.insert_one(document)
    return "Data inserted into MongoDB."
    
# Function to fetch all data and check if channel exists
def migrate_channel_details(channel_name_s):
    # Read data from channels table
    channels_df = pd.read_sql_table('channel_details', con=sql_engine)
    
    # Check if channel exists
    if channel_name_s in channels_df['title'].values:
        return f"Your Provided Channel {channel_name_s} is Already exists"
    
    # Fetch channel details from MongoDB
    single_channel_details = mongo_db["channel_data_capstone"].find_one({"channel.title": channel_name_s})
    
    if single_channel_details:
        # Insert channel details into channels table
        channel_data = single_channel_details["channel"]
        channel_df = pd.DataFrame([channel_data])  
        channel_df.to_sql('channel_details', con=sql_engine, if_exists='append', index=False)
        return "Channel details migrated successfully."
    else:
        return f"No data found for channel {channel_name_s} in MongoDB."

# Function to insert video data
def migrate_video_details(channel_name_s):
    # Fetch video details from MongoDB
    single_channel_details = mongo_db["channel_data_capstone"].find_one({"channel.title": channel_name_s})
    video_info = single_channel_details.get("videos", [])
    
    if video_info:
        # Insert video details into videos table
        video_df = pd.DataFrame(video_info)  
        video_df.to_sql('video_details', con=sql_engine, if_exists='append', index=False)
        return "Video details migrated successfully."
    else:
        return f"No video data found for channel {channel_name_s} in MongoDB."

# Function to insert comment data
def migrate_comment_details(channel_name_s):
    # Fetch comment details from MongoDB
    single_channel_details = mongo_db["channel_data_capstone"].find_one({"channel.title": channel_name_s})
    comment_info = single_channel_details.get("comments", [])
    
    if comment_info:
        # Insert comment details into comments table
        comment_df = pd.DataFrame(comment_info)  
        comment_df.to_sql('comment_details', con=sql_engine, if_exists='append', index=False)
        return "Comment details migrated successfully."
    else:
        return f"No comment data found for channel {channel_name_s} in MongoDB."
    
def tables(channel_name): # function that stores channel details, video details and comment details in SQL
    migrate_channel_details(channel_name)
    migrate_video_details(channel_name)
    migrate_comment_details(channel_name)

    return "Tables Created Successfully"

def show_channels_table(): #shows channel table
    ch_list=[]
    for ch_data in mongo_collection.find({},{"_id":0,"channel":1}):
        ch_list.append(ch_data["channel"])
    df=st.dataframe(ch_list)

    return df

def show_videos_table(): #shows video table
    vi_list=[]
    for vi_data in mongo_collection.find({},{"_id":0,"videos":1}):
        for i in range(len(vi_data["videos"])):
            vi_list.append(vi_data["videos"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table(): #shows comment table
    com_list=[]
    for com_data in mongo_collection.find({},{"_id":0,"comments":1}):
        for i in range(len(com_data["comments"])):
            com_list.append(com_data["comments"][i])
    df3=st.dataframe(com_list)

    return df3
#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

img = Image.open("download (1).png")
st.image(img,width=250)
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    for ch_data in mongo_collection.find({},{"_id":0,"channel":1}):
        ch_ids.append(ch_data["channel"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=scrape_and_store(channel_id)
        st.success(insert)
        st.balloons()

all_channels= []
for ch_data in mongo_collection.find({},{"_id":0,"channel":1}):
    all_channels.append(ch_data["channel"]["title"])
        
unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL questions
sql_question = st.selectbox("Select the question",("1 What are the names of all the videos and their corresponding channels?",
                                                    "2 Which channels have the most number of videos, and how many videos do they have?",
                                                    "3 What are the top 10 most viewed videos and their respective channels?",
                                                    "4 How many comments were made on each video, and what are their corresponding video names?",
                                                    "5 Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                    "7 What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8 What are the names of all the channels that have published videos in the year 2022?",
                                                    "9 What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10 Which videos have the highest number of comments, and what are their corresponding channel names?"
 
                                                  ))
if st.button("Get the table"):

    if sql_question == "1 What are the names of all the videos and their corresponding channels?" :
            result = sql_session.execute(text("select channel_details.title, video_details.title from channel_details join video_details on channel_details.channel_id = video_details.channel_id"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title', 'Video Title'])
            st.write(df)

    elif sql_question == "2 Which channels have the most number of videos, and how many videos do they have?":
            result = sql_session.execute(text("SELECT title, video_count FROM channel_details ORDER BY video_count DESC"))
            # Fetch all results
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Title', 'Video Count'])
            # Display the DataFrame
            st.write(df)

    elif sql_question == "3 What are the top 10 most viewed videos and their respective channels?":
            result = sql_session.execute(text("select channel_details.title, video_details.title, video_details.view_count from channel_details join video_details where channel_details.channel_id = video_details.channel_id order by video_details.view_count desc limit 10"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title','Video Title', 'View Count'])
            # Display the DataFrame
            st.write(df)

    elif sql_question ==  "4 How many comments were made on each video, and what are their corresponding video names?":
            result = sql_session.execute(text("select title, comment_count from video_details"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Video Title', 'No of Comments'])
            # Display the DataFrame
            st.write(df) 

    elif sql_question ==   "5 Which videos have the highest number of likes, and what are their corresponding channel names?":
            result = sql_session.execute(text("select channel_details.title, video_details.title, video_details.like_count from channel_details join video_details on channel_details.channel_id = video_details.channel_id order by video_details.like_count desc"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel title','Video Title', 'No of likes'])
            # Display the DataFrame
            st.write(df) 

    elif sql_question ==  "6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            result = sql_session.execute(text("select title, like_count from video_details"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Video Title', 'No of likes'])
            # Display the DataFrame
            st.write(df) 

    elif sql_question ==   "7 What is the total number of views for each channel, and what are their corresponding channel names?":
            result = sql_session.execute(text("select title, view_count from channel_details"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title', 'No of Views'])
            # Display the DataFrame
            st.write(df)

    elif sql_question ==    "8 What are the names of all the channels that have published videos in the year 2022?":
            result = sql_session.execute(text("SELECT channel_details.title, video_details.date_of_publish FROM channel_details JOIN video_details ON channel_details.channel_id = video_details.channel_id WHERE SUBSTRING(video_details.date_of_publish, 1, 4) = '2022'"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title', 'Date of publish'])
            # Display the DataFrame
            st.write(df)  
    
    elif sql_question ==     "9 What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            result = sql_session.execute(text('''SELECT
                                                        channel_details.title,
                                                        AVG(
                                                            CASE
                                                                WHEN video_details.duration LIKE '%H%M%S' THEN (SUBSTRING(video_details.duration, 3, 2) * 3600) + (SUBSTRING(video_details.duration, 5, 2) * 60) + SUBSTRING(video_details.duration, 7, 2)
                                                                WHEN video_details.duration LIKE '%M%S' THEN (SUBSTRING(video_details.duration, 3, 2) * 60) + SUBSTRING(video_details.duration, 5, 2)
                                                                WHEN video_details.duration LIKE '%S' THEN SUBSTRING(video_details.duration, 3, 2)
                                                                ELSE 0
                                                            END
                                                        ) AS avg_duration_seconds
                                                    FROM
                                                        channel_details
                                                    JOIN
                                                        video_details ON channel_details.channel_id = video_details.channel_id
                                                    GROUP BY
                                                        channel_details.title;'''))
            rows = result.fetchall() 
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title', 'Average of videos in seconds'])
            # Display the DataFrame
            st.write(df)  
    
    elif sql_question ==    "10 Which videos have the highest number of comments, and what are their corresponding channel names?":
            result = sql_session.execute(text("select channel_details.title, video_details.title, video_details.comment_count from channel_details join video_details on channel_details.channel_id = video_details.channel_id order by video_details.comment_count desc"))
            rows = result.fetchall()
            # Convert the result to a DataFrame
            df = pd.DataFrame(rows, columns=['Channel Title', 'Video Title','Comment count'])
            # Display the DataFrame
            st.write(df) 

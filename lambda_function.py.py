import json
import os
import boto3
import requests
import logging
import time

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set your AWS region
REGION_NAME = 'eu-north-1'

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
s3 = boto3.client('s3', region_name=REGION_NAME)

# Spotify credentials
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Retrieve tokens from environment variables
ACCESS_TOKEN = os.getenv("SPOTIFY_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# Function to obtain access token using the refresh token
def get_access_token():
    global REFRESH_TOKEN  # Use the global variable for refresh_token
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": SPOTIPY_CLIENT_ID,
        "client_secret": SPOTIPY_CLIENT_SECRET
    }
    
    response = requests.post(url, headers=headers, data=payload)
    response_data = response.json()
    
    if "access_token" in response_data:
        # Update the global refresh token if a new one is provided
        if "refresh_token" in response_data:
            REFRESH_TOKEN = response_data["refresh_token"]
        return response_data["access_token"]
    else:
        logger.error("Failed to get access token: %s", response_data)
        raise Exception("Could not obtain access token")

# Function to get recently played tracks
def get_recently_played_tracks(access_token):
    url = "https://api.spotify.com/v1/me/player/recently-played"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        logger.error("Failed to fetch recently played tracks: %s", response.json())
        raise Exception("Could not fetch recently played tracks")
    
    return response.json()

# Function to save data to DynamoDB
def save_to_dynamodb(user_id, tracks):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    for track in tracks:
        item = {
            'user_id': user_id,
            'track_id': track['track']['id'],
            'track_name': track['track']['name'],
            'played_at': track['played_at'],
            'artists': [artist['name'] for artist in track['track']['artists']],
            'timestamp': int(time.time())
        }
        table.put_item(Item=item)

# Function to upload tracks to S3
def upload_to_s3(user_id, tracks):
    content = json.dumps(tracks, indent=4)
    file_name = f"recently_played/{user_id}_recently_played.json"
    
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=content,
        ContentType="application/json"
    )
    logger.info("Uploaded recently played tracks to S3: %s", file_name)

def lambda_handler(event, context):
    try:
        # Step 1: Get access token
        access_token = get_access_token()
        
        # Step 2: Get recently played tracks
        recently_played_tracks = get_recently_played_tracks(access_token)
        
        # Step 3: Save to DynamoDB
        user_id = "your_user_id"  # Your Spotify user ID
        save_to_dynamodb(user_id, recently_played_tracks['items'])
        
        # Step 4: Upload to S3
        upload_to_s3(user_id, recently_played_tracks['items'])
        
        return {
            'statusCode': 200,
            'body': json.dumps("Successfully fetched and stored recently played tracks.")
        }
    except Exception as e:
        logger.error("Error: %s", e)
        return {
            'statusCode': 500,
            'body': json.dumps("An error occurred: " + str(e))
        }

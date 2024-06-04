import os
import json
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import pandas as pd

# Configuración de la ruta del archivo CSV y JSON
file_path = 'docs/tracks_features.csv'
raw_responses_file_path = 'docs/raw_responses.json'

# Configuración de credenciales de Spotify
client_id = 'your_client_id'
client_secret = 'your_client_secret'

'''
# Configuración de credenciales de Spotify
client_id = 'your_client_id'
client_secret = 'your_client_secret'
redirect_uri = 'https://developer.spotify.com/'
username = 'your_username'
scope = 'user-library-read playlist-read-private playlist-read-collaborative playlist-modify-public playlist-modify-private'

# Configuración de la API de Spotify
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
sp = spotipy.Spotify(auth=token)
'''
def authenticate_spotify():
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    '''
    auth_manager=SpotifyOAuth(scope=scope, username=username, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
    return spotipy.Spotify(auth_manager=auth_manager)
    '''

def save_raw_responses(raw_responses):
    print("Saving raw responses...")
    with open(raw_responses_file_path, 'a') as file:
        for response in raw_responses['tracks']:
            file.write(json.dumps(response) + '\n')
    print("Raw responses saved.")

def load_raw_responses():
    print("Loading raw responses...")
    if os.path.exists(raw_responses_file_path):
        with open(raw_responses_file_path, 'r') as file:
            raw_responses_list = [json.loads(line.strip()) for line in file if line.strip()]
            print("Raw responses loaded.")
            return raw_responses_list
    return []

def calculate_wait_time(positive_responses, total_time):
    if positive_responses == 0:
        return 0 
    return total_time / positive_responses

def process_tracks(sp, tracks):
    if os.path.exists(raw_responses_file_path):
        raw_responses = load_raw_responses()
    else:
        raw_responses = {}
    positive_responses_since_429 = 0
    total_time_since_429 = 0
    wait_time = 120  

    valid_ids = []
    for response in raw_responses:
        if response and 'id' in response:
            valid_ids.append(response['id'])

    pending_tracks = tracks[~tracks['id'].isin(valid_ids)]

    track_ids = pending_tracks['id'].tolist()
    print(f"Processing {len(track_ids)} tracks...")
    for index in range(0, len(track_ids), 50):  
        batch_track_ids = track_ids[index:index + 50]

        try:
            start_time = time.time()
            print(f"Processing {len(batch_track_ids)} tracks, total: {index + len(batch_track_ids)}/{len(track_ids)}")
            batch_responses = sp.tracks(batch_track_ids)
            print("response received")
            save_raw_responses(batch_responses)
            end_time = time.time() 
            positive_responses_since_429 += 1

            total_time_since_429 += end_time - start_time

            print(f"Processed {len(batch_track_ids)} tracks, total: {index + len(batch_track_ids)}/{len(track_ids)}")
            time.sleep(0.5)
        except TimeoutError as te:
            print(f"TimeoutError: {te}. Retrying...")
            time.sleep(wait_time)
            continue  

        except spotipy.SpotifyException as e:
            if e.http_status == 429:  
                print(f"Rate limit exceeded. Waiting before retrying...")
                time.sleep(wait_time) 
            else:
                print(f"Error processing tracks: {e}")
        except Exception as e:
            print(f"Error processing tracks: {e}")
            continue

    if positive_responses_since_429 >= 10:  
        wait_time = calculate_wait_time(positive_responses_since_429, total_time_since_429)
        print(f"Rate limit calculated: {positive_responses_since_429} positive responses in a row.")
        print(f"Setting waiting time to approximately {wait_time:.2f} seconds between retries.")
        time.sleep(wait_time)
        positive_responses_since_429 = 0  
        total_time_since_429 = 0

if __name__ == "__main__":
    sp = authenticate_spotify()

    tracks_df = pd.read_csv(file_path)

    process_tracks(sp, tracks_df)
import requests
import json 
from datetime import date
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable

project_root = Path(__file__).parents[2]

CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
API_KEY = Variable.get("API_KEY")
max_results = 50

@task
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        channel_items = data['items'][0]
        channel_playlistid = channel_items['contentDetails']['relatedPlaylists']['uploads']

        return channel_playlistid
    
    except requests.exceptions.RequestException as e:
        raise e
    
@task    
def get_video_ids(playlist_id):
    video_ids = []
    page_token = None 

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url

            if page_token:
                url += f"&pageToken={page_token}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            page_token = data.get('nextPageToken')

            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_ids, batch_size):
        for video_id in range(0, len(video_ids), batch_size):
            yield video_ids[video_id: video_id + batch_size]
    
    try:
        for batch in batch_list(video_ids, max_results):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails%2C%20snippet%2Cstatistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    'video_id': video_id,
                    'title': snippet['title'],
                    'published_at': snippet['publishedAt'],
                    'duration': contentDetails['duration'],
                    'view_count': statistics.get('viewCount', None),
                    'like_count': statistics.get('likeCount', None),
                    'comment_count': statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)
        
        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def load_to_json(extracted_data, ds: str = None):
    # Airflow automatically injects the logical execution date via 'ds' (YYYY-MM-DD).
    # Using 'ds' ensures idempotency during backfills and avoids cross-midnight date drift.
    target_date = ds if ds else str(date.today())
    output_dir = project_root / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"YT_data_{target_date}.json"
    
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

    print(f"Loaded to:\n{file_path}")
    return str(file_path)

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    load_to_json(video_data)

# Developed by Ahmed Rizk Gawish
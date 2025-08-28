import requests
from dotenv import load_dotenv
import os
import pandas as pd

#####################################################################################
#### Defined class ExtractYouTubeVideos to call YouTube Data API v3, to featch ######
#### all the MV information from a YT Page. The usefull information is stored  ######
#### and can be retrived calling extract_yt_data method, returning a list[dict].#####
#### The extract_yt_data object can be transformed into a pandas DF and saved #######
####  into a file with .csv format. #################################################
#### The methods from this class are called in the run_youtube_pipeline.py ##########

class ExtractYouTubeVideos:
    def __init__(self, user_name : str):
        self.user_name = user_name
        self.api_key = self.get_api_key()
        self.channel_id = self.get_user_id()

    def get_api_key(self) -> str:
        load_dotenv(dotenv_path="../.env")  
        api_key = os.getenv("API_GCLOUD_SWIFT_PROJECT")
        return api_key

    def get_user_id(self) -> str:
        url = f"https://www.googleapis.com/youtube/v3/channels?part=id&forUsername={self.user_name}&key={self.api_key}"
        resp = requests.get(url).json()
        channel_id = resp['items'][0]['id']
        return channel_id
    
    def fetch_yt_data(self, max_results : int = 50) -> dict:
        url = (
            "https://www.googleapis.com/youtube/v3/search"
            f"?key={self.api_key}"
            f"&channelId={self.channel_id}"
            "&part=snippet,id"
            "&order=date"
            "&type=video"
            f"&maxResults={max_results}" # maxResults cannot be greater than 50.
            "&q=official%20music%20video"   #keyword filter
        )
        resp = requests.get(url).json()
        return resp

    def get_video_details_list(self, max_results : int = 50) -> list:
        resp = self.fetch_yt_data(max_results)
        videos = []
        for item in resp["items"]:
            if item["id"]["kind"] == "youtube#video": #Inner kind
                videos.append({
                    "etag" : item["etag"],
                    "id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "published": item["snippet"]["publishedAt"], 
                    "description": item["snippet"]["description"],
                    "link" : f"https://www.youtube.com/watch?v={item["id"]["videoId"]}", 
                    "thumbnails": item["snippet"]["thumbnails"]["high"]["url"]
                })
        return videos
    
    def create_csv(self, output_file_name : str, return_df : bool = False, max_results : int = 50):
        videos_details = self.get_video_details_list(max_results)
        df = pd.DataFrame(videos_details)
        df[:max_results].to_csv(f"{output_file_name}.csv", index=False, index_label=False)
        if return_df:
            return df

# user_name = "TaylorSwift"
# output_path = "../data/"
# output_file_name = "youtube_taylor_last_2_mv"
# output_file_path=f"{output_path}{output_file_name}"


# extract_yt_data = ExtractYouTubeVideos(user_name)
# videos = extract_yt_data.get_video_details_list(2)

# df = extract_yt_data.create_csv(output_file_path, True, 2)
# print(df)
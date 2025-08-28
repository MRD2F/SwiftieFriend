from extract_youtube_videos import ExtractYouTubeVideos
import numpy as np
import pandas as pd


def extract_pipeline(user_name : str, max_videos : int = 50) -> list[dict]: #, output_path : str, output_file_name : str,):
    extract_yt_data = ExtractYouTubeVideos(user_name)
    videos = extract_yt_data.get_video_details_list(max_videos)
    return videos

def save_yt_data(user_name : str, output_path : str, output_file_name : str, max_videos : int = 50):
    output_file_path=f"{output_path}{output_file_name}"

    # if len(info_mv) != df.shape[0]:
    # Exception("Not matching entries from list -> df")
    
    extract_yt_data = ExtractYouTubeVideos(user_name)
    df = extract_yt_data.create_csv(output_file_name=output_file_path, return_df=True, max_results=max_videos)
    print(df)


def clean_yt_data(df):

    # Replace NaN, empty strings, or "NULL" (case-insensitive) with "None"
    df['description'] = df['description'].apply(
        lambda x: "None" if (pd.isna(x) or str(x).strip() == "" or str(x).strip().upper() == "NULL") else x
    )

    #missing values aren’t converted to "nan" but instead to "None"
    df = df.fillna("None").astype(str)
    df = df.astype(str)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace string "None" with real NaN
    df['published'] = df['published'].replace("None", np.nan)

    # Convert to datetime, invalid values will become NaT
    df['published'] = pd.to_datetime(df['published'], errors='coerce')

    return df



# user_name = "TaylorSwift"
# max_videos=2
# print(extract_pipeline(user_name, max_videos))


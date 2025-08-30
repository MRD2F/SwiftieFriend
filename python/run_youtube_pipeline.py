from extract_youtube_videos import ExtractYouTubeVideos
from load_youtube_videos_info import LoadToBigQuery
from google.cloud import bigquery

import numpy as np
import pandas as pd

def extract_pipeline(user_name : str, max_results : int = 50) -> list[dict]: #, output_path : str, output_file_name : str,):
    extract_yt_data = ExtractYouTubeVideos(user_name)
    info_mv = extract_yt_data.get_video_details_list(max_results)
    return info_mv

def save_yt_data(user_name : str, output_path : str, output_file_name : str, save_df : bool = True, max_results : int = 50):
    # Create videos information list
    info_mv = extract_pipeline(user_name, max_results)
    #Convert list into pandas DF and apply cleaning function
    df_info_mv = pd.DataFrame(info_mv)[:max_results]
    df_mv_clean = clean_yt_data(df_info_mv)

    #Save clened DF into a .csv file
    output_file_path=f"{output_path}{output_file_name}"
    if save_df:
        df_mv_clean.to_csv(f"{output_file_path}.csv", index=False, index_label=False)
    return df_mv_clean

def drop_duplicates_with_description(df_ : pd.DataFrame) -> pd.DataFrame:
    """
    Checks if a row with the same title is repeted. In that case if one of those have a description, the one without will be dropped.
    Takes into account cases of videos that were reloadded with a description that was missing.
    """
    df = df_.copy()

    titles_duplicated_with_no_description = df[(df.duplicated(['title'])) & (df['description'] == "No Description Provided.")].title.unique()

    titles_duplicated_with_description = df[(df.title.isin(titles_duplicated_with_no_description)) & (df['description'] != "No Description Provided.")].title.unique()

    indexes_to_drop = df[df.title.isin(titles_duplicated_with_description) & (df['description'] == "No Description Provided.")].index
    df.drop(index=indexes_to_drop, inplace=True)
    return df

def create_mv_flags(df : pd.DataFrame) -> pd.DataFrame:
    # df["title"].apply(lambda x: ("official" in x.lower() and "video" in x.lower()))
    df["official_mv"] = df["title"].astype(str).str.lower().str.contains(r"official.*video")
    df["behind_scenes_mv"] = df["title"].astype(str).str.lower().str.contains(r"behind.*scenes")
    return df


def clean_yt_data(df_ : pd.DataFrame) -> pd.DataFrame:
    df = df_.copy()
    # Replace NaN, empty strings, or "NULL" (case-insensitive) with "None"
    df['description'] = df['description'].apply(
        lambda x: "No Description Provided." if (pd.isna(x) or str(x).strip() == "" or str(x).strip().upper() == "NULL") else x
    )

    #missing values aren’t converted to "nan" but instead to "None"
    df = df.fillna("None").astype(str)
    df = df.astype(str)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    #Added MV flags (offial music video, behind the scenes)
    df = create_mv_flags(df)

    # Replace string "None" with real NaN
    df["published"] = df["published"].replace("None", np.nan)

    # Convert to datetime, invalid values will become NaT
    df["published"] = pd.to_datetime(df["published"], errors='coerce')

    #Keep only duplicated row titles without description
    df = drop_duplicates_with_description(df)

    #Sort by date
    df.sort_values(by='published', ascending=False, inplace=True)

    #Drop duplicates with the oldest publised date
    df.drop_duplicates(['etag'], keep='first', inplace=True)
    df.drop_duplicates(['title'], keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.sort_values(by='published', ascending=False, inplace=True)

    return df

def load_to_bigquery(project_id, dataset_id, table_id, input_path, input_file_name,schema, add_new_records=True):
    load_bq = LoadToBigQuery(project_id, dataset_id, schema, None)
    if add_new_records:
        job = load_bq.add_records_to_table(table_id, input_path, input_file_name)
    else:
        job = load_bq.load_table_from_file_csv(table_id, input_path, input_file_name)
    return job


# def save_yt_data(user_name : str, output_path : str, output_file_name : str, max_videos : int = 50):
#     output_file_path=f"{output_path}{output_file_name}"

#     # if len(info_mv) != df.shape[0]:
#     # Exception("Not matching entries from list -> df")
    
#     extract_yt_data = ExtractYouTubeVideos(user_name)
#     df = extract_yt_data.create_csv(output_file_name=output_file_path, return_df=True, max_results=max_videos)
#     print(df)


if __name__ == "__main__":

    user_name = "TaylorSwift"
    max_videos=10
    output_path = "../data/yt_musics_videos/"
    output_file_name = f"youtube_taylor_last_{max_videos}_mv"
    save_df = True

    df = save_yt_data(user_name, output_path, output_file_name, save_df, max_videos)
    print(df)

    project_id = "swiftie-friend" 
    dataset_id = "social_media"
    table_id= "youtube_music_videos_v5"

    input_path=output_path
    input_file_name=output_file_name
    # input_path="../data/yt_musics_videos/"
    # input_file_name="youtube_taylor_last_10_mv"


    schema = [
        bigquery.SchemaField("etag", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("published", "TIMESTAMP"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("link", "STRING"),
        bigquery.SchemaField("thumbnails", "STRING"),
        bigquery.SchemaField("official_mv", "BOOLEAN"),
        bigquery.SchemaField("behind_scenes_mv", "BOOLEAN")
    ]

    job = load_to_bigquery(project_id, dataset_id, table_id, input_path, input_file_name,schema, add_new_records=True)


#info_mv = extract_pipeline(user_name, max_videos)
# df = clean_yt_data(pd.DataFrame(info_mv))
# print(df.description.iloc[0])
# print(type(df.published.iloc[0]))
# print(df[['description', 'published']])



# print(df.dtypes)
# print(df)

# user_name = "TaylorSwift"
# max_videos=2
# print(extract_pipeline(user_name, max_videos))


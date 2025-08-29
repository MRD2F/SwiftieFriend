from extract_youtube_videos import ExtractYouTubeVideos
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


# def save_yt_data(user_name : str, output_path : str, output_file_name : str, max_videos : int = 50):
#     output_file_path=f"{output_path}{output_file_name}"

#     # if len(info_mv) != df.shape[0]:
#     # Exception("Not matching entries from list -> df")
    
#     extract_yt_data = ExtractYouTubeVideos(user_name)
#     df = extract_yt_data.create_csv(output_file_name=output_file_path, return_df=True, max_results=max_videos)
#     print(df)


#EXAMPLE

user_name = "TaylorSwift"
max_videos=50
output_path = "../data/yt_musics_videos/"
output_file_name = f"youtube_taylor_last_{max_videos}_mv"
save_df = True

#info_mv = extract_pipeline(user_name, max_videos)
# df = clean_yt_data(pd.DataFrame(info_mv))
df = save_yt_data(user_name, output_path, output_file_name, save_df, max_videos)
print(df)
# print(df.description.iloc[0])
# print(type(df.published.iloc[0]))
# print(df[['description', 'published']])



# print(df.dtypes)
# print(df)

# user_name = "TaylorSwift"
# max_videos=2
# print(extract_pipeline(user_name, max_videos))


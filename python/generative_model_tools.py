import os
from rapidfuzz import fuzz
from google import genai
from google.genai import types
from dotenv import load_dotenv

from .transform import split_by_capitals, space_song_names

class GenerativeModelTools:
    """tools = [get_complete_lyrics, get_album_songs, 
            get_song_match, query_collection, classify_mood,
            get_best_match_name, get_database_info]
    """
    def __init__(self, collection_db, sentiment_generative_model="gemini-1.5-flash-8b"):
        self.vectordb = collection_db.get_collection()
        self.album_songs_summary = collection_db.load_song_summary()
        self.sentiment_generative_model = sentiment_generative_model

        def get_google_api_key():
            load_dotenv()
            GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY_SWIFTIE")
            if not GOOGLE_API_KEY:
                raise ValueError("Missing GOOGLE_API_KEY_SWIFTIE in .env") 
            return GOOGLE_API_KEY

        self.client = genai.Client(api_key=get_google_api_key())


    def get_database_info(self):
        return self.album_songs_summary

    def get_best_match_name(self, name : str, threshold : int = 87) -> list:
        song_name = space_song_names(name)
        matches=[]
        song_album={}
        for album, stored_songs in self.album_songs_summary.items():
            for stored_song in stored_songs:
                stored_song_spaced = space_song_names(stored_song)
                score = fuzz.partial_ratio(song_name, stored_song_spaced)
                if score >= threshold:
                    matches.append(stored_song)
                    song_album[stored_song] = album
        return matches, song_album


    def get_complete_lyrics(self, song_name : str, album_name: str = "", threshold : int = 87) -> dict:
        """ returns a dictionary with the most correlated songs matching the song_name requested"""
        matches, song_album = self.get_best_match_name(song_name, threshold)
        
        songs = {}
        #print(f'The best matches for the requested song "{song_name}" are: ')
        for song, album in song_album.items():
            #print(f'song: "{space_song_names(song_name)}" from album : "{space_song_names(album)}"')
            results = self.vectordb._collection.get(
                where={
                    "$and": [
                        {"song": {"$eq": song}},
                        {"album": {"$eq": album}}
                    ]
                }
            )
            songs[song] = results['documents']
        return songs

    def get_album_songs(self, album_name: str, threshold : int = 90):
        matches_albums = []
        album_tracks = {}
        for stored_album in self.album_songs_summary.keys():
            stored_album_spaced = space_song_names(stored_album)
            score = fuzz.partial_ratio(stored_album_spaced, space_song_names(album_name))
            if score >= threshold:
                matches_albums.append(stored_album)

        n = len(album_name.split(' '))
        matches_albums = list(set(matches_albums))
        for album in matches_albums:
            # print(n, split_by_capitals(album), space_song_names(album_name).lower())
            if (n == 1) & (space_song_names(album_name).lower() in split_by_capitals(album)):
                album_tracks[space_song_names(album)] = [space_song_names(i) for i in self.album_songs_summary[album]]
            elif (n > 1):
                album_tracks[space_song_names(album)] = [space_song_names(i) for i in self.album_songs_summary[album]]
                

        return album_tracks

    def get_song_match(self, query : str, n_results : int = 10):
        """Finds the most relevant lyrics song based on the query."""
        #  retriever = vectordb.as_retriever(search_type="mmr", search_kwargs={"k": n_results, "filter": {"album": "Lover"} })
        retriever = self.vectordb.as_retriever(search_type="mmr", search_kwargs={"k": n_results })

        docs = retriever.invoke(query)    
        print(docs)

        songs_info = {}
        for doc in docs:
            song_text = doc.page_content
            song_name = doc.metadata["song"]
            album_name = doc.metadata["album"]
            if song_name in songs_info:
                #print(f"Duplicate song found: {song_name}")
                #  print(len(songs_info[song_name]["lyrics"]))
                songs_info[song_name]["lyrics"] += "\n" + song_text
                #  print(len(songs_info[song_name]["lyrics"]))
                continue
            songs_info[song_name] = {
                "song_name": song_name,
                "lyrics": song_text,
                "album" : album_name
            }
        return songs_info

    def query_collection(self, matadata_label, value, return_lyrics=False):
        metadata = {matadata_label: value}
        results = self.vectordb._collection.get(where=metadata)
        if return_lyrics:
            return results['documents']
        else:
            return results

    def classify_mood(self, query: str) -> str:

        system_prompt = (
        "You are an intelligent query router for a chatbot. "
        "Your task is to classify user queries or song text based on the sentiment or mood expressed. "
        "Possible categories include: 'sad', 'happy', 'melancholy', 'dance', 'revenge', 'mad', 'weepy', 'depressed', 'charmed', 'joyful', 'celebrate', 'in love', 'angry', 'joy', 'gratitude', 'serenity', 'anxiety', 'resentment', 'despair'.\n\n"
        "Use the following rules:\n"
        "1. Analyze the emotional tone of the text (user query or lyrics).\n"
        "2. Select only **one or more** mood label that best captures the primaries emotional intents.\n"
        "3. If the text expresses multiple emotions, choose the most dominant or consistent ones.\n"
        "4. Use your understanding of natural language and human emotions to infer implicit mood where it's not obvious.\n"
        "5. Respond with the label only — do not include explanations or extra commentary.\n\n"
        "Examples:\n"
        "Input: 'Why did you leave me? Everything reminds me of you.' → Output: sad, angry, melancholy \n"
        "Input: 'I just met someone new and I can’t stop smiling!' → Output: in love, happy, joyful \n"
        "Input: 'This beat makes me want to dance all night!' → Output: dance\n"
        "Input: 'We’re gonna burn it all down, no mercy!' → Output: angry\n"
        "Input: 'I won, and they all doubted me.' → Output: revenge, resentment\n"
        "Input: 'Just got a promotion, let’s celebrate!' → Output: celebrate, dance, joyful \n"
        "Input: 'Walking alone in the rain, thinking of old times.' → Output: melancholy, sad, anxiety, despair \n"
    )

        chat = self.client.chats.create(model=self.sentiment_generative_model, 
                            config=types.GenerateContentConfig(
                            system_instruction=system_prompt
                        ))

        response = chat.send_message(query)
        return response.text

    # classify_mood('oh oh I am falling in love')
    
# if __name__ == "__main__":

#     from load import LoadCollectionDB

#     collection_name = "taylor_songs_collection"
#     file_summary_songs = "album_songs_summary"
#     load = LoadCollectionDB(collection_name, file_summary_songs)
#     tools = GenerativeModelTools(load)
#     print(tools.classify_mood('oh oh I am falling in love')
# )
    
    #print(tools.get_best_match_name('style'))

    #print(tools.classify_mood('I love revenge'))
    #print(tools.album_songs_summary)
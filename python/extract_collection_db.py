import json
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
  
class ExtractCollectionDB:
    def __init__(self, collection_name, file_summary_songs, data_path="../data/", vectordb_file_name="chroma_db"):
        self.collection_name = collection_name
        self.vectordb_file_name= vectordb_file_name
        self.data_path = data_path
        self.file_summary_songs = file_summary_songs

    def get_collection(self):
        model_name = "sentence-transformers/all-mpnet-base-v2"
        huggingface_embedding = HuggingFaceEmbeddings(model_name=model_name)
        
        vectordb = Chroma(
            persist_directory=f"{self.data_path}{self.vectordb_file_name}",
            collection_name=self.collection_name,
            embedding_function=huggingface_embedding
        )
        return vectordb

    def load_song_summary(self):
        with open(f"{self.data_path}{self.file_summary_songs}", 'r') as f:
            album_songs_summary = json.load(f)
        return album_songs_summary

# if __name__ == "__main__":
#     collection_name = "taylor_songs_collection"
#     file_summary_songs = "album_songs_summary"
#     load = ExtractCollectionDB(collection_name, file_summary_songs)
#     vectordb = load.get_collection()
#     album_songs_summary = load.load_song_summary()
#     #print(vectordb.get())
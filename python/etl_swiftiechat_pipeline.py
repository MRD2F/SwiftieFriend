from extract_collection_db  import ExtractCollectionDB
from transform import *
from generative_model_tools import GenerativeModelTools
from swiftie_chat import SwiftieChat

from IPython.display import Markdown, display


"""Extract → Several classes that query a local database. ✅

Transform → Cleaning the dataset (normalize text, remove duplicates, etc.). ✅

Load → Feed the processed data into a script that performs RAG operations (embedding the text, storing vectors, retrieving context for LLM). ✅

This is a classic ETL pattern but instead of ending in a database or BI system, your “load” step feeds into a vector store or RAG pipeline, which is still consistent with the concept.
"""
class ETLSwiftieChatPipeline:
    def __init__(self, collection_name, file_summary_songs, swiftie_chat_instructions, data_path="../data/", vectordb_file_name="chroma_db"):
        self.data_path = data_path
        self.vectordb_file_name= vectordb_file_name
        self.collection_db = ExtractCollectionDB(collection_name, file_summary_songs, data_path, vectordb_file_name)
        self.genrative_model_tools = GenerativeModelTools(self.collection_db)
        self.swiftie_chat = SwiftieChat(self.genrative_model_tools, swiftie_chat_instructions)
 

    def get_gradio_interface(self):
        gradio_interface = self.swiftie_chat.create_gradio_interface()
        return gradio_interface
    
    def lauch_gradio_interface(self, share=False):
        gradio_interface = self.get_gradio_interface()
        gradio_interface.launch(share)
    
    def ask_a_swiftie_with_history(self, user_message, history=None):
        response = self.swiftie_chat.ask_a_swiftie_with_history(user_message, history)
        return response

    def classify_mood(self, text):
        mood = self.genrative_model_tools.classify_mood(text)
        return mood
    

if __name__ == "__main__":

    collection_name = "taylor_songs_collection"
    file_summary_songs = "album_songs_summary"
    swiftie_chat_instructions = "../config/swiftie_chat_system_instructions.txt"
    etl_assistant_pipeline = ETLSwiftieChatPipeline(collection_name, file_summary_songs, swiftie_chat_instructions)
    #print(etl_assistant_pipeline.ask_a_swiftie_with_history("Give me the lyrics to 'All Too Well',give me the most emotional part"))
    #print(etl_assistant_pipeline.classify_mood('oh oh I am falling in love'))
    etl_assistant_pipeline.lauch_gradio_interface(share=True)

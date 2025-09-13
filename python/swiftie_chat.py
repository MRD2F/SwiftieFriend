from google import genai
from google.genai import types
from dotenv import load_dotenv
from IPython.display import Markdown, display
import os
from load import LoadCollectionDB
from generative_model_tools import GenerativeModelTools

class SwiftieChat:
    def __init__(self, genrative_model_tools, system_instructions, gen_model_name="gemini-1.5-flash-8b"):
        self.gen_tools = genrative_model_tools
        self.client = self.gen_tools.client
        self.gen_model_name = gen_model_name
        self.chat_session = None
        #self.system_instructions = system_instructions

        with open(system_instructions, "r", encoding="utf-8") as f:
            self.system_instructions = f.read()
        self.tools = [
                        self.gen_tools.get_complete_lyrics, self.gen_tools.get_album_songs, 
                        self.gen_tools.get_song_match, self.gen_tools.query_collection, self.gen_tools.classify_mood,
                        self.gen_tools.get_best_match_name, self.gen_tools.get_database_info
        ]

    def create_swiftie_chat(self):    
        chat = self.client.chats.create(model=self.gen_model_name, 
                        config=types.GenerateContentConfig(
                        system_instruction=self.system_instructions
                    ))

        return chat


    def ask_a_swiftie_with_history(self, user_message):
        global chat_session

        #if self.chat_session is None:
        chat_session = self.create_swiftie_chat()

        response = chat_session.send_message(user_message)
        
        return response.text

if __name__ == "__main__":
    collection_name = "taylor_songs_collection"
    file_summary_songs = "album_songs_summary"
    load = LoadCollectionDB(collection_name, file_summary_songs)
    tools = GenerativeModelTools(load)
    print(tools.classify_mood('oh oh I am falling in love'))
          
    swiftie_chat = SwiftieChat(tools, "../config/swiftie_chat_system_instructions.txt") 
    response = swiftie_chat.ask_a_swiftie_with_history("Can you give me a summary of Taylor Swift's albums?")
    print(response)
                               
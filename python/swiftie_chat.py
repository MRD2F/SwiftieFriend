from google.genai import types

# from IPython.display import Markdown, display
import gradio as gr

# model selection: https://research.aimultiple.com/llm-pricing/


class SwiftieChat:
    def __init__(
        self,
        genrative_model_tools,
        system_instructions,
        gen_model_name="gemini-1.5-flash-8b",
    ):
        self.gen_tools = genrative_model_tools
        self.client = self.gen_tools.client
        self.gen_model_name = gen_model_name

        with open(system_instructions, "r", encoding="utf-8") as f:
            self.system_instructions = f.read()

        self.tools = [
            self.gen_tools.get_complete_lyrics,
            self.gen_tools.get_album_songs,
            self.gen_tools.get_song_match,
            self.gen_tools.classify_mood,
            self.gen_tools.get_best_match_name,
            self.gen_tools.get_database_info,
        ]  # self.gen_tools.query_collection,

        self.chat_session = self.create_swiftie_chat()

    def create_swiftie_chat(self):
        chat = self.client.chats.create(
            model=self.gen_model_name,
            config=types.GenerateContentConfig(
                system_instruction=self.system_instructions, tools=self.tools
            ),
        )

        return chat

    # gr.ChatInterface passes two arguments to the function ask_a_swiftie_with_history
    # fn(message: str, history: list[tuple[str, str]])
    def ask_a_swiftie_with_history(self, user_message, history):
        # chat_session = self.create_swiftie_chat()

        # response = chat_session.send_message(user_message)
        response = self.chat_session.send_message(user_message)

        return response.text

    def create_gradio_interface(self):

        gradio_interface = gr.ChatInterface(
            fn=self.ask_a_swiftie_with_history,
            title="Your Swiftie BFF 💖",
            description="Ask me anything about Taylor Swift songs, albums, moods or lyrics!",
            textbox=gr.Textbox(
                type="text",
                placeholder="Ask me something like 'Give me a sad song like illicit affairs'... "
                "you know we talk a secret language we can't speak with anyone else",
            ),
            theme="soft",
            examples=[
                "What album is 'Enchanted' from?",
                "I’m in love and want something romantic",
                "Give me the lyrics to 'All Too Well'",
            ],
        )
        return gradio_interface


# if __name__ == "__main__":
#     from extract_collection_db import ExtractCollectionDB
#     from generative_model_tools import GenerativeModelTools

#     collection_name = "taylor_songs_collection"
#     file_summary_songs = "album_songs_summary"
#     collection_db = ExtractCollectionDB(collection_name, file_summary_songs)
#     tools = GenerativeModelTools(collection_db)
#     print(tools.classify_mood('oh oh I am falling in love'))

#     swiftie_chat = SwiftieChat(tools, "../config/swiftie_chat_system_instructions.txt")
#     response = swiftie_chat.ask_a_swiftie_with_history("Give me the lyrics to 'All Too Well',give me the most emotional part")
#     print(response)
#     swiftie_chat.create_gradio_interface().launch()

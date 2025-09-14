from huggingface_hub import InferenceClient

import sys
sys.path.append("python/")
from etl_swiftiechat_pipeline import ETLSwiftieChatPipeline  

"""
For more information on `huggingface_hub` Inference API support, please check the docs: https://huggingface.co/docs/huggingface_hub/v0.22.2/en/guides/inference
"""
client = InferenceClient("HuggingFaceH4/zephyr-7b-beta")

collection_name = "taylor_songs_collection"
file_summary_songs = "album_songs_summary"
vectordb_file_name="chroma_db"
data_path="data/"
swiftie_chat_instructions = "config/swiftie_chat_system_instructions.txt"
etl_assistant_pipeline = ETLSwiftieChatPipeline(collection_name, file_summary_songs, swiftie_chat_instructions, data_path, vectordb_file_name)

if __name__ == "__main__":
    # print(etl_assistant_pipeline.ask_a_swiftie_with_history("Give me the lyrics to 'All Too Well',give me the most emotional part"))
    etl_assistant_pipeline.lauch_gradio_interface()
def test_imports():
    import gradio

    assert True


def test_imports_modules():

    from python.extract_collection_db import ExtractCollectionDB
    from python.transform import (
        normalize_lyrics,
        clean_text,
        remove_symbols,
        split_by_capitals,
        space_song_names,
    )
    from python.generative_model_tools import GenerativeModelTools
    from python.swiftie_chat import SwiftieChat
    from python.etl_swiftiechat_pipeline import ETLSwiftieChatPipeline

    assert True


def test_etl_swiftiechat_pipeline_initialization():
    from python.etl_swiftiechat_pipeline import ETLSwiftieChatPipeline

    collection_name = "taylor_songs_collection"
    file_summary_songs = "album_songs_summary"
    swiftie_chat_instructions = "config/swiftie_chat_system_instructions.txt"
    data_path = ("/data/",)
    vectordb_file_name = "chroma_db"

    etl_pipeline = ETLSwiftieChatPipeline(
        collection_name,
        file_summary_songs,
        swiftie_chat_instructions,
        data_path,
        vectordb_file_name,
    )

    assert etl_pipeline.collection_db is not None
    assert etl_pipeline.genrative_model_tools is not None
    assert etl_pipeline.swiftie_chat is not None


def test_gradio_interface_runs():
    from python.etl_swiftiechat_pipeline import ETLSwiftieChatPipeline

    collection_name = "taylor_songs_collection"
    file_summary_songs = "album_songs_summary"
    swiftie_chat_instructions = "config/swiftie_chat_system_instructions.txt"
    data_path = ("/data/",)
    vectordb_file_name = "chroma_db"

    etl_pipeline = ETLSwiftieChatPipeline(
        collection_name,
        file_summary_songs,
        swiftie_chat_instructions,
        data_path,
        vectordb_file_name,
    )
    gradio_interface = etl_pipeline.get_gradio_interface()
    assert gradio_interface is not None

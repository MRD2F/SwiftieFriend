---
title: SwiftieFriend
emoji: 💬
colorFrom: yellow
colorTo: purple
sdk: gradio
sdk_version: 5.0.1
app_file: app.py
pinned: false
short_description: 'Swiftie Generative AI Bot '
---

# SwiftieFriend

**SwiftieFriend** is a chatbot assistant dedicated exclusively to exploring and understanding **Taylor Swift's** music. Powered by the Gemini model and a comprehensive dataset containing the lyrics of all Taylor Swift albums, this assistant offers fans a unique way to interact with her songs, albums, moods, and more — all via an intuitive Gradio interface.

---

## About

You are interacting with a smart assistant designed specifically for Taylor Swift fans. The assistant is built to help users explore Taylor Swift’s songs and albums by querying a local database of lyrics and metadata.

The assistant follows this core instruction:  
*“You are a smart assistant that helps users explore and understand Taylor Swift’s songs and albums using a local database. You have access to tools that let you retrieve lyrics, match songs to moods, or identify albums. Choose the most relevant tool to satisfy the user query.”*

---

## Features

The assistant supports the following capabilities:

1. **Retrieve Lyrics:** Get the full lyrics of a song by providing its name.
2. **Album Song List:** Return the list of songs contained in a given album.
3. **Song Match:** Find the song that best matches a specific user query or request.
4. **Song Albums:** List all albums that contain a given song.
5. **Mood Classification:** Classify and describe the mood of a specific song.
6. **Album Similarity:** Retrieve all albums that contain songs similar to a user’s request.
7. **Similar Songs:** Show all songs similar to the query, including remixes or live TV performances.

---

## Dataset

The project is built upon a dataset containing the complete lyrics of Taylor Swift's songs across all her albums.

---

## Technology Stack

- **Model:** Gemini (Google's state-of-the-art language model)
- **Interface:** Gradio — for an easy and interactive web-based UI
- **Backend:** Local database storing lyrics, album info, and song metadata

---

## Installation & Usage

1. Clone the repository:
   ```bash
   git clone https://github.com/MRD2F/SwiftieFriend.git
   cd SwiftieFriend

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt

3. Run the Gradio App:
   ```bash
   python app.py

4. Open your browser at the provided local URL to interact with SwiftieFriend!

---

## How to Use

- Enter song names, album names, or queries about Taylor Swift's music.
- The assistant will provide lyrics, song lists, mood analysis, or suggest songs based on your input.
- Explore Taylor Swift’s discography in a fun, interactive way!

---

## Contributing

Feel free to open issues or submit pull requests to improve the bot's knowledge base, interface, or features.

---

## License

This project is licensed under the MIT License.

---

## Contact

Created by Maria Rosaria Di Domenico — passionate Swiftie and developer.  
Feel free to reach out for collaboration or questions!

---

Enjoy diving deep into Taylor Swift’s music with **SwiftieFriend**! 🎤🎸✨
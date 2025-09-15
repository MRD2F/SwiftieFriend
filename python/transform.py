import re
import wordninja


def normalize_lyrics(text):
    text = re.sub(r"\[.*?post-chorus.*?\]", "[Post-Chorus]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?chorus.*?\]", "[Chorus]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?verse.*?\]", "[Verse]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?bridge.*?\]", "[Bridge]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?into.*?\]", "[Intro]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?interlude.*?\]", "[Interlude]", text, flags=re.IGNORECASE)
    text = re.sub(r"\[.*?outro.*?\]", "[Outro]", text, flags=re.IGNORECASE)
    return text


def clean_text(text):
    text = re.sub(r"[\u2000-\u200A\u202F\u205F\u3000]", " ", text)
    text = normalize_lyrics(text)
    # text  = re.sub(r"\\'", "", text)
    return text


def remove_symbols(s):
    return re.sub(r"[-?_,.]", "", s)


def split_by_capitals(s):
    # Creates keyword list with the words
    s = remove_symbols(s)
    # n_cap_letters = len(s)
    keywords = [i.lower() for i in re.findall(r"[A-Z][^A-Z]*", s)]
    if len(keywords) == 1:
        return keywords
    else:
        return [i.lower() for i in wordninja.split(s)]


def space_song_names(s):
    # Joins the cleaned keywords
    # Convert LavanderHaze -> Lavander Haze , Anti-Hero -> anti hero
    return " ".join(i for i in split_by_capitals(s))

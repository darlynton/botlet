def split_message(text, max_length=1500):
    return [text[i:i+max_length] for i in range(0, len(text), max_length)]
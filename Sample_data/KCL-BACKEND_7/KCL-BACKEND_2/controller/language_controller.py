from deep_translator import GoogleTranslator

def translate_message(text, lang):
    try:
        if lang == "en":
            return text
        return GoogleTranslator(source="en", target=lang).translate(text)
    except Exception:
        return text  # fallback to English


def translate_dict(data, lang):
    try:
        if lang == "en":
            return data

        translated = {}
        for key, value in data.items():
            if isinstance(value, str):
                translated[key] = GoogleTranslator(source="en", target=lang).translate(value)
            else:
                translated[key] = value  # non-string values remain same
        return translated
    except Exception:
        return data

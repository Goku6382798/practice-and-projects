from deep_translator import GoogleTranslator

def translate_message(text, lang):
    try:
        if lang == "en":
            return text
        return GoogleTranslator(source="en", target=lang).translate(text)
    except Exception:
        return text  # fallback to English


import os
import requests

def generate_response(user_input, config):
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": os.getenv("GEMINI_API_KEY")
    }

    payload = {
        "contents": [
            {"parts": [{"text": f"{config.get('persona_intro', '')}\n\nUser: {user_input}"}]}
        ]
    }

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
    except Exception as e:
        print(f"Error calling Gemini API or decoding JSON: {e}")
        return "Sorry, I couldn't process your request."

    print(f"Gemini API response: {data}")

    if "candidates" in data and len(data["candidates"]) > 0:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    else:
        print("No 'candidates' key found in Gemini response.")
        return "Sorry, I couldn't generate a response."
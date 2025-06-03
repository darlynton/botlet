import os
import requests

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID")

def send_message(to_number, message):
    if not META_ACCESS_TOKEN or not META_PHONE_NUMBER_ID:
        return {"status": "error", "message": "Missing META_ACCESS_TOKEN or META_PHONE_NUMBER_ID"}

    url = f"https://graph.facebook.com/v19.0/{META_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {
            "body": message
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        return response.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}
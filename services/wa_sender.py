# services/meta_sender.py
import requests
import os
import json

def send_meta_whatsapp_message(to_number, message_body):
    # Ensure these environment variables are correctly set in your .env
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
    PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID") # Your WhatsApp Phone Number ID from Meta Dashboard

    if not META_ACCESS_TOKEN or not PHONE_NUMBER_ID:
        print("Error: META_ACCESS_TOKEN or PHONE_NUMBER_ID environment variables are not set for Meta API.")
        return {'status': 'error', 'message': 'Meta API configuration missing'}

    url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "text",
        "text": {
            "body": message_body
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        response_json = response.json()
        print(f"Meta API message sent successfully: {response_json}")
        return {'status': 'success', 'data': response_json}
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error sending Meta API message: {e}")
        error_details = {'status': 'error', 'message': f"HTTP Error: {e.response.text}"}
        if e.response:
            try:
                error_data = e.response.json()
                print(f"Meta API Error Response: {json.dumps(error_data, indent=2)}")
                error_details['meta_error'] = error_data
            except json.JSONDecodeError:
                print(f"Meta API Error Response (raw): {e.response.text}")
        return error_details
    except requests.exceptions.RequestException as e:
        print(f"General Request Error sending Meta API message: {e}")
        return {'status': 'error', 'message': f"Request Error: {e}"}
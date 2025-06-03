# WhatsApp AI Chatbot

A modular, customizable WhatsApp chatbot using Flask, WaSenderAPI, and Gemini AI.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Add your Gemini API key to `.env`

3. Run the app:
   ```bash
   python app.py
   ```

4. Expose to internet using ngrok or deploy to Railway/Render

## Config

Edit `config/bots.json` to customize the bot's personality.



whatsapp-bot/
├── app.py                  # Flask webhook listener
├── config/
│   └── bots.json           # Persona & behavior settings for each bot
├── services/
│   ├── wa_handler.py       # WhatsApp message handling (WaSenderAPI)
│   ├── ai_engine.py        # Gemini/OpenAI abstraction
│   └── message_utils.py    # Message formatting, chunking
├── .env                    # API keys, secrets
├── requirements.txt
└── README.md
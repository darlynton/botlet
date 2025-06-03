from google.cloud import aiplatform # New import for Vertex AI SDK
from vertexai.generative_models import GenerativeModel # Add this line
import os

class GeminiClient:
    def __init__(self, project_id, location="us-central1"):
        # Initialize Vertex AI client
        # It will automatically pick up GOOGLE_APPLICATION_CREDENTIALS
        # or gcloud auth (if that ever works for you)
        aiplatform.init(project=project_id, location=location)

        # Load the generative model
        # Check Vertex AI documentation for the exact model name you need
        # common ones are 'gemini-pro', 'gemini-pro-vision', 'gemini-1.5-flash', 'gemini-1.5-pro'
        self.model = GenerativeModel("gemini-2.5-flash-preview-05-20") 

    def generate_content(self, prompt_text):
        try:
            # Use the generate_content method from the Vertex AI SDK
            response = self.model.generate_content(prompt_text)
            # The structure of the response might differ slightly from google-genai
            # You'll usually access the text via .text or check .parts
            return response.text
        except Exception as e:
            return f"Error generating content: {e}"
import os

class Config:
    def __init__(self):
        self.TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.BQ_PROJECT = os.environ.get("BQ_PROJECT")
        self.BQ_LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast1")
        self.APF_ALLOWED = {"TH", "PH", "BD", "PK", "BR", "MX", "CO"}
        
        if not self.TELEGRAM_TOKEN:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment")
        if not self.BQ_PROJECT:
            raise RuntimeError("Missing BQ_PROJECT in environment")

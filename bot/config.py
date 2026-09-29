import os
import logging

logger = logging.getLogger(__name__)

# Kura data warehouse (see the header of every file under sql/):
#   data project  kz-kura   -> datasets prod_dw / int_dw, location US
#   job project   kz-dp-ops -> the bot identity runs its jobs here
#                              (it has no jobs.create on kz-kura)
KURA_DEFAULT_LOCATION = "US"


class Config:
    def __init__(self):
        self.TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
        # Job project the queries run in (billing / jobs.create), e.g. kz-dp-ops.
        self.BQ_PROJECT = os.environ.get("BQ_PROJECT")
        # Location of the datasets being queried. Kura lives in US.
        self.BQ_LOCATION = os.environ.get("BQ_LOCATION", KURA_DEFAULT_LOCATION)
        self.APF_ALLOWED = {"TH", "PH", "BD", "PK", "BR", "MX", "CO"}

        if not self.TELEGRAM_TOKEN:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment")
        if not self.BQ_PROJECT:
            raise RuntimeError("Missing BQ_PROJECT in environment (job project, e.g. kz-dp-ops)")
        if self.BQ_LOCATION.upper() != KURA_DEFAULT_LOCATION:
            logger.warning(
                "BQ_LOCATION=%s but the Kura datasets (kz-kura.prod_dw / int_dw) live in US; "
                "queries will fail with 'dataset not found' unless BQ_LOCATION=US",
                self.BQ_LOCATION,
            )

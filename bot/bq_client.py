# bq_client.py
from google.cloud import bigquery
import logging
import pandas as pd
import re

logger = logging.getLogger(__name__)
# directory = "//home//ubuntu//sql"
directory = ".//sql"
class BigQueryClient:
    def __init__(self, config):
        self.config = config
        self._sql_cache: dict[str, str] = {}
        self.client = bigquery.Client(
            project=config.BQ_PROJECT, 
            location=config.BQ_LOCATION
        )
                # ▼▼▼ NEW: LOAD BRAND MAPPING CSV AT STARTUP ▼▼▼
        try:
            mapping_path = f"{directory}//brand_mapping.csv"
            self.brand_mapping_df = pd.read_csv(mapping_path)
            # Ensure the 'brand' column is lowercase for consistent joining
            self.brand_mapping_df['brand'] = self.brand_mapping_df['brand'].str.upper()
            logger.info("Successfully loaded brand_mapping.csv")
        except FileNotFoundError:
            logger.error("FATAL: brand_mapping.csv not found! The bot may not function correctly.")
            self.brand_mapping_df = pd.DataFrame() # Create empty df to avoid errors

    def _load_sql(self, filename: str) -> str:
        """Load and cache SQL templates from disk."""
        cached = self._sql_cache.get(filename)
        if cached is not None:
            return cached
        path = f"{directory}//{filename}"
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        self._sql_cache[filename] = sql
        return sql
        
    async def execute_apf_query(self, target_country):
        sql = self._load_sql("apf_function.sql")
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_country", "STRING", target_country)
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            result = query_job.result()
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    # ▼ NEW: for /dist
    async def execute_dist_query(
        self,
        target_date: str,
        selected_country: str | None,
        selected_pgw: str | None = None,
    ):
        """
        Distribution (channels by country) for an EXACT local date (Asia/Bangkok).
        Params:
        - target_date: 'YYYY-MM-DD'
        - selected_country: STRING or None
        - selected_pgw: STRING prefix (e.g., 'DPP') or None
        """
        sql = self._load_sql("dist_function.sql")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
                bigquery.ScalarQueryParameter("selected_country", "STRING", selected_country),
                bigquery.ScalarQueryParameter("selected_pgw", "STRING", selected_pgw),
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            result = query_job.result()
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error executing /dist query: {e}")
            raise

    async def execute_dpf_query(
        self,
        target_country: str | None,
        selected_pgw: str | None = None,
    ):
        """
        Deposit Performance (DPF): last 3 local days.
        Current local day is capped at local "now"; previous days are full-day totals.
        Optional filter by country (TH/PH/BD/PK/ID) when target_country is provided.
        Optional filter by PGW method prefix (e.g., DPP).
        """
        sql = self._load_sql("dpf_function.sql")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_country", "STRING", target_country),
                bigquery.ScalarQueryParameter("selected_pgw", "STRING", selected_pgw),
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            result = query_job.result()
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error executing /dpf query: {e}")
            raise

    async def execute_dpf_yesterday_full_totals(
        self,
        target_country: str | None,
        selected_pgw: str | None = None,
    ):
        """
        Returns full completed deposit totals of local yesterday by country.
        This is used as baseline for DPP estimation sentences.
        """
        sql = self._load_sql("dpf_yesterday_full_function.sql")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_country", "STRING", target_country),
                bigquery.ScalarQueryParameter("selected_pgw", "STRING", selected_pgw),
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            result = query_job.result()
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error executing /dpf yesterday-full query: {e}")
            raise

    async def execute_usage_last_3_days(self):
        """
        Returns last 3 local days BigQuery usage for the current bot identity:
        - query_count
        - total_gb (billed bytes in GB)
        """
        sql = self._load_sql("usage_function.sql")

        # Guard identifier injection when replacing INFORMATION_SCHEMA path parts.
        if not re.fullmatch(r"[A-Za-z0-9_-]+", str(self.config.BQ_PROJECT or "")):
            raise ValueError("Invalid BQ_PROJECT format")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", str(self.config.BQ_LOCATION or "")):
            raise ValueError("Invalid BQ_LOCATION format")

        sql = sql.replace("__PROJECT_ID__", self.config.BQ_PROJECT)
        sql = sql.replace("__BQ_LOCATION__", self.config.BQ_LOCATION)

        try:
            query_job = self.client.query(sql)
            result = query_job.result()
            rows = [dict(row) for row in result]
            return rows[:3]
        except Exception as e:
            logger.error(f"Error executing /usage query: {e}")
            raise

    async def execute_pmh_query(self, target_date: str, selected_country: str | None) -> list[dict]:
        """
        Executes the Payment Health query for a specific date and optional country.
        """
        sql = self._load_sql("pmh_function.sql")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
                bigquery.ScalarQueryParameter("selected_country", "STRING", selected_country),
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            # results = [dict(row) for row in query_job.result()]
        
            df = query_job.to_dataframe()
            # print(df.head(5))
            # print(self.brand_mapping_df.head(5))
            df_final = df.merge(self.brand_mapping_df, how = "left")

            results = df_final.to_dict(orient='records')
            logger.debug("/pmh rows=%s", len(results))
            return results
        except Exception as e:
            # CORRECTED LOG MESSAGE
            logger.error(f"Error executing /pmh query: {e}")
            raise   
        
    # in bq_client.py
    async def execute_pmh_week_query(self, as_of_date: str, selected_country: str | None) -> list[dict]:
        sql = self._load_sql("pmh_week_function.sql")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("as_of_date", "DATE", as_of_date),
                bigquery.ScalarQueryParameter("selected_country", "STRING", selected_country),
            ]
        )
        try:
            query_job = self.client.query(sql, job_config=job_config)
            df = query_job.to_dataframe()

            # keep your mapping behavior (brand upper)
            df["brand"] =  df["brand"].str.upper().str.strip()
            df_final = df.merge(self.brand_mapping_df, how="left")

            missing = df_final[df_final["group_name"].isna()]["brand"].dropna().unique()
            if len(missing) > 0:
                logger.warning("Missing group_name mapping for brands: %s", ", ".join(map(str, missing)))
            return df_final.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Error executing /pmh_week query: {e}")
            raise

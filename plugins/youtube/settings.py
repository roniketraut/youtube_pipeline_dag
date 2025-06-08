import os
import pandas as pd
import requests
import logging
logging.basicConfig(format = '%(levelname)s: %(message)s', level=logging.DEBUG)
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
api_key = os.getenv("YT_API_KEY")
print(api_key)

youtube = build("youtube", "v3", developerKey=api_key)

channels = {"MrBeast": "UCX6OQ3DkcsbYNE6H8uQQuVA", "T-Series": "UCq-Fj5jknLsUf-MWSy4_brA", "Cocomelon - Nursery Rhymes": "UCbCmjCuTUZos6Inko4u57UQ", "SET India": "UCpEhnqL0y41EpW2TvWAHD7Q", "Vlad and Niki": "UCvlE5gTbOvjiolFlEm-c_Ow", "Kids Diana Show": "UCk8GzjMOrta8yxDcKfylJYw", "Like Nastya": "UCJplp5SjeGSdVdwsfb9Q7lQ", "Stokes Twins": "UCWEHe8zUZJpS6jO9MsciXfA", "Zee Music Company": "UCFFbwnve3yF62-tVXkTyHqg", "PewDiePie": "UC-lHJZR3Gqxm24_Vd_AJ5Yw", "WWE": "UCJ5v_MCY6GNUBTO8-D3XoAg", "Goldmines": "UC4rlAVgAK0SGk-yTfe48Qpw", "Sony SAB": "UC6-F5tO8uklgE9Zy8IvbdFw", "김프로KIMPRO": "UC6tVx6L6j4U0R6e7qJ9DQ2A", "Blackpink": "UCwmFOfFuvRPI112vR5DNnrA", "ChuChu TV Nursery Rhymes & Kids Songs": "UCb7F3O6ZL1MxBepDSCLr7RQ", "Alan's Universe": "UCsU-I-vHLiaMfV_ceaYz5rQ", "Zee TV": "UCyFZ0zAq4IhFJnD8KrzUHCQ", "5-Minute Crafts": "UC295-Dw_tDNtZXFeAPAW6Aw", "Pinkfong": "UCcdwLMPsaU2ezNSJU1nFoBQ", "BANGTANTV": "UC3IZKseVpdzPSBaWxBxundA", "Colors TV": "UCZf__ehlCEBPop-_sldpBUQ", "A4": "UC4-79UOlP48-QNGgCko5p2g", "HYBE LABELS": "UC3IZKseVpdzPSBaWxBxundA", "ZAMZAM ELECTRONICS TRADING": "UCw4wQpIUVK4Z9rA4KkD8rOw"}

def get_channel_stats(channel_id):
    request = youtube.channels().list(
        part = "snippet, contentDetails, statistics",
        id = channel_id
    )

    response = request.execute()
    return response



# Initializing an empty df
df1 = pd.DataFrame()

seen_ids = set()

for channel_label, channel_id in channels.items():
    if channel_id in seen_ids:
        logging.info(f"Skipping duplicate channel_id for label: {channel_label}")
        continue

    seen_ids.add(channel_id)

    data = get_channel_stats(channel_id)

    if "items" in data:
        df0 = pd.json_normalize(data["items"])
        df0["channel_label"] = channel_label
        df0["date"] = datetime.now().date()
        df1 = pd.concat([df1, df0], axis=0, ignore_index=True)
    else:
        logging.warning(f"No 'items' found for channel {channel_label}")

logging.debug(f"The dataframe has {df1.shape[0]} rows and {df1.shape[1]} columns")
logging.debug(f"The dataframe has following dtypes\n{df1.dtypes}")

def get_aggregated_channel_data():
    """
    Returns the DataFrame containing stats for all channels,
    which was built when the module was imported.
    """
    logging.info(f"Returning aggregated DataFrame df1 with shape: {df1.shape}")
    return df1
    




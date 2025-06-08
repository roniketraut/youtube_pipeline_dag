import pandas as pd
import logging
logging.basicConfig(format = '%(levelname)s: %(message)s', level=logging.DEBUG)

def transform(df):
    df.drop(columns = ["kind", "etag", 'snippet.thumbnails.default.height', 'snippet.thumbnails.medium.url',
       'snippet.thumbnails.medium.width', 'snippet.thumbnails.medium.height',
       'snippet.thumbnails.high.url', 'snippet.thumbnails.high.width',
       'snippet.thumbnails.high.height', 'snippet.thumbnails.default.url', 'snippet.thumbnails.default.width'], errors = 'ignore', inplace = True)

    logging.info("Dropped the unnecessary columns")

    df.rename(columns = {"snippet.title": "channel_name", "snippet.description": "channel_description", "snippet.customUrl": "custom_url",
                     "snippet.publishedAt": "publish_date", "snippet.defaultLanguage": "default_language", "snippet.localized.title": "localized_title",
                     "snippet.localized.description": "local_description", "snippet.country": "country", "contentDetails.relatedPlaylists.likes": "content_detail_related_playlist_likes",
                     "statistics.viewCount": "view_count", "statistics.subscriberCount": "suscriber_count", "statistics.hiddenSubscriberCount": "hidden_subscriber",
                     "statistics.videoCount": "video_count", "contentDetails.relatedPlaylists.uploads":"contentdetails_relatedplaylists_uploads", "id": "channel_id"}, inplace = True)

    logging.info("Renamed the columns for ease of understanding")
    logging.debug(f"The dataframe has {df.shape[0]} rows and {df.shape[1]} columns")

    # Correct indentation for the return statement
    return df
    
#df_transformed = transform(df1)
#logging.debug(df_transformed)

#assert df1.shape[1] == 17, "The DataFrame does not have 17 columns"
#logging.info("Assertion passed: The DataFrame has 17 columns.")



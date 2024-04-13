import s3fs
import pandas as pd
from datetime import datetime
import json
import tweepy


api_key='943400505253904384-DEC1MQgf3Lw1CpqsycyQCjqCA0c5GsE'
api_key_Secret='jdH6MmhILnyVgPfrOLvo53bUhh1glnmD1c1KLgwGjoqxZ'
consumer_token='hf2qmQwoyqK8lb9NXtnoptaET'
consumer_token_secret='6w7L9oTSZPoYFnMUaRbBoaW4xbCM6ZMCquPhNRCybrPoLR8SdB'



#twitter authentication
auth=tweepy.OAuthHandler(api_key,api_key_Secret)
auth.set_access_token(consumer_token,consumer_token_secret)


#Creating an API Object

api=tweepy.API(auth)

tweets=api.user_timeline(screen_name='@elonmusk',
                            count=200,
                            include_rts=False,
                            tweet_mode='extended'
                        )

#print(tweets)
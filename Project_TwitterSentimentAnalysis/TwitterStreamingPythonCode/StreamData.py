
import tweepy

import sys
from urlextract import URLExtract


access_token = "1039581020352389123-GZQrhexfH9BITWz3SLMvuGT8i4wbd6"
access_secret = "pIPjqiQUlbNFbMj2gKi3NYfXn08ZiEjhsmSt0LOG1LuoD"
consumer_key = "SNE5jJmuxsrss3K5CqVoRzNjQ" 
consumer_secret = "8vlmOWYM6iM7Q6VQZffizvcCq34EQPUKeOvijsq7pXwCMjPjEn"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
orig_stdout = sys.stdout
File = open("/Users/madhurisarode/Documents/BDP lessons/lab2/KidsMovieLionKingnn.txt", 'w')
sys.stdout=File
extractor = URLExtract()
#Use csv Writer

for tweet in tweepy.Cursor(api.search,q="#lionking",count=1000,
                           lang="en",
                           since="2019-01-01").items():
    print (tweet.text.encode('utf-8'))
    urls = extractor.find_urls(tweet.text)
    #print(urls)
    
sys.stdout = orig_stdout
File.close()
#File.writerow()
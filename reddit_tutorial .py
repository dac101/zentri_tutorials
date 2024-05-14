import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import os
from datetime import datetime
from nltk.sentiment import SentimentIntensityAnalyzer
import csv
import pyarrow as pa
import pyarrow.parquet as pq

class RedditSentimentAnalyzer:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_url = 'https://www.reddit.com/api/v1/access_token'
        self.base_url = 'https://oauth.reddit.com'
        self.headers = {'User-Agent': 'MyAPI/0.0.1'}
        self.authenticate()
        self.sia = SentimentIntensityAnalyzer()  # Initialize the sentiment analyzer
        self.ensure_directory_exists()  # Ensure the reddit_data directory exists

    def authenticate(self):
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        data = {'grant_type': 'client_credentials'}
        response = requests.post(self.auth_url, auth=auth, data=data, headers=self.headers)
        self.access_token = response.json()['access_token']
        self.headers['Authorization'] = f'bearer {self.access_token}'
        
    def ensure_directory_exists(self):
        if not os.path.exists('reddit_data'):
            os.makedirs('reddit_data')

    def fetch_and_analyze_posts(self, subreddit):
        all_posts = []
        response = requests.get(f'{self.base_url}/r/{subreddit}/top', headers=self.headers)
        try:
            posts = response.json()['data']['children']
            for post in posts:
                post_data = {
                    'title': post['data'].get('title', 'N/A'),
                    'category': post['data'].get('category', 'N/A'),
                    'likes': post['data'].get('likes', 0),
                    'num_comments': post['data'].get('num_comments', 0),
                    'subreddit': post['data'].get('subreddit', 'N/A'),
                    'view_count': post['data'].get('view_count', 0),
                    'selftext': post['data'].get('selftext', '')
                }
                # Perform sentiment analysis
                text_to_analyze = post_data['title'] + " " + post_data['selftext']
                sentiment_score = self.sia.polarity_scores(text_to_analyze)
                post_data.update({
                    'neg_sentiment': sentiment_score['neg'],
                    'neu_sentiment': sentiment_score['neu'],
                    'pos_sentiment': sentiment_score['pos'],
                    'compound_sentiment': sentiment_score['compound'],
                    'overall_sentiment': self.categorize_sentiment(sentiment_score['compound'])
                })
                all_posts.append(post_data)
        except KeyError as e:
            print(f"Key error: {e} - one of the keys was not found in the response.")
        except Exception as e:
            print(f"An error occurred: {e}")
        return all_posts

    def save_posts_to_csv(self, posts, subreddit_name):
        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')  # Use detailed timestamp
        file_path = f"reddit_data/{subreddit_name}_{date_str}_{timestamp}.csv"
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = ['title', 'category', 'likes', 'num_comments', 'subreddit', 'view_count', 'selftext',
                          'neg_sentiment', 'neu_sentiment', 'pos_sentiment', 'compound_sentiment', 'overall_sentiment']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(posts)
        return file_path

    def save_posts_to_parquet(self, posts, subreddit_name):
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')  # Use detailed timestamp

            posts_df = pd.DataFrame(posts)

            # Adjust these types according to your actual data
            schema = pa.schema([
                ('title', pa.string()),
                ('category', pa.string()),
                ('likes', pa.int64()),
                ('num_comments', pa.int64()),
                ('subreddit', pa.string()),
                ('view_count', pa.int64()),
                ('selftext', pa.string()),
                ('neg_sentiment', pa.float64()),
                ('neu_sentiment', pa.float64()),
                ('pos_sentiment', pa.float64()),
                ('compound_sentiment', pa.float64()),
                ('overall_sentiment', pa.string())
            ])

            file_name = f"reddit_data/{subreddit_name}_{date_str}_{timestamp}.parquet"
            table = pa.Table.from_pandas(posts_df, schema=schema)
            pq.write_table(table, file_name)

        except KeyError as e:
            print(f"Key error: {e} - one of the keys was not found in the response.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def categorize_sentiment(compound_score):
        if compound_score > 0.05:
            return 'good'
        elif compound_score < -0.05:
            return 'bad'
        else:
            return 'neutral'

# Usage
subreddits = ["dividendscanada", "wealthsimple"]
analyzer = RedditSentimentAnalyzer('Your Client ID', 'Your Client Secret')
for subreddit in subreddits:
    posts = analyzer.fetch_and_analyze_posts(subreddit)
    analyzer.save_posts_to_csv(posts, subreddit)
    analyzer.save_posts_to_parquet(posts, subreddit)

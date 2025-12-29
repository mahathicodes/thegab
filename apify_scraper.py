#!/usr/bin/env python3
"""
TheGab: TikTok Scraper using Apify API + Supabase Upload
Calls Apify API to scrape TikTok, extracts restaurants, uploads to Supabase
"""

import requests
import json
import os
from datetime import datetime
from typing import List, Dict
from collections import Counter

# NLP and text processing
import spacy
from rapidfuzz import fuzz
import re

# Data processing
import pandas as pd


class RestaurantExtractor:
    """Extract restaurant names from TikTok captions"""

    KNOWN_RESTAURANTS = {
        'pai': 'Pai Northern Thai Kitchen',
        'kenzo': 'Kenzo Ramen',
        'alo': 'Alo',
        'canteen': 'Canteen',
        'ramen': 'Ramen Restaurant',
        'molly': "Molly's Cafe",
        'hot pot': 'Hot Pot Restaurant',
        'shim': 'Shim Korean BBQ',
        'goro': 'Goro Ramen',
        'piri': 'Piri Piri Grille',
        'boku': 'Boku Sushi',
        'peaches': 'Peaches Restaurant',
        'giulio': 'Giulio Pizzeria',
        'su': 'Su Sushi',
        'kim': "Kim's Korean BBQ",
        'tabasco': 'Tabasco Grille',
        'sushi': 'Sushi Restaurant',
        'pizza': 'Pizza Restaurant',
        'bbq': 'BBQ Restaurant'
    }

    def __init__(self):
        print("🧠 Loading spaCy NLP model...")
        self.nlp = spacy.load('en_core_web_sm')

    def clean_text(self, text: str) -> str:
        """Clean social media text"""
        if not text:
            return ""
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()

    def extract_restaurants(self, caption: str) -> List[Dict]:
        """Extract restaurant mentions from caption"""
        restaurants = []
        if not caption:
            return restaurants

        cleaned = self.clean_text(caption)
        seen = set()

        for keyword, restaurant_name in self.KNOWN_RESTAURANTS.items():
            if keyword in cleaned:
                score = fuzz.partial_ratio(cleaned, keyword) / 100

                if score >= 0.60 and restaurant_name not in seen:
                    restaurants.append({
                        'name': restaurant_name,
                        'mentioned_as': keyword,
                        'confidence': round(score, 2)
                    })
                    seen.add(restaurant_name)

        return restaurants


def scrape_tiktok_with_apify(hashtag: str, max_posts: int = 50) -> List[Dict]:
    """Scrape TikTok using Apify API"""

    APIFY_TOKEN = os.getenv('APIFY_TOKEN')
    if not APIFY_TOKEN:
        print("❌ Missing APIFY_TOKEN environment variable")
        print("   Set it in GitHub Secrets")
        return []

    print(f"  📡 Calling Apify API for #{hashtag}...")

    # Apify API endpoint (API Dojo TikTok Scraper)
    actor_id = "apidojo/tiktok-scraper"
    run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs"

    # Configure scraper
    input_data = {
        "hashtag": hashtag,
        "videos": max_posts,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {APIFY_TOKEN}"
    }

    try:
        # Start the run
        print(f"  ⏳ Starting Apify actor...")
        response = requests.post(run_url, json=input_data, headers=headers, timeout=60)
        response.raise_for_status()

        run_data = response.json()
        run_id = run_data['data']['id']

        print(f"  🔄 Run ID: {run_id}")
        print(f"  ⏳ Waiting for results (this may take 2-5 minutes)...")

        # Poll for completion
        import time
        max_wait = 600  # 10 minutes
        start_time = time.time()

        while time.time() - start_time < max_wait:
            status_url = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}"
            status_response = requests.get(status_url, headers=headers, timeout=30)
            status_response.raise_for_status()

            run_status = status_response.json()['data']['status']

            if run_status == 'SUCCEEDED':
                print(f"  ✅ Run succeeded!")
                break
            elif run_status == 'FAILED':
                print(f"  ❌ Run failed")
                return []
            else:
                print(f"     Status: {run_status}...")
                time.sleep(10)
        else:
            print(f"  ⏱️  Run timed out after 10 minutes")
            return []

        # Get results
        print(f"  📥 Fetching results...")
        dataset_url = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}/dataset/items"
        dataset_response = requests.get(dataset_url, headers=headers, timeout=30)
        dataset_response.raise_for_status()

        videos = dataset_response.json()

        # Transform Apify data to our format
        posts = []
        for video in videos:
            post = {
                'id': str(video.get('id', '')),
                'url': video.get('videoUrl', ''),
                'caption': video.get('description', ''),
                'likes': video.get('diggCount', 0),
                'comments': video.get('commentCount', 0),
                'shares': video.get('shareCount', 0),
                'views': video.get('playCount', 0),
                'creator': video.get('authorId', ''),
                'create_time': datetime.now().isoformat(),
                'hashtag': hashtag,
                'scraped_at': datetime.now().isoformat(),
                'source': 'apify'
            }
            posts.append(post)

        print(f"  ✅ Fetched {len(posts)} videos")
        return posts

    except Exception as e:
        print(f"  ❌ Error: {str(e)[:80]}")
        return []


def upload_to_supabase(posts: List[Dict]) -> bool:
    """Upload posts to Supabase"""

    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Missing Supabase credentials")
        return False

    from supabase import create_client

    print(f"\n📤 Uploading {len(posts)} posts to Supabase...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    uploaded = 0
    errors = 0

    for post in posts:
        try:
            row = {
                'id': post.get('id'),
                'url': post.get('url'),
                'caption': post.get('caption', ''),
                'likes': post.get('likes', 0),
                'comments': post.get('comments', 0),
                'shares': post.get('shares', 0),
                'views': post.get('views', 0),
                'creator': post.get('creator', ''),
                'create_time': post.get('create_time'),
                'hashtag': post.get('hashtag'),
                'restaurants': json.dumps(post.get('restaurants', [])),
                'has_restaurant_mention': post.get('has_restaurant_mention', False),
                'scraped_at': post.get('scraped_at')
            }

            supabase.table('tiktok_posts').upsert(row).execute()
            uploaded += 1

            if uploaded % 10 == 0:
                print(f"   ✓ Uploaded {uploaded} posts...")

        except Exception as e:
            print(f"   ❌ Error uploading {post.get('id')}: {str(e)[:60]}")
            errors += 1

    print(f"   ✅ Uploaded: {uploaded} posts")
    if errors > 0:
        print(f"   ⚠️  Errors: {errors}")

    return uploaded > 0


def main():
    """Main pipeline"""

    print("="*60)
    print("🎬 THEGAB: TIKTOK SCRAPER (APIFY) + RESTAURANT EXTRACTION")
    print("="*60)

    # Initialize extractor
    extractor = RestaurantExtractor()

    # Configure hashtags
    hashtags = ['torontofood', 'torontorestaurants']
    max_posts_per_hashtag = 50

    # Scrape with Apify
    print(f"\n📊 Scraping {len(hashtags)} hashtags with Apify...")
    all_posts = []

    for hashtag in hashtags:
        print(f"\n🎯 Hashtag: #{hashtag}")
        posts = scrape_tiktok_with_apify(hashtag, max_posts_per_hashtag)
        all_posts.extend(posts)

    print(f"\n{'='*60}")
    print(f"✅ Total scraped: {len(all_posts)} posts")

    if len(all_posts) == 0:
        print("⚠️  No posts scraped.")
        return

    # Extract restaurants
    print(f"\n🔄 Processing posts and extracting restaurants...")
    processed_posts = []

    for post in all_posts:
        caption = post.get('caption', '')
        restaurants = extractor.extract_restaurants(caption)

        processed_post = {
            **post,
            'restaurants': restaurants,
            'has_restaurant_mention': len(restaurants) > 0
        }
        processed_posts.append(processed_post)

    # Analysis
    print(f"\n{'='*60}")
    print("📈 DATA QUALITY REPORT")
    print(f"{'='*60}")

    if len(processed_posts) > 0:
        df = pd.DataFrame(processed_posts)

        print(f"\n📌 Posts Collected:")
        print(f"   Total: {len(df)}")
        posts_with_rests = df['has_restaurant_mention'].sum()
        print(f"   With restaurants: {posts_with_rests}")
        extraction_rate = (posts_with_rests / len(df) * 100) if len(df) > 0 else 0
        print(f"   Extraction rate: {extraction_rate:.1f}%")

        print(f"\n👥 Real Engagement Metrics:")
        print(f"   Avg likes: {df['likes'].mean():.0f}")
        print(f"   Avg comments: {df['comments'].mean():.0f}")
        print(f"   Avg views: {df['views'].mean():.0f}")
        print(f"   Total engagement: {(df['likes'] + df['comments'] + df['shares']).sum():.0f}")

        # Top restaurants
        restaurant_mentions = Counter()
        for post in processed_posts:
            for rest in post['restaurants']:
                restaurant_mentions[rest['name']] += 1

        print(f"\n🏪 Top Restaurants Mentioned:")
        if restaurant_mentions:
            for rest, count in restaurant_mentions.most_common(10):
                print(f"   • {rest}: {count} mentions")
        else:
            print("   ℹ️ No restaurants found in captions")

        # Save results
        print(f"\n💾 Saving results...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        json_file = f'posts_raw_{timestamp}.json'
        with open(json_file, 'w') as f:
            json.dump(processed_posts, f, indent=2)
        print(f"   ✅ Saved: {json_file}")

        # Upload to Supabase
        success = upload_to_supabase(processed_posts)

        print(f"\n{'='*60}")
        if success:
            print("✅ PIPELINE COMPLETE!")
            print("✅ Data uploaded to Supabase")
        else:
            print("⚠️  Posts scraped but upload failed")
        print("="*60)


if __name__ == '__main__':
    main()

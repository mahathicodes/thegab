#!/usr/bin/env python3
"""
TheGab: TikTok Restaurant Scraper - Local Testing Script
Scrapes real TikTok data and extracts restaurant mentions
"""

import asyncio
import json
from datetime import datetime
from typing import List, Dict
from collections import Counter

# NLP and text processing
import spacy
from rapidfuzz import fuzz
import re

# TikTok scraping
try:
    from TikTokApi import TikTokApi
    TIKTOKAPI_AVAILABLE = True
except ImportError:
    TIKTOKAPI_AVAILABLE = False
    print("⚠️  TikTokApi not installed. Install with: pip install TikTokApi")

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


async def scrape_tiktok_hashtag(hashtag: str, max_posts: int = 30) -> List[Dict]:
    """Scrape TikTok hashtag using TikTokApi"""

    if not TIKTOKAPI_AVAILABLE:
        print(f"❌ TikTokApi not available. Install with: pip install TikTokApi")
        return []

    print(f"  📡 Connecting to TikTok...")
    posts = []

    try:
        async with TikTokApi() as api:
            print(f"  🔐 Creating sessions...")
            await api.create_sessions(num_sessions=1, sleep_after=3)

            print(f"  🏷️  Loading hashtag #{hashtag}...")
            tag = api.hashtag(name=hashtag)

            print(f"  ⬇️  Fetching videos...")
            count = 0

            async for video in tag.videos(count=max_posts):
                try:
                    post = {
                        'id': str(video.id),
                        'url': f'https://www.tiktok.com/@{video.author.username}/video/{video.id}',
                        'caption': video.desc if hasattr(video, 'desc') else '',
                        'likes': video.stats.digg_count if hasattr(video.stats, 'digg_count') else 0,
                        'comments': video.stats.comment_count if hasattr(video.stats, 'comment_count') else 0,
                        'shares': video.stats.share_count if hasattr(video.stats, 'share_count') else 0,
                        'views': video.stats.play_count if hasattr(video.stats, 'play_count') else 0,
                        'creator': video.author.username if hasattr(video.author, 'username') else 'unknown',
                        'create_time': datetime.fromtimestamp(video.create_time).isoformat() if hasattr(video, 'create_time') else datetime.now().isoformat(),
                        'hashtag': hashtag,
                        'scraped_at': datetime.now().isoformat(),
                        'source': 'tiktokapi'
                    }
                    posts.append(post)
                    count += 1

                    if count % 5 == 0:
                        print(f"     ✓ Fetched {count} videos...")

                except Exception as e:
                    print(f"     ⚠️  Error parsing video: {str(e)[:50]}")
                    continue

            print(f"  ✅ Fetched {len(posts)} videos")
            return posts

    except Exception as e:
        print(f"  ❌ Error scraping #{hashtag}: {str(e)[:80]}")
        return []


def main():
    """Main pipeline"""

    print("="*60)
    print("🎬 THEGAB: TIKTOK RESTAURANT SCRAPER - LOCAL TEST")
    print("="*60)

    # Initialize extractor
    extractor = RestaurantExtractor()

    # Configure hashtags to scrape
    hashtags = ['torontofood', 'torontorestaurants']
    max_posts_per_hashtag = 15

    # Scrape hashtags
    print(f"\n📊 Scraping {len(hashtags)} hashtags...")
    all_posts = []

    for hashtag in hashtags:
        print(f"\n🎯 Hashtag: #{hashtag}")
        posts = asyncio.run(scrape_tiktok_hashtag(hashtag, max_posts_per_hashtag))
        all_posts.extend(posts)

    print(f"\n{'='*60}")
    print(f"✅ Total scraped: {len(all_posts)} posts")

    if len(all_posts) == 0:
        print("⚠️  No posts scraped. Check your internet connection or try different hashtags.")
        return

    # Process posts
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

        # Sample captions
        print(f"\n📝 Sample Captions with Restaurants:")
        sample_count = 0
        for post in processed_posts:
            if post['has_restaurant_mention'] and sample_count < 3:
                caption = post.get('caption', '[No caption]')[:90]
                rests = [r['name'] for r in post['restaurants']]
                print(f"   • {caption}...")
                print(f"     Restaurants: {rests}")
                sample_count += 1

        # Save results
        print(f"\n💾 Saving results...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save JSON
        json_file = f'posts_raw_{timestamp}.json'
        with open(json_file, 'w') as f:
            json.dump(processed_posts, f, indent=2)
        print(f"   ✅ Saved: {json_file}")

        # Save CSV
        csv_file = f'restaurant_metrics_{timestamp}.csv'
        restaurant_stats = []
        for rest_name, count in restaurant_mentions.most_common():
            rest_posts = [p for p in processed_posts if any(r['name'] == rest_name for r in p['restaurants'])]
            total_eng = sum(p.get('likes', 0) + p.get('comments', 0) + p.get('shares', 0) for p in rest_posts)
            avg_eng = total_eng / len(rest_posts) if rest_posts else 0

            restaurant_stats.append({
                'restaurant': rest_name,
                'mentions': count,
                'posts': len(rest_posts),
                'total_engagement': int(total_eng),
                'avg_engagement': int(avg_eng)
            })

        pd.DataFrame(restaurant_stats).to_csv(csv_file, index=False)
        print(f"   ✅ Saved: {csv_file}")

        # Save summary
        summary_file = f'summary_{timestamp}.json'
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_posts': len(processed_posts),
            'posts_with_restaurants': int(posts_with_rests),
            'extraction_rate': f'{extraction_rate:.1f}%',
            'unique_restaurants': len(restaurant_mentions),
            'top_restaurant': restaurant_mentions.most_common(1)[0][0] if restaurant_mentions else None
        }
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"   ✅ Saved: {summary_file}")

        print(f"\n{'='*60}")
        print("✅ PIPELINE COMPLETE!")
        print(f"{'='*60}")
        print(f"\n📁 Files saved in current directory")
        print(f"📊 Next: Review results, then set up GitHub Actions for automation")


if __name__ == '__main__':
    main()

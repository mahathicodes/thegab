#!/usr/bin/env python3
"""
Upload scraped TikTok data to Supabase
"""

import json
import os
from datetime import datetime
from glob import glob
from supabase import create_client, Client

# Environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing SUPABASE_URL or SUPABASE_KEY environment variables")
    print("   Set them in GitHub Secrets")
    exit(1)

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("="*60)
print("📤 UPLOADING TO SUPABASE")
print("="*60)


def upload_posts_to_supabase(json_file):
    """Upload raw posts to Supabase"""

    print(f"\n📁 Reading: {json_file}")

    with open(json_file, 'r') as f:
        posts = json.load(f)

    print(f"   Posts to upload: {len(posts)}")

    if len(posts) == 0:
        print("   ⚠️  No posts to upload")
        return

    # Upload each post
    uploaded = 0
    errors = 0

    for post in posts:
        try:
            # Transform post data for Supabase
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
                'restaurants': json.dumps(post.get('restaurants', [])),  # JSON array
                'has_restaurant_mention': post.get('has_restaurant_mention', False),
                'scraped_at': post.get('scraped_at')
            }

            # Insert into Supabase (use upsert to handle duplicates)
            response = supabase.table('tiktok_posts').upsert(row).execute()
            uploaded += 1

            if uploaded % 10 == 0:
                print(f"   ✓ Uploaded {uploaded} posts...")

        except Exception as e:
            print(f"   ❌ Error uploading post {post.get('id')}: {str(e)[:80]}")
            errors += 1

    print(f"   ✅ Uploaded: {uploaded} posts")
    if errors > 0:
        print(f"   ⚠️  Errors: {errors}")


def upload_restaurants_to_supabase(csv_file):
    """Upload aggregated restaurant metrics to Supabase"""

    print(f"\n📊 Reading: {csv_file}")

    import pandas as pd
    df = pd.read_csv(csv_file)

    print(f"   Restaurants to upload: {len(df)}")

    uploaded = 0
    errors = 0

    for _, row in df.iterrows():
        try:
            data = {
                'name': row['restaurant'],
                'mentions': int(row['mentions']),
                'posts': int(row['posts']),
                'total_engagement': int(row['total_engagement']),
                'avg_engagement': int(row['avg_engagement']),
                'last_updated': datetime.now().isoformat()
            }

            # Upsert by restaurant name
            response = supabase.table('restaurants').upsert(
                data,
                on_conflict='name'
            ).execute()
            uploaded += 1

        except Exception as e:
            print(f"   ❌ Error uploading {row['restaurant']}: {str(e)[:80]}")
            errors += 1

    print(f"   ✅ Uploaded: {uploaded} restaurants")
    if errors > 0:
        print(f"   ⚠️  Errors: {errors}")


def main():
    """Upload latest scraped data"""

    # Find latest JSON file
    json_files = sorted(glob('posts_raw_*.json'), reverse=True)
    csv_files = sorted(glob('restaurant_metrics_*.csv'), reverse=True)

    if not json_files:
        print("❌ No posts_raw_*.json files found")
        return

    latest_json = json_files[0]
    latest_csv = csv_files[0] if csv_files else None

    print(f"Latest files:")
    print(f"  JSON: {latest_json}")
    if latest_csv:
        print(f"  CSV:  {latest_csv}")

    # Upload posts
    upload_posts_to_supabase(latest_json)

    # Upload restaurant metrics
    if latest_csv:
        upload_restaurants_to_supabase(latest_csv)

    print(f"\n{'='*60}")
    print("✅ UPLOAD COMPLETE!")
    print("="*60)


if __name__ == '__main__':
    main()

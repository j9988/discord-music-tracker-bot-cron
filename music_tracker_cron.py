"""
Cron-compatible version of the music tracker bot
This version checks for releases once and exits (perfect for scheduled runs)
"""
import json
import os
from datetime import datetime
import asyncio
import aiohttp
import sys
from typing import List, Dict
import logging

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ARTISTS_FILE = 'artists.json'
TRACKED_RELEASES_FILE = 'tracked_releases.json'
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')  # We'll use webhook instead of bot


class MusicTrackerCron:
    def __init__(self):
        self.artists = []
        self.tracked_releases = {}
        self.spotify_token = None
        self.spotify_token_expiry = None
        
    def load_artists(self):
        """Load artist data from JSON file"""
        try:
            with open(ARTISTS_FILE, 'r', encoding='utf-8') as f:
                self.artists = json.load(f)
            logger.info(f"Loaded {len(self.artists)} artists")
        except FileNotFoundError:
            logger.warning(f"{ARTISTS_FILE} not found.")
            self.artists = []
            
    def load_tracked_releases(self):
        """Load previously tracked releases"""
        try:
            with open(TRACKED_RELEASES_FILE, 'r', encoding='utf-8') as f:
                self.tracked_releases = json.load(f)
        except FileNotFoundError:
            self.tracked_releases = {}
            
    def save_tracked_releases(self):
        """Save tracked releases to file"""
        with open(TRACKED_RELEASES_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.tracked_releases, f, indent=2)
    
    async def get_spotify_token(self):
        """Get Spotify API access token"""
        if self.spotify_token and self.spotify_token_expiry > datetime.now():
            return self.spotify_token
            
        client_id = os.getenv('SPOTIFY_CLIENT_ID')
        client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        
        if not client_id or not client_secret:
            logger.error("Spotify credentials not set")
            return None
            
        async with aiohttp.ClientSession() as session:
            from datetime import timedelta
            auth_url = 'https://accounts.spotify.com/api/token'
            auth_data = {'grant_type': 'client_credentials'}
            
            async with session.post(
                auth_url,
                data=auth_data,
                auth=aiohttp.BasicAuth(client_id, client_secret)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    self.spotify_token = data['access_token']
                    self.spotify_token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300)
                    return self.spotify_token
                else:
                    logger.error(f"Failed to get Spotify token: {response.status}")
                    return None
    
    async def check_spotify_releases(self, artist: Dict) -> List[Dict]:
        """Check for new Spotify releases"""
        if not artist.get('spotify_id'):
            return []
            
        token = await self.get_spotify_token()
        if not token:
            return []
            
        new_releases = []
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        async with aiohttp.ClientSession() as session:
            headers = {'Authorization': f'Bearer {token}'}
            url = f"https://api.spotify.com/v1/artists/{artist['spotify_id']}/albums"
            params = {'include_groups': 'album,single', 'limit': 10}
            
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for album in data.get('items', []):
                        release_date = album.get('release_date', '')
                        album_id = album['id']
                        
                        if release_date >= yesterday:
                            track_key = f"spotify_{album_id}"
                            if track_key not in self.tracked_releases:
                                new_releases.append({
                                    'platform': 'Spotify',
                                    'artist': artist['name'],
                                    'title': album['name'],
                                    'type': album['album_type'],
                                    'url': album['external_urls']['spotify'],
                                    'image': album['images'][0]['url'] if album['images'] else None,
                                    'release_date': release_date,
                                    'track_key': track_key
                                })
        
        return new_releases
    
    async def check_youtube_releases(self, artist: Dict) -> List[Dict]:
        """Check for new YouTube releases"""
        if not artist.get('youtube_channel_id'):
            return []
            
        youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        if not youtube_api_key:
            return []
            
        new_releases = []
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).isoformat() + 'Z'
        
        async with aiohttp.ClientSession() as session:
            for channel_id in artist['youtube_channel_id']:
                url = "https://www.googleapis.com/youtube/v3/search"
                params = {
                    'part': 'snippet',
                    'channelId': channel_id,
                    'maxResults': 10,
                    'order': 'date',
                    'type': 'video',
                    'publishedAfter': yesterday,
                    'key': youtube_api_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        for item in data.get('items', []):
                            video_id = item['id']['videoId']
                            track_key = f"youtube_{video_id}"
                            
                            if track_key not in self.tracked_releases:
                                new_releases.append({
                                    'platform': 'YouTube',
                                    'artist': artist['name'],
                                    'title': item['snippet']['title'],
                                    'type': 'video',
                                    'url': f"https://www.youtube.com/watch?v={video_id}",
                                    'image': item['snippet']['thumbnails']['high']['url'],
                                    'release_date': item['snippet']['publishedAt'][:10],
                                    'track_key': track_key
                                })
        
        return new_releases
    
    async def check_melon_releases(self, artist: Dict) -> List[Dict]:
        """Check for new Melon releases"""
        if not artist.get('melon_url'):
            return []
            
        try:
            from melon_scraper import MelonScraper
            scraper = MelonScraper()
            new_releases = await scraper.scrape_artist_songs(
                artist['melon_url'],
                artist['name']
            )
            await scraper.close_session()
            
            filtered_releases = []
            for release in new_releases:
                if release['track_key'] not in self.tracked_releases:
                    filtered_releases.append(release)
            
            return filtered_releases
        except Exception as e:
            logger.error(f"Melon check error: {e}")
            return []
    
    async def check_all_releases(self) -> List[Dict]:
        """Check all platforms for all artists"""
        all_releases = []
        
        for artist in self.artists:
            logger.info(f"Checking releases for {artist['name']}")
            
            spotify_releases = await self.check_spotify_releases(artist)
            youtube_releases = await self.check_youtube_releases(artist)
            melon_releases = await self.check_melon_releases(artist)
            
            all_releases.extend(spotify_releases)
            all_releases.extend(youtube_releases)
            all_releases.extend(melon_releases)
            
            await asyncio.sleep(1)
        
        return all_releases
    
    async def send_to_discord_webhook(self, releases: List[Dict]):
        """Send releases to Discord via webhook"""
        if not DISCORD_WEBHOOK_URL:
            logger.error("DISCORD_WEBHOOK_URL not set")
            return
        
        async with aiohttp.ClientSession() as session:
            for release in releases:
                embed = {
                    "title": f"🎵 New {release['type'].title()} Released!",
                    "description": f"**{release['artist']}** - {release['title']}",
                    "color": 3447003,
                    "fields": [
                        {
                            "name": "Platform",
                            "value": release['platform'],
                            "inline": True
                        },
                        {
                            "name": "Release Date",
                            "value": release['release_date'],
                            "inline": True
                        },
                        {
                            "name": "Listen Now",
                            "value": f"[Click Here]({release['url']})",
                            "inline": False
                        }
                    ],
                    "timestamp": datetime.now().isoformat(),
                    "footer": {
                        "text": "Music Release Tracker"
                    }
                }
                
                if release['image']:
                    embed['thumbnail'] = {"url": release['image']}
                
                payload = {"embeds": [embed]}
                
                async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                    if response.status == 204:
                        logger.info(f"Sent: {release['artist']} - {release['title']}")
                    else:
                        logger.error(f"Failed to send webhook: {response.status}")
                
                await asyncio.sleep(1)
    
    async def run(self):
        """Main execution"""
        logger.info("Starting music tracker cron job...")
        
        self.load_artists()
        self.load_tracked_releases()
        
        if not self.artists:
            logger.warning("No artists configured")
            return
        
        # Check for new releases
        new_releases = await self.check_all_releases()
        
        if new_releases:
            logger.info(f"Found {len(new_releases)} new releases")
            
            # Send to Discord
            await self.send_to_discord_webhook(new_releases)
            
            # Mark as tracked
            for release in new_releases:
                self.tracked_releases[release['track_key']] = {
                    'artist': release['artist'],
                    'title': release['title'],
                    'timestamp': datetime.now().isoformat()
                }
            
            self.save_tracked_releases()
        else:
            logger.info("No new releases found")
        
        logger.info("Cron job completed!")


async def main():
    tracker = MusicTrackerCron()
    await tracker.run()


if __name__ == "__main__":
    asyncio.run(main())

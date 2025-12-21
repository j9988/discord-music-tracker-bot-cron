import json
import os
from datetime import datetime, timedelta, timezone
import asyncio
import aiohttp
import sys
from typing import List, Dict
import logging

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
ARTISTS_FILE = 'artists.json'
TRACKED_RELEASES_FILE = 'tracked_releases.json'
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
CHECK_HOURS = int(os.getenv('CHECK_HOURS', '2'))  # Configurable check window (default 2 hours)


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
            logger.info(f"Loaded {len(self.tracked_releases)} tracked releases")
        except FileNotFoundError:
            logger.info("No previous tracked releases found, starting fresh")
            self.tracked_releases = {}
            
    def save_tracked_releases(self):
        """Save tracked releases to file"""
        with open(TRACKED_RELEASES_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.tracked_releases, f, indent=2)
        logger.info(f"Saved {len(self.tracked_releases)} tracked releases")
    
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
                    logger.info("Successfully obtained Spotify token")
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
        # Check releases from the past CHECK_HOURS + buffer
        check_period = datetime.now() - timedelta(hours=CHECK_HOURS + 1)
        check_date = check_period.strftime('%Y-%m-%d')
        
        async with aiohttp.ClientSession() as session:
            headers = {'Authorization': f'Bearer {token}'}
            url = f"https://api.spotify.com/v1/artists/{artist['spotify_id']}/albums"
            params = {'include_groups': 'album,single', 'limit': 20}  # Increased limit
            
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        for album in data.get('items', []):
                            release_date = album.get('release_date', '')
                            album_id = album['id']
                            
                            # Check if release is recent enough
                            if release_date >= check_date:
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
                                    logger.info(f"New Spotify release: {artist['name']} - {album['name']}")
                    elif response.status == 429:
                        logger.warning(f"Rate limited on Spotify for {artist['name']}")
                    else:
                        logger.error(f"Spotify API error for {artist['name']}: {response.status}")
            except Exception as e:
                logger.error(f"Error checking Spotify for {artist['name']}: {e}")
        
        return new_releases
    
    async def check_youtube_releases(self, artist: Dict) -> List[Dict]:
        """Check for new YouTube releases"""
        if not artist.get('youtube_channel_id'):
            return []
            
        youtube_api_key = os.getenv('YOUTUBE_API_KEY')
        if not youtube_api_key:
            logger.warning("YouTube API key not set")
            return []
            
        new_releases = []
        # Check videos published in the last CHECK_HOURS + buffer
        check_time = datetime.now(timezone.utc) - timedelta(hours=CHECK_HOURS + 1)
        published_after = check_time.isoformat()
        
        logger.info(f"Checking YouTube for videos published after: {published_after}")
        
        # Handle both string and list for youtube_channel_id
        channel_ids = artist['youtube_channel_id']
        if isinstance(channel_ids, str):
            channel_ids = [channel_ids]
        
        async with aiohttp.ClientSession() as session:
            for channel_id in channel_ids:
                url = "https://www.googleapis.com/youtube/v3/search"
                params = {
                    'part': 'snippet',
                    'channelId': channel_id,
                    'maxResults': 20,  # Increased to catch more
                    'order': 'date',
                    'type': 'video',
                    'publishedAfter': published_after,
                    'key': youtube_api_key
                }
                
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            items = data.get('items', [])
                            logger.info(f"Found {len(items)} YouTube videos for {artist['name']} (channel: {channel_id})")
                            
                            for item in items:
                                video_id = item['id']['videoId']
                                track_key = f"youtube_{video_id}"
                                
                                if track_key not in self.tracked_releases:
                                    published_at = item['snippet']['publishedAt']
                                    new_releases.append({
                                        'platform': 'YouTube',
                                        'artist': artist['name'],
                                        'title': item['snippet']['title'],
                                        'type': 'video',
                                        'url': f"https://www.youtube.com/watch?v={video_id}",
                                        'image': item['snippet']['thumbnails']['high']['url'],
                                        'release_date': published_at[:10],
                                        'published_at': published_at,
                                        'track_key': track_key
                                    })
                                    logger.info(f"New YouTube video: {artist['name']} - {item['snippet']['title']}")
                                else:
                                    logger.debug(f"Already tracked: {track_key}")
                        elif response.status == 403:
                            error_data = await response.json()
                            logger.error(f"YouTube API quota exceeded or invalid key: {error_data}")
                        else:
                            logger.error(f"YouTube API error for {artist['name']}: {response.status}")
                except Exception as e:
                    logger.error(f"Error checking YouTube for {artist['name']}: {e}")
                
                await asyncio.sleep(0.5)  # Rate limiting between requests
        
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
                    logger.info(f"New Melon release: {artist['name']} - {release['title']}")
            
            return filtered_releases
        except ImportError:
            logger.warning("melon_scraper module not found, skipping Melon checks")
            return []
        except Exception as e:
            logger.error(f"Melon check error for {artist['name']}: {e}")
            return []
    
    async def check_all_releases(self) -> List[Dict]:
        """Check all platforms for all artists"""
        all_releases = []
        
        for artist in self.artists:
            logger.info(f"=== Checking releases for {artist['name']} ===")
            
            spotify_releases = await self.check_spotify_releases(artist)
            youtube_releases = await self.check_youtube_releases(artist)
            melon_releases = await self.check_melon_releases(artist)
            
            all_releases.extend(spotify_releases)
            all_releases.extend(youtube_releases)
            all_releases.extend(melon_releases)
            
            logger.info(f"Found {len(spotify_releases)} Spotify, {len(youtube_releases)} YouTube, {len(melon_releases)} Melon releases")
            
            await asyncio.sleep(1)  # Rate limiting between artists
        
        return all_releases
    
    async def send_to_discord_webhook(self, releases: List[Dict]):
        """Send releases to Discord via webhook"""
        if not DISCORD_WEBHOOK_URL:
            logger.error("DISCORD_WEBHOOK_URL not set")
            return
        
        async with aiohttp.ClientSession() as session:
            for release in releases:
                # Determine emoji based on platform
                emoji = {
                    'Spotify': '🎵',
                    'YouTube': '📹',
                    'Melon': '🍈'
                }.get(release['platform'], '🎵')
                
                embed = {
                    "title": f"{emoji} New {release['type'].title()} Released!",
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
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "footer": {
                        "text": "Music Release Tracker"
                    }
                }
                
                if release.get('image'):
                    embed['thumbnail'] = {"url": release['image']}
                
                payload = {"embeds": [embed]}
                
                try:
                    async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                        if response.status == 204:
                            logger.info(f"✓ Sent to Discord: {release['artist']} - {release['title']}")
                        else:
                            error_text = await response.text()
                            logger.error(f"Failed to send webhook: {response.status} - {error_text}")
                except Exception as e:
                    logger.error(f"Error sending to Discord: {e}")
                
                await asyncio.sleep(1)  # Rate limiting
    
    def cleanup_old_tracked_releases(self, days=30):
        """Remove tracked releases older than X days to prevent file bloat"""
        cutoff = datetime.now() - timedelta(days=days)
        original_count = len(self.tracked_releases)
        
        keys_to_remove = []
        for key, value in self.tracked_releases.items():
            timestamp_str = value.get('timestamp', '')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    if timestamp < cutoff:
                        keys_to_remove.append(key)
                except:
                    pass
        
        for key in keys_to_remove:
            del self.tracked_releases[key]
        
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} old tracked releases (older than {days} days)")
    
    async def run(self):
        """Main execution"""
        logger.info("=" * 60)
        logger.info("Starting music tracker cron job...")
        logger.info(f"Check window: last {CHECK_HOURS} hours")
        logger.info("=" * 60)
        
        self.load_artists()
        self.load_tracked_releases()
        
        if not self.artists:
            logger.warning("No artists configured in artists.json")
            return
        
        # Check for new releases
        new_releases = await self.check_all_releases()
        
        if new_releases:
            logger.info("=" * 60)
            logger.info(f"🎉 Found {len(new_releases)} NEW RELEASES!")
            logger.info("=" * 60)
            
            # Send to Discord
            await self.send_to_discord_webhook(new_releases)
            
            # Mark as tracked
            for release in new_releases:
                self.tracked_releases[release['track_key']] = {
                    'artist': release['artist'],
                    'title': release['title'],
                    'platform': release['platform'],
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            
            # Cleanup old entries and save
            self.cleanup_old_tracked_releases(days=30)
            self.save_tracked_releases()
        else:
            logger.info("=" * 60)
            logger.info("No new releases found")
            logger.info("=" * 60)
        
        logger.info("Cron job completed successfully!")


async def main():
    try:
        tracker = MusicTrackerCron()
        await tracker.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

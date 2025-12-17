"""
Enhanced Melon scraper module for the Discord music bot
This module handles web scraping of Melon.com for Korean music releases
"""

import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class MelonScraper:
    """Handles scraping Melon.com for new releases"""
    
    def __init__(self):
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    async def get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)
        return self.session
    
    async def close_session(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def scrape_artist_songs(self, melon_url: str, artist_name: str) -> list:
        """
        Scrape artist's song page on Melon
        
        Args:
            melon_url: URL to artist's song page on Melon
            artist_name: Name of the artist
            
        Returns:
            List of new releases found
        """
        new_releases = []
        
        try:
            session = await self.get_session()
            
            # Fetch the page
            async with session.get(melon_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.error(f"Melon returned status {response.status} for {artist_name}")
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                # Find song table - Melon uses a specific table structure
                # Structure: <table> with class containing song listings
                song_table = soup.find('table', {'class': 'list_tb'}) or soup.find('tbody')
                
                if not song_table:
                    logger.warning(f"Could not find song table for {artist_name}")
                    return []
                
                # Get yesterday's date for comparison
                yesterday = datetime.now() - timedelta(days=1)
                
                # Find all song entries
                songs = song_table.find_all('tr', {'data-song-no': True})
                
                for song in songs[:10]:  # Check last 10 songs
                    try:
                        # Extract song information
                        song_no = song.get('data-song-no')
                        
                        # Title
                        title_element = song.find('div', {'class': 'ellipsis rank01'})
                        if not title_element:
                            title_element = song.find('a', {'class': 'fc_gray'})
                        
                        if not title_element:
                            continue
                            
                        title = title_element.get_text(strip=True)
                        song_link = f"https://www.melon.com/song/detail.htm?songId={song_no}"
                        
                        # Release date
                        issue_date = song.find('span', {'class': 'cnt'})
                        if not issue_date:
                            # Try alternative selector
                            issue_date = song.find('td', {'class': 'wrap_date'})
                        
                        if issue_date:
                            date_text = issue_date.get_text(strip=True)
                            # Parse Korean date format (e.g., "2024.12.13")
                            try:
                                release_date = datetime.strptime(date_text, '%Y.%m.%d')
                                
                                # Check if released recently (within last day)
                                if release_date >= yesterday:
                                    # Get album art if available
                                    img_element = song.find('img')
                                    image_url = img_element.get('src') if img_element else None
                                    
                                    # Check for album information
                                    album_element = song.find('div', {'class': 'ellipsis rank02'})
                                    album_name = album_element.get_text(strip=True) if album_element else None
                                    
                                    new_releases.append({
                                        'platform': 'Melon',
                                        'artist': artist_name,
                                        'title': title,
                                        'album': album_name,
                                        'type': 'single',
                                        'url': song_link,
                                        'image': image_url,
                                        'release_date': release_date.strftime('%Y-%m-%d'),
                                        'track_key': f"melon_{song_no}"
                                    })
                                    
                            except ValueError as e:
                                logger.warning(f"Could not parse date '{date_text}': {e}")
                                continue
                    
                    except Exception as e:
                        logger.error(f"Error parsing song entry: {e}")
                        continue
                
                logger.info(f"Found {len(new_releases)} new Melon releases for {artist_name}")
                
        except aiohttp.ClientError as e:
            logger.error(f"Network error scraping Melon for {artist_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error scraping Melon for {artist_name}: {e}")
        
        return new_releases
    
    async def get_artist_info(self, artist_id: str) -> dict:
        """
        Get additional artist information from Melon
        
        Args:
            artist_id: Melon artist ID
            
        Returns:
            Dictionary with artist info
        """
        artist_url = f"https://www.melon.com/artist/timeline.htm?artistId={artist_id}"
        
        try:
            session = await self.get_session()
            
            async with session.get(artist_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    return {}
                
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                # Extract artist name
                artist_name_elem = soup.find('p', {'class': 'title_atist'})
                artist_name = artist_name_elem.get_text(strip=True) if artist_name_elem else None
                
                # Extract artist image
                artist_img_elem = soup.find('div', {'class': 'wrap_atistimg'})
                if artist_img_elem:
                    img_elem = artist_img_elem.find('img')
                    artist_image = img_elem.get('src') if img_elem else None
                else:
                    artist_image = None
                
                return {
                    'name': artist_name,
                    'image': artist_image
                }
                
        except Exception as e:
            logger.error(f"Error getting artist info: {e}")
            return {}


# Standalone test function
async def test_melon_scraper():
    """Test the Melon scraper"""
    scraper = MelonScraper()
    
    try:
        # Test with Loopy
        releases = await scraper.scrape_artist_songs(
            "https://www.melon.com/artist/song.htm?artistId=1908520",
            "Loopy"
        )
        
        print(f"Found {len(releases)} releases:")
        for release in releases:
            print(f"  - {release['title']} ({release['release_date']})")
            
    finally:
        await scraper.close_session()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_melon_scraper())

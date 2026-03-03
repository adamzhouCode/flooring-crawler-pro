from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
import sys

def test_manual_search():
    print("🔎 Starting Manual DuckDuckGo Lite Test: 'Shanghai Flooring Wholesale'...")
    url = "https://duckduckgo.com/lite/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    try:
        data = {"q": "Shanghai Flooring Wholesale"}
        # Use curl_cffi to impersonate a real browser
        resp = curl_requests.post(url, data=data, headers=headers, impersonate="chrome110", timeout=15)
        
        if resp.status_code != 200:
            print(f"❌ ERROR: HTTP {resp.status_code}")
            return False
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []
        for a in soup.find_all('a', class_='result-link'):
            link = a.get('href')
            if link and link.startswith('http'):
                results.append(link)
                print(f"✅ Found: {link}")

        if not results:
            print("❌ ERROR: Manual scraper found 0 results. The page structure might have changed or you are blocked.")
            return False
            
        print(f"\n✨ SUCCESS: Found {len(results)} live results via Manual Scraper.")
        return True
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_manual_search()
    sys.exit(0 if success else 1)

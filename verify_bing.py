import requests
import sys

def test_bing_api(api_key):
    print(f"🔎 Testing Bing API with Key: {api_key[:5]}***")
    endpoint = "https://api.bing.microsoft.com/v7.0/search"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"q": "Shanghai Flooring Wholesale", "count": 5}
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            pages = data.get("webPages", {}).get("value", [])
            print(f"✅ SUCCESS: Found {len(pages)} live results from Bing.")
            for i, page in enumerate(pages):
                print(f"   {i+1}. {page['name']} -> {page['url']}")
            return True
        elif response.status_code == 401:
            print("❌ ERROR: 401 Unauthorized. Your Bing API Key is invalid.")
        else:
            print(f"❌ ERROR: HTTP {response.status_code} - {response.text}")
        return False
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python verify_bing.py YOUR_BING_API_KEY")
    else:
        test_bing_api(sys.argv[1])

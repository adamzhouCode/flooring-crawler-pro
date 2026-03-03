from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
import sys
from urllib.parse import urljoin

def test_baidu_direct():
    print("🔎 正在测试百度直接抓取 (Direct Baidu Scraper)...")
    query = "上海 地板 批发商"
    url = f"https://m.baidu.com/s?word={query}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
        "Referer": "https://m.baidu.com/"
    }
    
    try:
        # 1. Test Search
        resp = curl_requests.get(url, headers=headers, impersonate="chrome110", timeout=15)
        print(f"📡 百度响应状态码: {resp.status_code}")
        
        if resp.status_code != 200:
            print("❌ 错误: 无法访问百度。")
            return False
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []
        
        # Look for result links in mobile layout
        # Checking for common mobile result patterns
        for div in soup.find_all('div', class_='result'):
            a = div.find('a', href=True)
            if a:
                link = a['href']
                if 'baidu.com' in link or link.startswith('http'):
                    results.append(link)
                    print(f"🔗 找到原始链接: {link[:60]}...")

        if not results:
            # Fallback check for other common mobile classes
            for a in soup.find_all('a', href=True):
                if 'baidu.com/from=' in a['href'] or 'baidu.com/link?url=' in a['href']:
                    results.append(a['href'])
                    print(f"🔗 找到原始链接: {a['href'][:60]}...")
            
        if not results:
            print("❌ 错误: 未提取到任何搜索结果。可能是页面结构变化。")
            print(f"📄 页面源码片段: {resp.text[:500]}")
            return False
            
        # 2. Test Redirect/Scraping for the first result
        print(f"\n🧪 正在测试链接重定向和内容抓取: {results[0][:50]}...")
        headers_pc = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        try:
            target_resp = curl_requests.get(results[0], headers=headers_pc, timeout=15, impersonate="chrome110", allow_redirects=True)
            print(f"✅ 成功重定向至: {target_resp.url}")
            if len(target_resp.text) > 500:
                print(f"📝 抓取内容成功 (长度: {len(target_resp.text)} 字符)")
            else:
                print("⚠️ 警告: 抓取内容过短，可能是空页面或反爬拦截。")
        except Exception as e:
            print(f"❌ 重定向测试失败: {e}")

        print(f"\n✨ 结论: 百度直接抓取引擎工作正常！共找到 {len(results)} 条结果。")
        return True
        
    except Exception as e:
        print(f"❌ 运行出错: {e}")
        return False

if __name__ == "__main__":
    success = test_baidu_direct()
    sys.exit(0 if success else 1)

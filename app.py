import streamlit as st
import pandas as pd
import json
import time
import os
import re
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

# Scraper & Search
import requests
from bs4 import BeautifulSoup
import trafilatura
from curl_cffi import requests as curl_requests

# AI Models
from google import genai
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def get_secret(key: str, default: str = "") -> str:
    """Helper to get secret from streamlit secrets or env vars"""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except:
        pass
    return os.getenv(key, default)

def check_password():
    """Returns True if the user had the correct password."""
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == get_secret("APP_PASSWORD", "admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input("登录密码", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input + error.
        st.text_input("登录密码", type="password", on_change=password_entered, key="password")
        st.error("😕 密码错误")
        return False
    else:
        # Password correct.
        return True

# --- 行业预设配置 (INDUSTRY PRESETS) ---
INDUSTRY_PRESETS = {
    "地板经销商 (Distributors)": {
        "queries": ['{city}地板经销商'],
        "persona": "高级采购经理",
        "focus": "寻找位于{city}的地板品牌商和经销商（非生产工厂）。关注：代理的品牌、经销区域、批发能力、联系方式。排除：地板生产工厂、制造商（这些是我们的竞争对手）。【重要】该企业必须位于或服务于{city}，如果企业明确属于其他城市/省份，relevance_score 必须 ≤ 3。"
    },
    "地板零售门店 (Retailers)": {
        "queries": ['{city}地板专卖店'],
        "persona": "客户经理",
        "focus": "寻找位于{city}的独立地板零售门店或建材市场中的地板商户。关注：经营品牌、门店地址、联系方式。评估其引入新品牌的意愿。排除：地板工厂直营店。【重要】门店必须位于{city}，如果明确属于其他城市/省份，relevance_score 必须 ≤ 3。"
    },
    "房地产开发商 (Developers)": {
        "queries": ['{city}房地产开发商 精装修'],
        "persona": "供应链管理专家",
        "focus": "寻找在{city}有在建或规划住宅/商业项目的房地产开发商。关注：项目规模、精装修楼盘（需要集采地板）、采购部联系方式。【重要】项目必须位于{city}，如果明确属于其他城市/省份，relevance_score 必须 ≤ 3。"
    },
    "装饰装修公司 (Decoration)": {
        "queries": ['{city}装饰公司 地板'],
        "persona": "合作伙伴经理",
        "focus": "寻找位于{city}的承接精装修项目的装饰设计公司（非地板工厂）。关注：项目案例中是否涉及地板选材、合作品牌、项目规模和合作联系方式。【重要】企业必须位于{city}，如果明确属于其他城市/省份，relevance_score 必须 ≤ 3。"
    },
    "地板施工安装 (Contractors)": {
        "queries": ['{city}地板铺装 施工'],
        "persona": "项目合作经理",
        "focus": "寻找位于{city}的承接地面铺装工程的施工企业（非地板生产商）。关注：工程资质、过往项目规模、材料采购渠道和联系方式。【重要】企业必须位于{city}，如果明确属于其他城市/省份，relevance_score 必须 ≤ 3。"
    }
}

# 行业关键词预筛（页面必须包含至少一个才送AI分析）
FLOORING_KEYWORDS = ['地板', '木地板', '地面', '地砖', '地材', '铺装', '建材', 'flooring', 'floor', '装修', '装饰', '房地产', '楼盘', '开发商']

# URL 过滤：模式匹配 + 黑名单（替代逐个域名打地鼠）
# 域名中包含这些关键词的直接跳过（政府、新闻、门户、平台）
SKIP_DOMAIN_PATTERNS = [
    'gov.cn', '.gov.', 'news', 'baike', 'wiki',
    'zhihu.com', 'douban.com', 'bilibili.com', 'toutiao.com',
    'jianshu.com', 'csdn.net', 'weibo.com', 'qq.com',
    'sohu.com', 'sina.com', '163.com', 'ifeng.com',
    'baidu.com', 'map.baidu', 'tieba.baidu',
    'cnr.cn', 'cctv.com', 'people.com.cn',
    'customs.gov', 'stats.gov',
]

# 非企业官网的域名后缀/特征
BLACKLISTED_DOMAINS = {
    'bjnews.com.cn', 'chinafloor.cn', 'chinatimber.org', 'zhilengwang.cn',
    'shzh.net', 'zol.com.cn', '360che.com', 'pchouse.com.cn',
    'chery.cn', 'epson.com.cn', 'ciwf.com.cn',
}

def is_url_blacklisted(url: str) -> bool:
    """Check if URL is from a non-business site (news, gov, portal, etc.)"""
    domain = urlparse(url).netloc.lstrip('www.').lower()
    # Pattern matching — catches all gov.cn subdomains, news sites, etc.
    if any(pat in domain for pat in SKIP_DOMAIN_PATTERNS):
        return True
    # Exact domain blacklist
    if any(domain == bd or domain.endswith('.' + bd) for bd in BLACKLISTED_DOMAINS):
        return True
    return False

# --- 核心引擎类 ---

class GlobalRateLimiter:
    def __init__(self, daily_limit: int = 500):
        self.lock = threading.Lock()
        self.daily_limit = daily_limit
        self.count = 0
        self.date = datetime.now().date()

    def check(self) -> bool:
        with self.lock:
            current_date = datetime.now().date()
            if current_date > self.date:
                self.count = 0
                self.date = current_date
            if self.count >= self.daily_limit:
                return False
            self.count += 1
            return True

    def get_status(self):
        return self.count, self.daily_limit

@st.cache_resource
def get_limiter():
    return GlobalRateLimiter(daily_limit=10)

class SearchEngine:
    @staticmethod
    def search_google(query: str, api_key: str, cx: str, max_results: int = 10) -> List[str]:
        """使用 Google Custom Search API，自动分页，限定中文/中国结果"""
        url = "https://www.googleapis.com/customsearch/v1"
        all_links = []
        # Google CSE allows max 10 per request, paginate with start param
        for start in range(1, max_results + 1, 10):
            num = min(10, max_results - start + 1)
            params = {
                "key": api_key,
                "cx": cx,
                "q": query,
                "num": num,
                "start": start,
                "lr": "lang_zh-CN",    # Only Simplified Chinese pages
                "gl": "cn",            # Boost results from China
                "cr": "countryCN",     # Restrict to documents originating in China
                "hl": "zh-CN",         # Interface language (improves relevance)
            }
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                items = data.get("items", [])
                all_links.extend([item['link'] for item in items])
                if len(items) < num:
                    break  # No more results
            except Exception as e:
                st.error(f"Google 搜索失败 (start={start}): {e}")
                break
        return all_links

    @staticmethod
    def search_google_multi(queries: List[str], api_key: str, cx: str, max_results: int = 10) -> List[str]:
        """Run multiple simple queries and merge/dedup results"""
        all_links = []
        seen = set()
        per_query = max(5, max_results // len(queries)) if queries else max_results
        for q in queries:
            results = SearchEngine.search_google(q, api_key, cx, per_query)
            for link in results:
                if link not in seen:
                    seen.add(link)
                    all_links.append(link)
        return all_links[:max_results]

    @staticmethod
    def search_brave(query: str, api_key: str, max_results: int = 10) -> List[str]:
        """使用 Brave Search API (备选方案)"""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {"Accept": "application/json", "X-Subscription-Token": api_key}
        params = {"q": query, "count": max_results}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            data = response.json()
            return [v['url'] for v in data.get("web", {}).get("results", [])]
        except Exception as e:
            st.error(f"Brave 搜索失败: {e}")
            return []

class Scraper:
    @staticmethod
    def get_deep_context(url: str, depth: int = 2) -> str:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        try:
            resp = curl_requests.get(url, headers=headers, timeout=15, impersonate="chrome110", allow_redirects=True)
            if resp.status_code != 200: return f"[CRAWL_ERROR] HTTP {resp.status_code}"
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            text_bundle = f"=== 来源网址: {resp.url} ===\n"

            # Extract contact info early and place at top
            visible_text = soup.get_text()
            emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', visible_text))
            phones = set(re.findall(r'400-\d{3}-\d{4}|400\d{7}|\+?86-?1[3-9]\d{9}|1[3-9]\d{9}|\d{3,4}-\d{7,8}|\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', visible_text))
            if emails or phones:
                text_bundle += f"=== 原始联系信息 ===\n邮箱: {', '.join(emails)}\n电话: {', '.join(phones)}\n\n"

            main_text = trafilatura.extract(resp.content)
            if main_text and len(main_text) > 200:
                text_bundle += main_text
            else:
                content_parts = [tag.get_text(strip=True) for tag in soup.find_all(['h1', 'h2', 'h3', 'p', 'article']) if len(tag.get_text(strip=True)) > 10]
                text_bundle += "\n".join(content_parts[:40])
            
            if depth > 1:
                sub_links = []
                for a in soup.find_all('a', href=True):
                    t = a.get_text()
                    if any(kw in t for kw in ['About', 'Contact', 'Project', 'Products', 'Cases', 'Services', '关于', '联系', '项目', '产品', '案例', '工程', '合作', '服务']):
                        sub_links.append(urljoin(resp.url, a['href']))
                
                for sub_url in list(set(sub_links))[:3]:
                    try:
                        sub_resp = curl_requests.get(sub_url, headers=headers, timeout=8, impersonate="chrome110")
                        sub_text = trafilatura.extract(sub_resp.content) or sub_resp.text[:500]
                        text_bundle += f"\n\n--- 子页面 ({sub_url}) ---\n{sub_text[:1000]}"
                    except: continue
                    
            return text_bundle
        except Exception as e: return f"[CRAWL_ERROR] 抓取错误: {e}"

class AIBrain:
    def __init__(self, provider: str, api_key: str, model_name: str, base_url: Optional[str] = None):
        self.provider, self.api_key, self.model_name, self.base_url = provider, api_key, model_name, base_url

    def analyze(self, text: str, persona: str, focus: str) -> Dict:
        system_prompt = (
            f"你是一位{persona}，专门负责为地板行业寻找高价值商业线索。\n"
            f"分析重点：{focus}\n\n"
            "## 评分标准\n"
            "deal_score（成交潜力）：\n"
            "  8-10 = 直接地板相关企业，有明确联系方式\n"
            "  5-7 = 相关行业（建材、装修），可能有合作机会\n"
            "  3-4 = 间接相关，线索价值低\n"
            "  0-2 = 完全无关\n\n"
            "relevance_score（行业相关度）：\n"
            "  8-10 = 核心地板业务\n"
            "  5-7 = 邻近行业（建筑、房地产、装修）\n"
            "  0-4 = 无关行业\n\n"
            "## 示例\n"
            "输入：上海XX地板有限公司，主营实木地板批发，联系电话 021-55551234，邮箱 sales@xxfloor.com\n"
            "输出：{\"company_name\": \"上海XX地板有限公司\", \"email\": \"sales@xxfloor.com\", "
            "\"phone\": \"021-55551234\", \"relevance_score\": 9, \"deal_score\": 9, "
            "\"summary\": \"实木地板批发商，有完整联系方式\", \"why\": \"核心地板批发业务，直接联系方式齐全，高价值线索\"}\n\n"
            "## 要求\n"
            "返回严格 JSON 格式：{\"company_name\": \"\", \"email\": \"\", \"phone\": \"\", "
            "\"relevance_score\": 0-10, \"deal_score\": 0-10, \"summary\": \"\", \"why\": \"\"}\n\n"
            "## 关于 company_name\n"
            "- 优先从页面内容中提取完整公司名（如 XX有限公司）\n"
            "- 如果页面没有完整公司名，从网站品牌名/域名推断（如域名 artreefloor.com → 雅树地板）\n"
            "- 如果是新闻资讯、行业门户、价格行情等非企业官网页面，company_name 填空字符串\n"
            "- 只有确实无法判断所属企业时才留空"
        )
        # Extract URL from text bundle for AI context
        url_line = text[:200].split('\n')[0] if text else ""
        user_prompt = f"请分析以下网站内容并返回 JSON。\n来源: {url_line}\n\n{text[:8000]}"
        try:
            if self.provider == "Gemini":
                client = genai.Client(api_key=self.api_key)
                combined_prompt = system_prompt + "\n\n" + user_prompt
                response = client.models.generate_content(model=self.model_name, contents=combined_prompt, config={'response_mime_type': 'application/json'})
                return json.loads(response.text)
            elif self.provider in ["DeepSeek", "OpenAI", "Custom"]:
                client = OpenAI(api_key=self.api_key, base_url=self.base_url)
                response = client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"}
                )
                return json.loads(response.choices[0].message.content)
        except Exception as e: return {"error": str(e)}

# --- UI ---

st.set_page_config(page_title="地板爬虫", layout="wide", page_icon="�")

if not check_password():
    st.stop()

st.title("� 地板爬虫：专业智能拓客引擎")

with st.sidebar:
    st.header("⚙️ 搜索配置")
    engine_choice = st.selectbox("搜索引擎", ["Google Search API (首选)", "Brave API"])
    
    if engine_choice == "Google Search API (首选)":
        google_api_key = st.text_input("Google API Key", value=get_secret("GOOGLE_API_KEY"), type="password")
        google_cx = st.text_input("Search Engine ID (CX)", value=get_secret("GOOGLE_CX"), type="password")
        
    else:
        search_api_key = st.text_input("Brave API Key", value=get_secret("BRAVE_API_KEY"), type="password")
        st.caption("[获取 Brave API 密钥](https://api.search.brave.com/)")
    
    st.divider()
    st.header("⚙️ AI 分析设置")
    provider = st.selectbox("AI 服务商", ["DeepSeek", "Gemini", "OpenAI"])
    
    # 动态设置默认值
    if provider == "DeepSeek":
        default_key = get_secret("DEEPSEEK_API_KEY")
        default_model = "deepseek-chat"
        default_url = "https://api.deepseek.com"
    elif provider == "Gemini":
        default_key = get_secret("GEMINI_API_KEY", get_secret("GOOGLE_AI_API_KEY"))
        default_model = "gemini-2.0-flash"
        default_url = ""
    else: # OpenAI
        default_key = get_secret("OPENAI_API_KEY")
        default_model = "gpt-4o"
        default_url = "https://api.openai.com/v1"

    ai_api_key = st.text_input("AI API Key", value=default_key, type="password")
    custom_model = st.text_input("模型", value=default_model)
    
    # 只有部分服务商显示 Base URL 选项
    if provider in ["DeepSeek", "OpenAI"]:
        base_url = st.text_input("Base URL (选填)", value=default_url)
    else:
        base_url = None
    
    st.divider()
    max_results = st.slider("搜索结果数量", 5, 50, 10)
    crawl_depth = st.slider("抓取层级", 1, 3, 2)
    show_raw = st.checkbox("显示抓取原文 (调试)")

col1, col2 = st.columns([1, 1])
with col1:
    industry = st.selectbox("目标行业", list(INDUSTRY_PRESETS.keys()))
    city = st.text_input("目标城市", value="上海", placeholder="如：上海、广州")
with col2:
    default_query = INDUSTRY_PRESETS[industry]["queries"][0]
    search_template = st.text_input("搜索指令", value=default_query)
    final_query = search_template.format(city=city) if city else ""
    query_list = [final_query] if final_query else []
    if final_query:
        st.info(f"搜索指令: {final_query}")

if st.button("🚀 开始自动化拓客任务", use_container_width=True):
    is_admin = st.session_state.get("password_correct", False)
    limiter = get_limiter()
    if not is_admin and not limiter.check():
        st.error(f"❌ 已达到当日全局搜索上限 ({limiter.daily_limit})。请明天再试或联系管理员。")
    elif not ai_api_key:
        st.error("请输入 AI API 密钥。")
    elif engine_choice == "Google Search API (首选)" and (not google_api_key or not google_cx):
        st.error("请完整填写 Google API Key 和 CX ID。")
    elif engine_choice == "Brave API" and not search_api_key:
        st.error("请输入 Brave API Key。")
    elif not city: st.error("请输入城市。")
    else:
        with st.status(f"正在通过 {engine_choice} 执行 {len(query_list)} 条搜索指令...") as status:
            # Over-fetch 3x to compensate for filtering losses
            fetch_count = min(max_results * 3, 100)  # Google CSE caps at 100
            if engine_choice == "Google Search API (首选)":
                raw_urls = SearchEngine.search_google_multi(query_list, google_api_key, google_cx, fetch_count)
            else:
                raw_urls = []
                seen = set()
                per_q = max(5, fetch_count // len(query_list)) if query_list else fetch_count
                for q in query_list:
                    for u in SearchEngine.search_brave(q, search_api_key, per_q):
                        if u not in seen:
                            seen.add(u)
                            raw_urls.append(u)

            # Pre-filter: blacklist + domain dedup BEFORE analysis
            seen_domains = set()
            urls = []
            filtered_count = 0
            for u in raw_urls:
                if is_url_blacklisted(u):
                    filtered_count += 1
                    continue
                domain = urlparse(u).netloc
                if domain in seen_domains:
                    continue
                seen_domains.add(domain)
                urls.append(u)
                if len(urls) >= max_results:
                    break

            status.update(
                label=f"搜索完成！获取 {len(raw_urls)} → 过滤后 {len(urls)} 个有效目标（跳过 {filtered_count} 个非企业站）",
                state="complete" if urls else "error"
            )

        if urls:

            st.divider()
            brain = AIBrain(provider, ai_api_key, custom_model, base_url)
            persona = INDUSTRY_PRESETS[industry]["persona"]
            focus = INDUSTRY_PRESETS[industry]["focus"].format(city=city)
            progress_bar = st.progress(0, text=f"并行分析中... 0/{len(urls)}")

            def process_url(url):
                """Scrape and analyze a single URL (runs in thread). URLs already pre-filtered."""
                context = Scraper.get_deep_context(url, depth=crawl_depth)
                result = {"url": url, "context": context, "analysis": None, "skip_reason": None}
                if not context or context.startswith("[CRAWL_ERROR]"):
                    result["skip_reason"] = "抓取失败"
                elif len(context) <= 200:
                    result["skip_reason"] = "内容过短"
                elif not any(kw in context for kw in FLOORING_KEYWORDS):
                    result["skip_reason"] = "内容无行业关键词"
                else:
                    analysis = brain.analyze(context, persona, focus)
                    if "error" in analysis:
                        result["skip_reason"] = f"AI分析失败: {analysis.get('error', '')[:60]}"
                    else:
                        result["analysis"] = analysis
                return result

            # Run scraping + AI analysis in parallel (5 workers)
            max_workers = min(5, len(urls))
            results = []
            completed = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_url, url): url for url in urls}
                for future in as_completed(futures):
                    results.append(future.result())
                    completed += 1
                    progress_bar.progress(completed / len(urls), text=f"并行分析中... {completed}/{len(urls)}")

            progress_bar.progress(1.0, text="分析完成!")

            # Filter and store results in session_state
            leads_data = []
            raw_contexts = []
            funnel = {"total": len(raw_urls), "blacklisted": filtered_count, "crawl_fail": 0, "too_short": 0, "no_keyword": 0, "ai_fail": 0, "no_name": 0, "low_score": 0, "duplicate": 0, "accepted": 0}
            skipped_details = []
            seen_companies = set()

            for r in results:
                raw_contexts.append({"url": r["url"], "context": r["context"]})

                if r["skip_reason"]:
                    if "抓取失败" in r["skip_reason"]:
                        funnel["crawl_fail"] += 1
                    elif "内容过短" in r["skip_reason"]:
                        funnel["too_short"] += 1
                    elif "无行业关键词" in r["skip_reason"]:
                        funnel["no_keyword"] += 1
                    else:
                        funnel["ai_fail"] += 1
                    skipped_details.append({"url": r["url"], "reason": r["skip_reason"]})
                    continue

                analysis = r["analysis"]
                try:
                    deal_score = int(float(analysis.get('deal_score', 0)))
                    relevance_score = int(float(analysis.get('relevance_score', 0)))
                except (ValueError, TypeError):
                    deal_score, relevance_score = 0, 0

                company_name = analysis.get('company_name', '').strip()
                if not company_name:
                    funnel["no_name"] += 1
                    skipped_details.append({"url": r["url"], "reason": "AI未识别公司名"})
                    continue

                # Company-name-level dedup
                name_key = company_name.lower().replace(' ', '')
                if name_key in seen_companies:
                    funnel["duplicate"] += 1
                    skipped_details.append({"url": r["url"], "reason": f"重复公司: {company_name}"})
                    continue
                seen_companies.add(name_key)

                analysis['url'] = r['url']
                if deal_score > 2 and relevance_score >= 4:
                    funnel["accepted"] += 1
                    leads_data.append(analysis)
                else:
                    funnel["low_score"] += 1
                    skipped_details.append({"url": r["url"], "reason": f"评分过低 (deal={deal_score}, relevance={relevance_score})"})

            st.session_state["leads_data"] = leads_data
            st.session_state["raw_contexts"] = raw_contexts
            st.session_state["search_urls"] = urls
            st.session_state["result_city"] = city
            st.session_state["funnel"] = funnel
            st.session_state["skipped_details"] = skipped_details
            st.rerun()

# --- Display persisted results ---
if "leads_data" in st.session_state:
    urls = st.session_state["search_urls"]
    leads_data = st.session_state["leads_data"]
    raw_contexts = st.session_state["raw_contexts"]
    result_city = st.session_state["result_city"]

    # Diagnostic funnel
    if "funnel" in st.session_state:
        f = st.session_state["funnel"]
        with st.expander(f"📊 分析漏斗: {f['total']} 个 URL → {f['accepted']} 个有效线索"):
            cols = st.columns(4)
            cols[0].metric("非企业站", f.get("blacklisted", 0))
            cols[1].metric("抓取失败", f["crawl_fail"])
            cols[2].metric("内容过短/无行业词", f["too_short"] + f.get("no_keyword", 0))
            cols[3].metric("AI失败", f["ai_fail"])
            cols2 = st.columns(4)
            cols2[0].metric("无公司名", f["no_name"])
            cols2[1].metric("评分过低", f["low_score"])
            cols2[2].metric("重复公司", f.get("duplicate", 0))
            cols2[3].metric("有效线索", f["accepted"])

            if st.session_state.get("skipped_details"):
                st.caption("被过滤的 URL:")
                for s in st.session_state["skipped_details"]:
                    st.text(f"  {s['reason']}: {s['url'][:80]}")

    with st.expander(f"🛠️ 调试: 搜索到的 URL 列表 ({len(urls)} 个)"):
        for i, u in enumerate(urls): st.write(f"{i+1}. {u}")

    if show_raw:
        for rc in raw_contexts:
            if rc["context"]:
                with st.expander(f"原文: {rc['url']}"): st.text(rc["context"])

    if leads_data:
        for a in leads_data:
            company = a.get('company_name', '')
            score = a.get('deal_score', '?')
            st.success(f"💎 成功识别: **{company}** (潜力评分: {score}/10)")

        st.divider()
        st.header("📋 拓客报表")
        df = pd.DataFrame(leads_data).rename(columns={'company_name': '名称', 'deal_score': '潜力', 'summary': '业务', 'email': '邮箱', 'phone': '电话', 'url': '网址', 'why': '结论'})
        st.dataframe(df.sort_values(by='潜力', ascending=False), use_container_width=True)
        import io
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer: df.to_excel(writer, index=False)
        st.download_button("📥 导出 Excel", buffer.getvalue(), f"线索_{result_city}.xlsx")
    else:
        st.error("未发现有效线索。")

    if st.button("🗑️ 清除结果", use_container_width=True):
        for key in ["leads_data", "raw_contexts", "search_urls", "result_city", "funnel", "skipped_details"]:
            st.session_state.pop(key, None)
        st.rerun()

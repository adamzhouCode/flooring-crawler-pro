import streamlit as st
import pandas as pd
import json
import time
import os
import re
import random
import threading
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
    "地板批发/工厂 (Wholesale)": {
        "query": "{city} 地板批发商 厂家",
        "persona": "高级采购经理",
        "focus": "寻找当地库存、工厂直发意向以及品牌代理资质。"
    },
    "房地产开发商 (Developers)": {
        "query": "{city} 房地产开发商 住宅项目",
        "persona": "供应链管理专家",
        "focus": "关注大型住宅项目和材料集采需求。"
    },
    "装修/设计公司 (Design Firms)": {
        "query": "{city} 装修公司 室内设计",
        "persona": "合作伙伴经理",
        "focus": "寻找在商业或高端住宅项目中指定材料的设计公司。"
    },
    "地板零售/门店 (Retailers)": {
        "query": "{city} 地板店 展厅",
        "persona": "客户经理",
        "focus": "识别愿意经营新型工厂直销品牌的独立零售店。"
    }
}

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
        """使用 Google Custom Search API (最专业、最准确)"""
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": max_results
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return [item['link'] for item in data.get("items", [])]
        except Exception as e:
            st.error(f"Google 搜索失败: {e}")
            return []

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
            "\"relevance_score\": 0-10, \"deal_score\": 0-10, \"summary\": \"\", \"why\": \"\"}\n"
            "如果找不到公司名称，company_name 填空字符串。"
        )
        user_prompt = f"请分析以下网站内容并返回 JSON：\n\n{text[:8000]}"
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

st.set_page_config(page_title="精工铺地 Pro", layout="wide", page_icon="🏗️")

if not check_password():
    st.stop()

st.title("🏗️ 精工铺地 Pro：专业拓客引擎")

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
    search_template = st.text_input("指令模板", value=INDUSTRY_PRESETS[industry]["query"])
    final_query = search_template.format(city=city) if city else ""
    st.info(f"搜索指令: {final_query}")

if st.button("🚀 开始自动化拓客任务", use_container_width=True):
    limiter = get_limiter()
    if not limiter.check():
        st.error(f"❌ 已达到当日全局搜索上限 ({limiter.daily_limit})。请明天再试或联系管理员。")
    elif not ai_api_key:
        st.error("请输入 AI API 密钥。")
    elif engine_choice == "Google Search API (首选)" and (not google_api_key or not google_cx):
        st.error("请完整填写 Google API Key 和 CX ID。")
    elif engine_choice == "Brave API" and not search_api_key:
        st.error("请输入 Brave API Key。")
    elif not city: st.error("请输入城市。")
    else:
        with st.status(f"正在通过 {engine_choice} 获取全球最精准目标...") as status:
            if engine_choice == "Google Search API (首选)":
                urls = SearchEngine.search_google(final_query, google_api_key, google_cx, max_results)
            else:
                urls = SearchEngine.search_brave(final_query, search_api_key, max_results)
            status.update(label=f"搜索完成！发现 {len(urls)} 个高价值目标。", state="complete" if urls else "error")
        
        if urls:
            # Deduplicate by domain (keep first occurrence)
            seen_domains = set()
            unique_urls = []
            for u in urls:
                domain = urlparse(u).netloc
                if domain not in seen_domains:
                    seen_domains.add(domain)
                    unique_urls.append(u)
            urls = unique_urls

            with st.expander("🛠️ 调试: 搜索到的原始 URL 列表"):
                for i, u in enumerate(urls): st.write(f"{i+1}. {u}")

            st.divider()
            leads_data, log_container, log_messages = [], st.empty(), []
            progress_bar, brain = st.progress(0), AIBrain(provider, ai_api_key, custom_model, base_url)

            for i, url in enumerate(urls):
                log_messages.append(f"🔍 分析中 ({i+1}/{len(urls)}): {url[:40]}...")
                log_container.code("\n".join(log_messages[-3:]))

                context = Scraper.get_deep_context(url, depth=crawl_depth)
                if show_raw:
                    with st.expander(f"原文: {url}"): st.text(context)

                if context and not context.startswith("[CRAWL_ERROR]") and len(context) > 200:
                    analysis = brain.analyze(context, INDUSTRY_PRESETS[industry]["persona"], INDUSTRY_PRESETS[industry]["focus"])
                    if "error" not in analysis:
                        # Safe score parsing
                        try:
                            deal_score = int(float(analysis.get('deal_score', 0)))
                            relevance_score = int(float(analysis.get('relevance_score', 0)))
                        except (ValueError, TypeError):
                            deal_score, relevance_score = 0, 0

                        # Skip if no company name
                        company_name = analysis.get('company_name', '').strip()
                        if not company_name:
                            continue

                        analysis['url'] = url
                        if deal_score > 2 and relevance_score >= 4:
                            leads_data.append(analysis)
                            st.success(f"💎 成功识别: **{company_name}** (潜力评分: {deal_score}/10)")

                progress_bar.progress((i + 1) / len(urls))
                time.sleep(random.uniform(0.3, 1.0))

            if leads_data:
                st.divider()
                st.header("📋 拓客报表")
                df = pd.DataFrame(leads_data).rename(columns={'company_name': '名称', 'deal_score': '潜力', 'summary': '业务', 'email': '邮箱', 'phone': '电话', 'url': '网址', 'why': '结论'})
                st.dataframe(df.sort_values(by='潜力', ascending=False), use_container_width=True)
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer: df.to_excel(writer, index=False)
                st.download_button("📥 导出 Excel", buffer.getvalue(), f"线索_{city}.xlsx")
            else: st.error("未发现有效线索。")

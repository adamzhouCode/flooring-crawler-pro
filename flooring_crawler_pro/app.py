import streamlit as st
import pandas as pd
import json
import time
import os
import re
import random
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

# --- 行业预设配置 (INDUSTRY PRESETS) ---
INDUSTRY_PRESETS = {
    "地板批发/工厂 (Wholesale)": {
        "query": "{city} flooring wholesalers distributors flooring brands",
        "persona": "高级采购经理",
        "focus": "寻找当地库存、工厂直发意向以及品牌代理资质。"
    },
    "房地产开发商 (Developers)": {
        "query": "{city} real estate developers residential multifamily projects",
        "persona": "供应链管理专家",
        "focus": "关注大型住宅项目和材料集采需求。"
    },
    "装修/设计公司 (Design Firms)": {
        "query": "{city} interior design firms decoration companies commercial",
        "persona": "合作伙伴经理",
        "focus": "寻找在商业或高端住宅项目中指定材料的设计公司。"
    },
    "地板零售/门店 (Retailers)": {
        "query": "{city} flooring stores retailers showrooms vinyl laminate",
        "persona": "客户经理",
        "focus": "识别愿意经营新型工厂直销品牌的独立零售店。"
    }
}

# --- 核心引擎类 ---

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
            if resp.status_code != 200: return f"Error: HTTP {resp.status_code}"
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            text_bundle = f"=== 来源网址: {resp.url} ===\n"
            
            main_text = trafilatura.extract(resp.content)
            if main_text and len(main_text) > 200:
                text_bundle += main_text
            else:
                content_parts = [tag.get_text(strip=True) for tag in soup.find_all(['h1', 'h2', 'h3', 'p', 'article']) if len(tag.get_text(strip=True)) > 10]
                text_bundle += "\n".join(content_parts[:40])
            
            emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resp.text))
            phones = set(re.findall(r'1[3-9]\d{9}|\d{3,4}-\d{7,8}|\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', resp.text))
            if emails or phones:
                text_bundle += f"\n\n=== 原始联系信息 ===\n邮箱: {', '.join(emails)}\n电话: {', '.join(phones)}\n"
            
            if depth > 1:
                sub_links = []
                for a in soup.find_all('a', href=True):
                    t = a.get_text()
                    if any(kw in t for kw in ['About', 'Contact', 'Project', '关于', '联系', '项目']):
                        sub_links.append(urljoin(resp.url, a['href']))
                
                for sub_url in list(set(sub_links))[:depth-1]:
                    try:
                        sub_resp = curl_requests.get(sub_url, headers=headers, timeout=8, impersonate="chrome110")
                        sub_text = trafilatura.extract(sub_resp.content) or sub_resp.text[:500]
                        text_bundle += f"\n\n--- 子页面 ({sub_url}) ---\n{sub_text[:1000]}"
                    except: continue
                    
            return text_bundle
        except Exception as e: return f"抓取错误: {e}"

class AIBrain:
    def __init__(self, provider: str, api_key: str, model_name: str, base_url: Optional[str] = None):
        self.provider, self.api_key, self.model_name, self.base_url = provider, api_key, model_name, base_url

    def analyze(self, text: str, persona: str, focus: str) -> Dict:
        prompt = f"你是一位{persona}。任务：分析以下网站内容。重点：{focus}\n要求 JSON 返回：{{'company_name': '...', 'email': '...', 'phone': '...', 'relevance_score': 0-10, 'deal_score': 0-10, 'summary': '...', 'why': '...'}}\n内容: {text[:8000]}"
        try:
            if self.provider == "Gemini":
                client = genai.Client(api_key=self.api_key)
                response = client.models.generate_content(model=self.model_name, contents=prompt, config={'response_mime_type': 'application/json'})
                return json.loads(response.text)
            elif self.provider in ["DeepSeek", "OpenAI", "Custom"]:
                client = OpenAI(api_key=self.api_key, base_url=self.base_url)
                response = client.chat.completions.create(model=self.model_name, messages=[{"role": "user", "content": prompt}], response_format={"type": "json_object"})
                return json.loads(response.choices[0].message.content)
        except Exception as e: return {"error": str(e)}

# --- UI ---

st.set_page_config(page_title="精工铺地 Pro", layout="wide", page_icon="🏗️")
st.title("🏗️ 精工铺地 Pro：专业拓客引擎")

with st.sidebar:
    st.header("⚙️ 搜索配置")
    engine_choice = st.selectbox("搜索引擎", ["Google Search API (首选)", "Brave API"])
    
    if engine_choice == "Google Search API (首选)":
        google_api_key = st.text_input("Google API Key", type="password")
        google_cx = st.text_input("Search Engine ID (CX)", type="password")
        st.caption("[获取 Google API 密钥](https://developers.google.com/custom-search/v1/overview)")
    else:
        search_api_key = st.text_input("Brave API Key", type="password")
        st.caption("[获取 Brave API 密钥](https://api.search.brave.com/)")
    
    st.divider()
    st.header("⚙️ AI 分析设置")
    provider = st.selectbox("AI 服务商", ["DeepSeek", "Gemini", "OpenAI"])
    default_key = "sk-342ff1ad8a9b4223bb01937fc1cf7338" if provider == "DeepSeek" else ""
    ai_api_key = st.text_input("AI API Key", value=default_key, type="password")
    custom_model = st.text_input("模型", value="deepseek-chat" if provider == "DeepSeek" else "gemini-2.0-flash")
    base_url = "https://api.deepseek.com" if provider == "DeepSeek" else None
    
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
    if not ai_api_key:
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
                
                if context and "Error" not in context and len(context) > 200:
                    analysis = brain.analyze(context, INDUSTRY_PRESETS[industry]["persona"], INDUSTRY_PRESETS[industry]["focus"])
                    if "error" not in analysis:
                        analysis['url'] = url
                        if int(analysis.get('deal_score', 0)) > 2:
                            leads_data.append(analysis)
                            st.success(f"💎 成功识别: **{analysis.get('company_name')}** (潜力评分: {analysis.get('deal_score')}/10)")
                
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

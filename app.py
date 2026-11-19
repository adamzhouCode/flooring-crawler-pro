import streamlit as st
import pandas as pd
import json
import time
import os
import sys
import re
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

# --- Debug tracing (写入 debug_trace.log + stderr) ---
_TRACE_LOCK = threading.Lock()
_TRACE_FILE = "debug_trace.log"

def trace(stage: str, url: str = "", extra: str = ""):
    tid = threading.get_ident() % 10000
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    short_url = url[:70] if url else ""
    line = f"[{ts}] T{tid:<4} {stage:<22} {short_url} {extra}".rstrip()
    with _TRACE_LOCK:
        print(line, file=sys.stderr, flush=True)
        try:
            with open(_TRACE_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

# Scraper & Search
import requests
import urllib3
import logging
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
        entered = st.session_state["password"]
        admin_pw = get_secret("ADMIN_PASSWORD", "superadmin")
        user_pw = get_secret("APP_PASSWORD", "admin123")
        if entered == admin_pw:
            st.session_state["password_correct"] = True
            st.session_state["is_admin"] = True
        elif entered == user_pw:
            st.session_state["password_correct"] = True
            st.session_state["is_admin"] = False
        else:
            st.session_state["password_correct"] = False
            st.session_state["is_admin"] = False
        del st.session_state["password"]

    if "password_correct" not in st.session_state:
        st.text_input("登录密码", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("登录密码", type="password", on_change=password_entered, key="password")
        st.error("😕 密码错误")
        return False
    else:
        return True

import glob

# Load Industry Profiles
PROFILES = {}
profile_paths = glob.glob(os.path.join(os.path.dirname(__file__), "profiles", "*.json"))
for p_file in profile_paths:
    try:
        with open(p_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            PROFILES[data["industry_id"]] = data
    except Exception as e:
        logging.error(f"Failed to load profile {p_file}: {e}")

if not PROFILES:
    st.error("❌ 找不到行业配置文件 (profiles/*.json)，请确保系统中至少有一个活跃的行业 Profile。")
    st.stop()

# URL 过滤：匹配确切后缀与黑名单（解决子串匹配错杀问题）
SKIP_DOMAIN_SUFFIXES = [
    '.gov.cn', '.gov', '.edu.cn', '.edu', '.org.cn', '.mil'
]

BLACKLISTED_DOMAINS = {
    # 平台/社交/资讯类
    'zhihu.com', 'douban.com', 'bilibili.com', 'toutiao.com',
    'jianshu.com', 'csdn.net', 'weibo.com', 'qq.com',
    'sohu.com', 'sina.com', '163.com', 'ifeng.com', 'xinhuanet.com',
    'baidu.com', 'map.baidu.com', 'tieba.baidu.com',
    'cnr.cn', 'cctv.com', 'people.com.cn',
    'youtube.com', 'instagram.com', 'pinterest.com', 'linkedin.com',
    'facebook.com', 'twitter.com', 'x.com',
    'douyin.com', 'iesdouyin.com', 'cnblogs.com', 'xiaohongshu.com',
    # 招聘/黄页/百科/企业查询数据库
    'jobui.com', 'cnpp.cn', 'zhaopin.com', '51job.com', 'zhipin.com', 'liepin.com',
    'tianyancha.com', 'qcc.com', 'qichacha.com', '11467.com', 'kanzhun.com', 'xin.baidu.com',
    'yellowpages.com', 'yelp.com', 'dianping.com', '58.com', 'ganji.com',
    'aliexpress.com', 'taobao.com', 'jd.com', 'tmall.com', '1688.com',
    'b2b168.com', 'hc360.com', 'made-in-china.com', 'alibaba.com',
    'makepolo.com', 'ebdoor.com', '慧聪网', 'b2b', 'huangye88.com',
    # 英文黄页/点评/聚合类
    'yelp.com', 'yellowpages.com', 'glassdoor.com', 'bbb.org', 'houzz.com',
    'angi.com', 'homeadvisor.com', 'thumbtack.com', 'porch.com',
    'zoominfo.com', 'dnb.com', 'bloomberg.com', 'manta.com', 'mapquest.com',
    'superpages.com', 'dexknows.com', 'chamberofcommerce.com', 'realtor.com', 'zillow.com',
    # 具体杂项站点
    'bjnews.com.cn', 'chinafloor.cn', 'chinatimber.org', 'zhilengwang.cn',
    'shzh.net', 'zol.com.cn', '360che.com', 'pchouse.com.cn',
    'chery.cn', 'epson.com.cn', 'ciwf.com.cn',
    # 搜索引擎自身的跳转包装链接（Serper 在冷门市场会大量返回 google.com/goto?url=...，抓不开）
    'google.com', 'googleusercontent.com', 'bing.com', 'duckduckgo.com',
    'yandex.ru', 'yandex.kz', 'yandex.com', 'yandex.com.tr',
    # 俄语区（哈萨克/中亚/俄罗斯）目录站、聚合黄页、招聘站、C2C 集市
    '2gis.kz', '2gis.ru', '2gis.com', 'pulscen.kz', 'pulscen.ru',
    'flagma.kz', 'flagma.ru', 'satu.kz', 'prom.ua', 'tiu.ru',
    'olx.kz', 'olx.ua', 'hh.kz', 'hh.ru', 'gorodrabot.kz', 'rabota.kz',
    'optlist.ru', 'supl.biz', 'all.biz', 'cataloxy-kz.ru', 'cataloxy.ru',
    'avito.ru', 'drom.ru', 'wildberries.ru', 'ozon.ru',
    # 文档/资料聚合站
    'scribd.com', 'slideshare.net', 'studylib.net',
    # 实测哈萨克斯坦跑下来仍会混进的：短视频、分类信息、招标、企业名录
    'tiktok.com', 'kaspi.kz', 'bb.market', 'bizorg.su', 'orgs.biz',
    'uwork.kz', 'ets-tender.kz', 'tenders.kz', 'goszakup.gov.kz',
}

def normalize_domain(url: str) -> str:
    """取出域名并去掉 www. 前缀。
    注意：原先写的是 .lstrip('www.')，lstrip 按字符集剥离，会把 wood-floor.com 削成 ood-floor.com，
    导致黑名单匹配和域名去重都不准。这里按前缀正确处理。"""
    domain = urlparse(url).netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain


# ISO 639-2/3 三位语言码 -> Google/Serper 认的两位码（业务员从网上抄来的常是三位码，例如哈萨克语 kaz）
THREE_LETTER_LANG = {
    "kaz": "kk", "rus": "ru", "eng": "en", "spa": "es", "deu": "de", "ger": "de",
    "fra": "fr", "fre": "fr", "por": "pt", "ita": "it", "tur": "tr", "ara": "ar",
    "jpn": "ja", "kor": "ko", "vie": "vi", "tha": "th", "ind": "id", "pol": "pl",
    "ukr": "uk", "uzb": "uz", "nld": "nl", "dut": "nl", "aze": "az", "kat": "ka",
    "kir": "ky", "tgk": "tg", "hye": "hy", "zho": "zh", "chi": "zh",
}


def resolve_hl(lang_mode: str) -> str:
    """把 profile 里或业务员手填的语言码，转成搜索 API 真正认识的 hl 值。
    以前这里写死成「不是 zh 就一律 en」，导致非中英市场（例如哈萨克斯坦）永远按英文检索。"""
    lm = (lang_mode or "").strip().lower().replace("_", "-")
    if lm in ("zh", "zh-cn", "cn", "chs", "zh-hans"):
        return "zh-CN"
    if lm in ("zh-tw", "zh-hk", "zh-hant"):
        return "zh-TW"
    lm = THREE_LETTER_LANG.get(lm, lm)
    if len(lm) == 2 and lm.isascii() and lm.isalpha():
        return lm
    return "en"


# 一个公司可能同时出现在多个页面（laminat.liderpol.kz / parket.liderpol.kz），
# 只按公司名去重抓不住（AI 会给出 "AK-STROY" 和 "TOO AK-STROY" 两种写法），
# 所以再加一层「可注册域名」去重。注意 com.cn / co.uk 这类二级后缀不能简单取后两段。
MULTI_PART_TLDS = {
    'com.cn', 'net.cn', 'org.cn', 'gov.cn', 'edu.cn', 'ac.cn',
    'com.au', 'net.au', 'org.au', 'co.uk', 'org.uk', 'me.uk', 'ac.uk',
    'co.nz', 'net.nz', 'org.nz', 'co.za', 'org.za', 'net.za',
    'com.tr', 'com.br', 'com.mx', 'com.ar', 'com.sg', 'com.my',
    'com.hk', 'com.tw', 'co.jp', 'co.kr', 'co.in', 'com.ua',
    'com.ru', 'org.ru', 'net.ru', 'com.kz', 'org.kz', 'net.kz',
}


def registrable_domain(url: str) -> str:
    """取可注册域名：laminat.liderpol.kz -> liderpol.kz；shop.abc.com.cn -> abc.com.cn"""
    parts = normalize_domain(url).split('.')
    if len(parts) >= 3 and '.'.join(parts[-2:]) in MULTI_PART_TLDS:
        return '.'.join(parts[-3:])
    return '.'.join(parts[-2:]) if len(parts) >= 2 else (parts[0] if parts else '')


# AI 有时返回【批发/分销商】、有时返回 批发/分销商，两种写法会把结果页的
# 「包含业务类型」筛选器拆成双份选项，这里统一成不带括号的标准写法。
BUSINESS_TYPES = [
    "批发/分销商", "零售门店/C端展厅", "房地产/开发", "工程施工/包工",
    "装修/设计公司", "地材制造商/生产工厂", "其它领域/跑偏",
]


def normalize_business_type(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    stripped = re.sub(r"[【】\[\]()（）\s]", "", raw)
    for canon in BUSINESS_TYPES:
        if stripped == re.sub(r"\s", "", canon):
            return canon
    for canon in BUSINESS_TYPES:
        head = canon.split("/")[0]
        if stripped and (stripped in canon or head in stripped):
            return canon
    return raw


# 页面上电话和邮箱常常粘在一起，AI 会照抄出 "04info@ipol.kz" 这种带前缀数字的地址，
# 发信会直接退回。这里做一次清洗 + 校验。
_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]{1,64}@[A-Z0-9.\-]+\.[A-Z]{2,24}", re.IGNORECASE)
_ROLE_LOCAL_PARTS = {
    "info", "sales", "office", "mail", "email", "contact", "order", "orders",
    "support", "admin", "manager", "director", "shop", "team", "hello", "help",
    "zakaz", "opt", "optom", "sklad", "market", "marketing", "company", "client",
}


def clean_email_field(value: str) -> str:
    """从 AI 返回的 email 字段里取出第一个合法地址，并剥掉粘在前面的数字。"""
    raw = (value or "").strip()
    if not raw or raw.lower() in {"无", "n/a", "na", "none", "null", "未提供", "-"}:
        return ""
    raw = raw.replace("mailto:", " ")
    found = _EMAIL_RE.findall(raw)
    if not found:
        return ""
    addr = found[0].strip(".,;:<>[](){}'\"").lower()
    local, _, domain = addr.partition("@")
    m = re.match(r"^\d{1,8}([a-z][a-z0-9._%+\-]*)$", local)
    # 只有当数字确实不属于品牌名时才剥离，避免把 24opt@24opt.kz 改坏
    if m and m.group(1) in _ROLE_LOCAL_PARTS and local not in domain:
        addr = f"{m.group(1)}@{domain}"
    return addr


# AI 报错原文是一串英文堆栈，业务员看不懂。这里翻译成「发生了什么 + 该怎么办」。
AI_ERROR_RULES = [
    (("insufficient balance", "insufficient_balance", "余额不足", "payment required", "billing", " 402"),
     "AI 账户余额不足",
     "去 AI 服务商后台充值，充完回来重新点一次「🚀 开始自动化拓客任务」"),
    (("invalid api key", "incorrect api key", "api key not valid", "invalid_api_key",
      "unauthorized", "authentication", " 401"),
     "AI 密钥无效或已过期",
     "检查左侧「AI API Key」是否填对、有没有多余的空格"),
    (("no longer available", "model_not_found", "does not exist", "not found", " 404"),
     "AI 模型不存在或已被下线",
     "换一个模型名（左侧「模型」那一栏），或换一个 AI 服务商"),
    (("rate limit", "ratelimit", "too many requests", "quota", "resource_exhausted", " 429"),
     "AI 调用被限流 / 配额用尽",
     "等几分钟再试；或把左侧「搜索穷举深度」调小一些"),
    (("timeout", "timed out", "connection", "network", "unreachable", "proxy", "ssl"),
     "连不上 AI 服务",
     "检查网络连通性（Gemini / OpenAI 需要国际网络）"),
]

# 这三类再重试也没用，遇到就该立刻停，别让剩下的网站继续空转
AI_FATAL_LABELS = {"AI 账户余额不足", "AI 密钥无效或已过期", "AI 模型不存在或已被下线"}


def classify_ai_error(message: str):
    """把 AI 的原始报错翻译成 (人话, 怎么办, 是否属于该立刻中止的致命错误)。"""
    low = f" {(message or '').lower()} "
    for needles, label, action in AI_ERROR_RULES:
        if any(needle in low for needle in needles):
            return label, action, label in AI_FATAL_LABELS
    return "AI 分析失败", "展开下方「📊 分析漏斗」查看具体报错", False


def is_url_blacklisted(url: str) -> bool:
    """Check if URL is from a non-business site (news, gov, portal, etc.)"""
    domain = normalize_domain(url)
    
    if any(domain.endswith(suf) for suf in SKIP_DOMAIN_SUFFIXES):
        return True
        
    # Check exact domain or its subdomains (e.g., zhidao.baidu.com -> baidu.com)
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

def _brave_lang(lang_mode: str) -> str:
    """Brave 用的语言码，中文要写成 zh-hans。"""
    hl = resolve_hl(lang_mode)
    if hl == "zh-CN":
        return "zh-hans"
    if hl == "zh-TW":
        return "zh-hant"
    return hl.lower()


class SearchEngine:
    @staticmethod
    def search_google(query: str, api_key: str, cx: str, max_results: int = 10, lang_mode: str = "zh", country_code: str = "cn") -> List[str]:
        """使用 Google Custom Search API，自动分页，支持多语言和指定国家"""
        url = "https://www.googleapis.com/customsearch/v1"
        hl = resolve_hl(lang_mode)
        all_links = []
        for start in range(1, max_results + 1, 10):
            num = min(10, max_results - start + 1)
            params = {
                "key": api_key,
                "cx": cx,
                "q": query,
                "num": num,
                "start": start,
                "gl": country_code,
                "hl": hl,
            }
            # 英文市场保持原行为（只用 gl）；中文/俄语等非英文市场额外做语言+国别限定，
            # 否则 Google 会拿英文结果来凑数（哈萨克斯坦那次 156 条里 106 条是跳转包装链接）
            if hl != "en" and len(country_code) == 2:
                params.update({
                    "lr": f"lang_{hl}",
                    "cr": f"country{country_code.upper()}",
                })
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
    def search_google_multi(queries: List[str], api_key: str, cx: str, max_results: int = 10, lang_mode: str = "zh", country_code: str = "cn") -> List[str]:
        """Run multiple simple queries and merge/dedup results"""
        all_links = []
        seen = set()
        per_query = max(5, max_results // len(queries)) if queries else max_results
        for q in queries:
            results = SearchEngine.search_google(q, api_key, cx, per_query, lang_mode, country_code)
            for link in results:
                if link not in seen:
                    seen.add(link)
                    all_links.append(link)
        return all_links[:max_results]

    @staticmethod
    def search_serper_multi(queries: List[str], api_key: str, max_results: int = 10, lang_mode: str = "zh", country_code: str = "cn") -> List[str]:
        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        hl = resolve_hl(lang_mode).lower()
        all_links = []
        seen = set()
        per_query = max(10, max_results // len(queries)) if queries else max_results
        
        for query in queries:
            page = 1
            added_this_query = 0
            while added_this_query < per_query and len(all_links) < max_results:
                payload = {
                    "q": query,
                    "gl": country_code,
                    "hl": hl,
                    "num": 10,
                    "page": page,
                }
                try:
                    response = requests.post(url, json=payload, headers=headers, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    items = data.get("organic", [])
                    if not items:
                        break
                    for item in items:
                        link = item.get("link")
                        if link and link not in seen:
                            seen.add(link)
                            all_links.append(link)
                            added_this_query += 1
                    page += 1
                except Exception as e:
                    st.error(f"Serper 搜索失败 (query={query}, page={page}): {e}")
                    break
                    
        return all_links[:max_results]

    @staticmethod
    def search_brave(query: str, api_key: str, max_results: int = 10, lang_mode: str = "zh", country_code: str = "cn") -> List[str]:
        """使用 Brave Search API (备选方案)"""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {"Accept": "application/json", "X-Subscription-Token": api_key}
        params = {
            "q": query,
            "count": max_results,
            "country": country_code,
            "search_lang": _brave_lang(lang_mode)
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            data = response.json()
            return [v['url'] for v in data.get("web", {}).get("results", [])]
        except Exception as e:
            st.error(f"Brave 搜索失败: {e}")
            return []

class Scraper:
    BINARY_SUFFIXES = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                       '.zip', '.rar', '.7z', '.tar', '.gz', '.exe', '.dmg',
                       '.mp4', '.mp3', '.avi', '.mov', '.jpg', '.jpeg', '.png', '.gif')
    MAX_PARSE_BYTES = 3_000_000  # 3MB 上限，超过直接当巨页处理

    @staticmethod
    def get_deep_context(url: str, depth: int = 2) -> str:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        # 拦截二进制文件下载链接（PDF/压缩包等不是网页）
        if url.lower().split('?')[0].endswith(Scraper.BINARY_SUFFIXES):
            return "[CRAWL_ERROR] 非网页链接(二进制文件)"
        try:
            # 禁用警告
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            try:
                # 优先尝试 curl_cffi，解决大部分 Cloudflare / TLS 指纹拦截
                trace("curl_cffi.get>>", url)
                resp = curl_requests.get(url, headers=headers, timeout=10, impersonate="chrome110", allow_redirects=True)
                trace("curl_cffi.get<<", url, f"status={resp.status_code} bytes={len(resp.content)}")
            except Exception as e_curl:
                trace("curl_cffi.fail", url, str(e_curl)[:80])
                try:
                    # Fallback 到标准 requests（无指纹，且不验证 SSL），适合老站及部分被 curl_cffi 阻断的站
                    trace("requests.get>>", url)
                    resp = requests.get(url, headers=headers, timeout=10, verify=False, allow_redirects=True)
                    trace("requests.get<<", url, f"status={resp.status_code} bytes={len(resp.content)}")
                except Exception as e_req:
                    if "timeout" in str(e_req).lower():
                        return f"[CRAWL_ERROR] 网站连接超时(死链或仅限国内IP访问)"
                    if "403" in str(e_req) or "forbidden" in str(e_req).lower():
                        return f"[CRAWL_ERROR] 网站拒绝访问(403防火墙拦截)"
                    return f"[CRAWL_ERROR] 无法访问该网站 ({str(e_req)[:50]})"

            if resp.status_code != 200:
                if resp.status_code in [403, 405, 401]:
                    return f"[CRAWL_ERROR] 网站拒绝访问 (HTTP {resp.status_code})"
                if resp.status_code in [404, 500, 502, 504]:
                    return f"[CRAWL_ERROR] 网页打不开或已失效 (HTTP {resp.status_code})"
                return f"[CRAWL_ERROR] HTTP {resp.status_code}"

            # Content-Type 兜底：很多服务器不把 URL 结尾当 PDF，但返回 application/pdf
            ctype = resp.headers.get('Content-Type', '').lower()
            if any(bad in ctype for bad in ('application/pdf', 'application/octet-stream',
                                             'application/zip', 'application/msword', 'image/', 'video/', 'audio/')):
                return f"[CRAWL_ERROR] 非 HTML 内容 (Content-Type: {ctype[:50]})"

            # 体积兜底：超过 3MB 直接截断，防止 bs4/trafilatura 卡死
            raw_bytes = resp.content
            if len(raw_bytes) > Scraper.MAX_PARSE_BYTES:
                trace("bs4.oversize", url, f"bytes={len(raw_bytes)} -> truncated to {Scraper.MAX_PARSE_BYTES}")
                raw_bytes = raw_bytes[:Scraper.MAX_PARSE_BYTES]

            trace("bs4.parse>>", url, f"bytes={len(raw_bytes)}")
            soup = BeautifulSoup(raw_bytes, 'html.parser')
            trace("bs4.parse<<", url)
            text_bundle = f"=== 来源网址: {resp.url} ===\n"

            # Extract contact info early and place at top
            visible_text = soup.get_text()
            emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', visible_text))
            phones = set(re.findall(r'400-\d{3}-\d{4}|400\d{7}|\+?86-?1[3-9]\d{9}|1[3-9]\d{9}|\d{3,4}-\d{7,8}|\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', visible_text))
            if emails or phones:
                text_bundle += f"=== 原始联系信息 ===\n邮箱: {', '.join(emails)}\n电话: {', '.join(phones)}\n\n"

            # Semantic Structure Identification
            meta_bundle = []
            if soup.title and soup.title.string:
                meta_bundle.append(f"网页标题: {soup.title.string.strip()}")
            
            desc_tag = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'name': 'Description'})
            if desc_tag and desc_tag.get('content'):
                meta_bundle.append(f"业务简介(Meta): {desc_tag['content'].strip()}")
                
            keyword_tag = soup.find('meta', attrs={'name': 'keywords'}) or soup.find('meta', attrs={'name': 'Keywords'})
            if keyword_tag and keyword_tag.get('content'):
                meta_bundle.append(f"核心关键词(Meta): {keyword_tag['content'].strip()}")
                
            img_alts = [img.get('alt').strip() for img in soup.find_all('img', alt=True) if len(img.get('alt', '').strip()) > 3]
            if img_alts:
                unique_alts = list(dict.fromkeys(img_alts))[:20]  # Cap at 20 unique image labels
                meta_bundle.append(f"产品图片元素(AltTags): {', '.join(unique_alts)}")
                
            if meta_bundle:
                text_bundle += "=== 核心业务标识 (Semantic Metadata) ===\n" + "\n".join(meta_bundle) + "\n\n"

            # Aggressive structural pruning before extracting ANYTHING
            for noisy_tag in soup.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style', 'noscript', 'canvas', 'video', 'button']):
                noisy_tag.decompose()
            for noisy_block in soup.find_all(attrs={'class': re.compile(r'menu|nav|footer|sidebar|banner|slider|carousel', re.I)}):
                noisy_block.decompose()

            # Primary Text Extraction on cleaned DOM
            trace("trafilatura>>", url)
            main_text = trafilatura.extract(str(soup))
            trace("trafilatura<<", url, f"len={len(main_text) if main_text else 0}")
            if main_text and len(main_text) > 200:
                text_bundle += "=== 页面主体正文 ===\n" + main_text
            else:
                text_bundle += "=== 页面离散文本 (去噪提纯后) ===\n"
                content_parts = [tag.get_text(strip=True) for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'span', 'li']) if len(tag.get_text(strip=True)) > 15]
                unique_parts = list(dict.fromkeys(content_parts))
                text_bundle += "\n".join(unique_parts[:100])
            
            if depth > 1:
                sub_links = []
                keywords = ['about', 'contact', 'project', 'products', 'cases', 'services', 'profile', '关于', '联系', '项目', '产品', '案例', '工程', '合作', '服务', '简介', '业务', '分公司', '门店']
                for a in soup.find_all('a', href=True):
                    t = a.get_text().strip().lower()
                    if any(kw in t for kw in keywords):
                        sub_links.append(urljoin(resp.url, a['href']))
                
                for sub_url in list(set(sub_links))[:3]:
                    try:
                        # 子页面同样过滤二进制文件
                        if sub_url.lower().split('?')[0].endswith(Scraper.BINARY_SUFFIXES):
                            continue
                        trace("sub.get>>", sub_url)
                        # 用流式下载长空 sub页，单个子页 限制 512KB
                        sub_resp = curl_requests.get(sub_url, headers=headers, timeout=8, impersonate="chrome110")
                        sub_ctype = sub_resp.headers.get('Content-Type', '').lower()
                        if any(bad in sub_ctype for bad in ('pdf', 'octet-stream', 'zip', 'msword', 'image/', 'video/', 'audio/')):
                            continue
                        sub_content = sub_resp.content[:512_000]  # 子页面最多处理 512KB
                        trace("sub.get<<", sub_url, f"bytes={len(sub_content)}")
                        sub_soup = BeautifulSoup(sub_content, 'html.parser')
                        
                        # Prune DOM before extraction
                        for noisy_tag in sub_soup.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style', 'noscript', 'canvas', 'video', 'button']):
                            noisy_tag.decompose()
                        for noisy_block in sub_soup.find_all(attrs={'class': re.compile(r'menu|nav|footer|sidebar|banner|slider|carousel', re.I)}):
                            noisy_block.decompose()

                        trace("sub.trafi>>", sub_url)
                        sub_text = trafilatura.extract(str(sub_soup))
                        trace("sub.trafi<<", sub_url, f"len={len(sub_text) if sub_text else 0}")
                        if not sub_text or len(sub_text) < 100:
                            content_parts = [tag.get_text(strip=True) for tag in sub_soup.find_all(['p', 'div', 'li', 'h1', 'h2']) if len(tag.get_text(strip=True)) > 15]
                            sub_text = "\n".join(list(dict.fromkeys(content_parts))[:30])
                            
                        if sub_text and len(sub_text.strip()) > 20:
                            text_bundle += f"\n\n--- 子页面 ({sub_url}) ---\n{sub_text[:1500]}"
                    except: continue
                    
            return text_bundle
        except Exception as e: return f"[CRAWL_ERROR] 抓取错误: {e}"

class AIBrain:
    def __init__(self, provider: str, api_key: str, model_name: str, base_url: Optional[str] = None, debug_log: bool = False):
        self.provider, self.api_key, self.model_name, self.base_url = provider, api_key, model_name, base_url
        self.debug_log = debug_log

    def analyze(self, text: str, persona: str, focus: str, scoring_rules: Dict) -> Dict:
        system_prompt = (
            f"你是一位{persona}，专门负责为我们寻找高价值商业线索。\n"
            f"分析重点：{focus}\n\n"
            "## 评分标准\n"
            "deal_score（商业合作潜力）：\n"
            f"  {scoring_rules.get('deal_score', '0-10 评分法则')}\n\n"
            "relevance_score（行业相关度）：\n"
            f"  {scoring_rules.get('relevance_score', '0-10 评分法则')}\n\n"
            "## 示例\n"
            f"{scoring_rules.get('example', '')}\n\n"
            "## 要求\n"
            "返回严格 JSON 格式：{\"company_name\": \"\", \"business_type\": \"\", \"email\": \"\", \"phone\": \"\", "
            "\"relevance_score\": 0-10, \"deal_score\": 0-10, \"summary\": \"\", \"why\": \"\"}\n\n"
            "## 关于 business_type (业务类型分类)\n"
            "从以下预设类型中选择最符合的一个（必填）：\n"
            "【批发/分销商】, 【零售门店/C端展厅】, 【房地产/开发】, 【工程施工/包工】, 【装修/设计公司】, 【地材制造商/生产工厂】, 【其它领域/跑偏】\n"
            "如果网站是一个明确的门店或者只做终端客户选购，请务必标注为【零售门店/C端展厅】。\n\n"
            "## 关于 company_name\n"
            "- 优先从页面内容中提取完整公司名（如 XX有限公司）\n"
            "- 如果页面没有完整公司名，从网站品牌名推断\n"
            "- 如果是非企业实体页面，填空字符串\n"
            "- 只有确实无法判断所属企业时才留空"
        )
        # Extract URL from text bundle for AI context
        url_line = text[:200].split('\n')[0] if text else ""
        user_prompt = f"请分析以下网站内容并返回 JSON。即使网站是英文，你给出的 summary 和 why 必须用中文，方便我阅读。\n来源: {url_line}\n\n{text[:8000]}"
        
        if self.debug_log:
            try:
                with open("debug_payloads.txt", "a", encoding="utf-8") as f:
                    f.write(f"\n[{'='*30} PAYLOAD LOG {'='*30}]\n")
                    f.write(f"MODEL: {self.model_name}\n")
                    f.write(f"SYSTEM PROMPT:\n{system_prompt}\n\n")
                    f.write(f"USER PROMPT:\n{user_prompt}\n")
                    f.write(f"[{'='*73}]\n\n")
            except Exception as e:
                print(f"Error writing debug log: {e}")

        try:
            if self.provider == "Gemini":
                trace("ai.gemini>>", url_line)
                client = genai.Client(api_key=self.api_key)
                combined_prompt = system_prompt + "\n\n" + user_prompt
                response = client.models.generate_content(model=self.model_name, contents=combined_prompt, config={'response_mime_type': 'application/json'})
                trace("ai.gemini<<", url_line)
                return json.loads(response.text)
            elif self.provider in ["DeepSeek", "OpenAI", "Custom"]:
                trace("ai.openai>>", url_line, f"provider={self.provider}")
                client = OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=45.0, max_retries=1)
                response = client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"}
                )
                trace("ai.openai<<", url_line)
                return json.loads(response.choices[0].message.content)
        except Exception as e:
            trace("ai.FAIL", url_line, str(e)[:80])
            return {"error": str(e)}

# --- UI ---

st.set_page_config(page_title="地板爬虫", layout="wide", page_icon="🎯")

if not check_password():
    st.stop()

st.title("🎯 地板爬虫：专业智能拓客引擎")

with st.sidebar:
    st.header("🏢 工作区 (Workspace)")
    profile_names = list(PROFILES.keys())
    # Ensure "flooring" is the default if it exists
    default_index = 0
    if "flooring" in profile_names:
        default_index = profile_names.index("flooring")
        
    display_names = [PROFILES[p]["industry_name"] for p in profile_names]
    selected_idx = st.selectbox("选择行业模板 (Profile)", range(len(display_names)), index=default_index, format_func=lambda i: display_names[i])
    
    selected_id = profile_names[selected_idx]
    active_profile = PROFILES[selected_id]
    active_keywords = [k.lower() for k in active_profile.get("keywords", [])]
    
    st.divider()
    st.header("⚙️ 搜索配置")
    engine_choice = st.selectbox("搜索引擎", ["Serper (首选)", "Google CSE", "Brave API"])

    if engine_choice == "Serper (首选)":
        serper_api_key = st.text_input("Serper API Key", value=get_secret("SERPER_API_KEY"), type="password")
    elif engine_choice == "Google CSE":
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
        # gemini-2.0-flash 已被 Google 下线（调用直接 404），默认值改成当前可用型号，
        # 并允许用 .env / secrets 里的 GEMINI_MODEL 覆盖
        default_model = get_secret("GEMINI_MODEL", "gemini-3.8-flash")
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
    _is_admin = st.session_state.get("is_admin", False)
    _max_slider = 500 if _is_admin else 300
    max_results = st.slider("搜索穷举深度 (URL数量)", 10, _max_slider, 150)
    crawl_depth = st.slider("抓取层级", 1, 3, 2)
    show_raw = st.checkbox("显示抓取原文 (原 debug)")
    
    st.divider()
    st.caption("🔧 高级调试选项")
    enable_debug_log = st.checkbox("导出 AI 分析生肉文本到文件 (debug_payloads.txt)")

col1, col2 = st.columns([1, 1])
with col1:
    MARKET_OPTIONS = active_profile["markets"]
    market_choice = st.selectbox("搜索区域/语言 (Market & Region)", list(MARKET_OPTIONS.keys()))
    
    market_config = MARKET_OPTIONS[market_choice]
    lang_mode = market_config["lang"]
    country_code = market_config["country"]
    default_city = market_config["default_city"]
    preset_group = market_config["preset_group"]
    presets_dict = active_profile["preset_groups"].get(preset_group, {})
    
    if market_choice == "🌐 自定义国家/语言 (Custom)":
        col2_1, col2_2 = st.columns(2)
        with col2_1:
            country_code = st.text_input("国家代码 (Country Code)", value="de", help="例如: de (德国), mx (墨西哥), sg (新加坡)")
        with col2_2:
            lang_mode = st.text_input("语言代码 (Language)", value="en", help="例如: en (英语), es (西班牙语), de (德语)")
    
    industry = st.selectbox("目标行业", list(presets_dict.keys()))

    # 市场在 profile 里配了城市表就走下拉：业务员选中文标签，系统用当地语言的城市名去检索
    city_options = market_config.get("cities") or []
    if city_options:
        labels = [c["label"] for c in city_options]
        picked = st.selectbox("目标城市", labels)
        city = next(c["value"] for c in city_options if c["label"] == picked)
        if city != picked:
            st.caption(f"检索时实际使用当地写法：**{city}**（不用手动输入）")
    else:
        city = st.text_input("目标城市", value=default_city, placeholder="如：上海、广州、Dallas、Cape Town")
with col2:
    preset_queries = presets_dict[industry].get("queries") or [""]
    default_query = preset_queries[0]
    search_template = st.text_input("主检索词 (可自定义修改)", value=default_query)

    def _fill(tpl: str):
        """套用城市名；业务员手改时打漏了花括号也不至于让整个页面崩掉。"""
        try:
            return tpl.format(city=city)
        except (KeyError, IndexError, ValueError):
            return tpl

    query_list = []
    if city and search_template:
        query_list = [_fill(search_template)]
        # 同一行业的其余预设词一起跑，结果自动合并去重。
        # 单条复杂词在小市场召回率很低，多条简单词覆盖面明显更好。
        for tpl in preset_queries[1:]:
            q = _fill(tpl)
            if q and q not in query_list:
                query_list.append(q)
        if len(query_list) == 1:
            st.code(f"检索词: {query_list[0]}", language=None)
        else:
            preview = "\n".join(f"{i + 1}. {q}" for i, q in enumerate(query_list))
            st.code(f"并跑 {len(query_list)} 条检索词（结果自动合并去重）:\n{preview}", language=None)


if st.button("🚀 开始自动化拓客任务", use_container_width=True):
    is_admin = st.session_state.get("is_admin", False)
    limiter = get_limiter()
    if not is_admin and not limiter.check():
        st.error(f"❌ 已达到当日全局搜索上限 ({limiter.daily_limit})。请明天再试或联系管理员。")
    elif not ai_api_key:
        st.error("请输入 AI API 密钥。")
    elif engine_choice == "Serper (首选)" and not serper_api_key:
        st.error("请输入 Serper API Key。")
    elif engine_choice == "Google CSE" and (not google_api_key or not google_cx):
        st.error("请完整填写 Google API Key 和 CX ID。")
    elif engine_choice == "Brave API" and not search_api_key:
        st.error("请输入 Brave API Key。")
    elif not city: st.error("请输入城市。")
    else:
        with st.status(f"正在通过 {engine_choice} ({market_choice}) 穷举深搜 {len(query_list)} 个变体矩阵...") as status:
            fetch_count = max_results
            if engine_choice == "Serper (首选)":
                raw_urls = SearchEngine.search_serper_multi(query_list, serper_api_key, fetch_count, lang_mode, country_code)
            elif engine_choice == "Google CSE":
                raw_urls = SearchEngine.search_google_multi(query_list, google_api_key, google_cx, fetch_count, lang_mode, country_code)
            else:
                raw_urls = []
                seen = set()
                per_q = max(5, fetch_count // len(query_list)) if query_list else fetch_count
                for q in query_list:
                    for u in SearchEngine.search_brave(q, search_api_key, per_q, lang_mode, country_code):
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
                domain = normalize_domain(u)
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
            
            # 清理旧的 debug log
            if enable_debug_log and os.path.exists("debug_payloads.txt"):
                try: os.remove("debug_payloads.txt")
                except: pass
                
            trace("=== SESSION START ===", "", f"urls={len(urls)} workers={min(10, len(urls))} provider={provider}")
            brain = AIBrain(provider, ai_api_key, custom_model, base_url, debug_log=enable_debug_log)
            persona = presets_dict[industry]["persona"]
            focus = presets_dict[industry]["focus"].format(city=city)
            progress_bar = st.progress(0, text=f"并行分析中... 0/{len(urls)}")

            # AI 挂掉（没余额 / 密钥错 / 模型下线）时不要继续空转：
            # 连续几次致命错误后就停掉后面的 AI 调用，把原因留给界面显示。
            ai_outage = {"label": None, "action": None, "hits": 0}
            ai_outage_lock = threading.Lock()

            def note_ai_error(label, action, fatal):
                if not fatal:
                    return
                with ai_outage_lock:
                    ai_outage["hits"] += 1
                    if ai_outage["hits"] >= 3 and not ai_outage["label"]:
                        ai_outage["label"], ai_outage["action"] = label, action

            def ai_is_down():
                with ai_outage_lock:
                    return ai_outage["label"], ai_outage["action"]

            def process_url(url):
                """Scrape and analyze a single URL (runs in thread). URLs already pre-filtered."""
                trace("PROC.start", url)
                t0 = time.time()
                try:
                    context = Scraper.get_deep_context(url, depth=crawl_depth)
                    result = {"url": url, "context": context, "analysis": None, "skip_reason": None}
                    if not context or context.startswith("[CRAWL_ERROR]"):
                        result["skip_reason"] = "抓取失败"
                    elif len(context) <= 80:
                        result["skip_reason"] = "内容过短"
                    elif active_keywords and not any(kw in context.lower() for kw in active_keywords):
                        result["skip_reason"] = "内容无行业关键词"
                    else:
                        down_label, down_action = ai_is_down()
                        if down_label:
                            # AI 已经确认挂了，后面的站直接跳过，不再浪费时间和额度
                            result["skip_reason"] = f"AI已中止: {down_label}"
                            result["ai_error"] = (down_label, down_action)
                        else:
                            scoring_rules = active_profile.get("scoring_rules", {})
                            analysis = brain.analyze(context, persona, focus, scoring_rules)
                            # Guard: only treat as valid if it's a proper dict
                            if not isinstance(analysis, dict):
                                result["skip_reason"] = "AI返回格式异常"
                            elif "error" in analysis:
                                raw_err = str(analysis.get('error', ''))
                                label, action, fatal = classify_ai_error(raw_err)
                                note_ai_error(label, action, fatal)
                                result["ai_error"] = (label, action)
                                result["skip_reason"] = f"AI分析失败: {label} ({raw_err[:90]})"
                            else:
                                result["analysis"] = analysis
                    trace("PROC.end", url, f"elapsed={time.time()-t0:.1f}s skip={result.get('skip_reason')}")
                    return result
                except Exception as e:
                    trace("PROC.CRASH", url, str(e)[:80])
                    return {"url": url, "context": None, "analysis": None, "skip_reason": f"线程异常: {str(e)[:80]}"}

            # Run scraping + AI analysis in parallel (10 workers, balanced speed/stability)
            max_workers = min(10, len(urls))
            results = []
            completed = 0
            executor = ThreadPoolExecutor(max_workers=max_workers)
            futures = {executor.submit(process_url, url): url for url in urls}
            pending = set(futures.keys())
            STALL_TIMEOUT = 60  # 秒：超过这么久没新完成，就报 pending
            watchdog_fired = False
            while pending:
                try:
                    done_iter = as_completed(pending, timeout=STALL_TIMEOUT)
                    future = next(done_iter)
                except StopIteration:
                    break
                except FuturesTimeoutError:
                    stuck = [futures[f] for f in pending if not f.done()]
                    trace("WATCHDOG.STALL", "", f"pending={len(stuck)} urls={stuck}")
                    for f in pending:
                        if not f.done():
                            url_key = futures.get(f, "unknown")
                            results.append({"url": url_key, "context": None, "analysis": None, "skip_reason": "watchdog 强制终止 (60s 无进展)"})
                            f.cancel()
                            completed += 1
                    progress_bar.progress(1.0, text=f"⚠️ watchdog 终止 {len(stuck)} 个卡死任务 (查看 debug_trace.log)")
                    watchdog_fired = True
                    break

                pending.discard(future)
                try:
                    results.append(future.result(timeout=1))
                except Exception as e:
                    url_key = futures.get(future, "unknown")
                    results.append({"url": url_key, "context": None, "analysis": None, "skip_reason": f"线程超时/崩溃: {str(e)[:60]}"})
                completed += 1
                progress_bar.progress(completed / len(urls), text=f"并行分析中... {completed}/{len(urls)}")

            # 不等待卡死的线程（shutdown wait=False 让主流程继续，僵尸线程会在进程退出时自然终结）
            executor.shutdown(wait=False, cancel_futures=True)
            if watchdog_fired:
                st.warning("⚠️ 检测到卡死任务，已强制跳过。请查看项目根目录的 `debug_trace.log` 定位卡住的 URL 和阶段。")

            progress_bar.progress(1.0, text="分析完成!")

            # Filter and store results in session_state
            leads_data = []
            raw_contexts = []
            funnel = {"total": len(raw_urls), "blacklisted": filtered_count, "crawl_fail": 0, "too_short": 0, "no_keyword": 0, "ai_fail": 0, "no_name": 0, "low_score": 0, "duplicate": 0, "accepted": 0}
            skipped_details = []
            ai_errors = {}   # {人话原因: (次数, 怎么办)}
            seen_companies = set()

            # 同一家公司命中多个页面时，让分数高的那条先进去重，否则保留下来的是随机一条
            def _rank(r):
                a = r.get("analysis")
                if not isinstance(a, dict):
                    return (1, 0)
                try:
                    return (0, -int(float(a.get("deal_score", 0))))
                except (ValueError, TypeError):
                    return (0, 0)

            for r in sorted(results, key=_rank):
                raw_contexts.append({"url": r["url"], "context": r["context"]})

                if r["skip_reason"]:
                    if r.get("ai_error"):
                        _lbl, _act = r["ai_error"]
                        _cnt = ai_errors.get(_lbl, (0, _act))[0]
                        ai_errors[_lbl] = (_cnt + 1, _act)
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
                # Guard: some AI models wrap the result in a list, unwrap it
                if isinstance(analysis, list):
                    if analysis and isinstance(analysis[0], dict):
                        analysis = analysis[0]
                    else:
                        skipped_details.append({"url": r["url"], "reason": "AI 返回格式异常 (list)"})
                        funnel["ai_fail"] += 1
                        continue
                if not isinstance(analysis, dict):
                    skipped_details.append({"url": r["url"], "reason": "AI 返回格式异常 (非dict)"})
                    funnel["ai_fail"] += 1
                    continue
                try:
                    deal_score = int(float(analysis.get('deal_score', 0)))
                    relevance_score = int(float(analysis.get('relevance_score', 0)))
                except (ValueError, TypeError):
                    deal_score, relevance_score = 0, 0

                # 统一 AI 返回的格式差异，否则结果页的类型筛选器会被拆成双份选项
                analysis['business_type'] = normalize_business_type(analysis.get('business_type', ''))
                analysis['email'] = clean_email_field(analysis.get('email', ''))

                company_name = analysis.get('company_name', '').strip()
                if not company_name:
                    # Fallback: use domain as company name
                    company_name = normalize_domain(r['url'])
                    analysis['company_name'] = company_name
                    funnel["no_name"] += 1

                # 去重：公司名 + 可注册域名双口径。
                # 只看名字会漏掉 AK-STROY / TOO AK-STROY 这类同一家公司的不同写法。
                name_key = company_name.lower().replace(' ', '')
                domain_key = registrable_domain(r['url'])
                if name_key in seen_companies:
                    funnel["duplicate"] += 1
                    skipped_details.append({"url": r["url"], "reason": f"重复公司: {company_name}"})
                    continue
                if domain_key and domain_key in seen_companies:
                    funnel["duplicate"] += 1
                    skipped_details.append({"url": r["url"], "reason": f"重复公司(同域名 {domain_key}): {company_name}"})
                    continue
                seen_companies.add(name_key)
                if domain_key:
                    seen_companies.add(domain_key)

                analysis['url'] = r['url']
                # 无状态一次性保留，抛弃原有的硬拦截
                leads_data.append(analysis)
                funnel["accepted"] += 1

            st.session_state["leads_data"] = leads_data
            st.session_state["raw_contexts"] = raw_contexts
            st.session_state["search_urls"] = urls
            st.session_state["result_city"] = city
            st.session_state["funnel"] = funnel
            st.session_state["skipped_details"] = skipped_details
            st.session_state["ai_errors"] = ai_errors
            st.rerun()

# --- Display persisted results ---
if "leads_data" in st.session_state:
    urls = st.session_state["search_urls"]
    leads_data = st.session_state["leads_data"]
    raw_contexts = st.session_state["raw_contexts"]
    result_city = st.session_state["result_city"]

    # AI 出问题时最优先告诉业务员。否则「未发现有效线索」会被误读成「这个市场没客户」。
    _ai_errors = st.session_state.get("ai_errors") or {}
    if _ai_errors:
        _total_fail = sum(c for c, _ in _ai_errors.values())
        _lines = [
            f"- **{lbl}**（{cnt} 个网站）→ {act}"
            for lbl, (cnt, act) in sorted(_ai_errors.items(), key=lambda kv: -kv[1][0])
        ]
        st.error(
            f"🛑 **AI 没能完成分析，{_total_fail} 个网站没有被评分。**\n\n"
            + "\n".join(_lines)
            + "\n\n⚠️ **这不代表这个城市没有客户。** 按上面的办法处理完，重新点一次"
              "「🚀 开始自动化拓客任务」即可。"
        )

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
        st.divider()
        st.header("📋 交互式线索调音台")
        
        # 将原始数据转化为 DataFrame
        df = pd.DataFrame(leads_data)
        if 'business_type' not in df.columns:
            df['business_type'] = '未知'
            
        df = df.rename(columns={
            'company_name': '名称', 
            'business_type': '类型',
            'deal_score': '潜力', 
            'relevance_score': '相关度',
            'summary': '业务', 
            'email': '邮箱', 
            'phone': '电话', 
            'url': '网址', 
            'why': 'AI 判定理由'
        })
        
        # Convert score columns to numeric if needed, filling missing with 0
        df['潜力'] = pd.to_numeric(df['潜力'], errors='coerce').fillna(0).astype(int)
        df['相关度'] = pd.to_numeric(df['相关度'], errors='coerce').fillna(0).astype(int)

        # 实时过滤器 UI
        st.markdown("##### 🎛️ 实时过滤条件")
        col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
        with col_f1:
            all_types = df["类型"].dropna().unique().tolist()
            selected_types = st.multiselect("包含业务类型", all_types, default=all_types)
        with col_f2:
            min_rel = st.slider("最低相关度", 0, 10, 5, help="建议 ≥5 ，排除非行业相关的外围企业")
        with col_f3:
            min_deal = st.slider("最低潜力分", 0, 10, 4, help="建议 ≥4，排除没留电话或合作可能性低的企业")
            
        # 根据动态过滤器计算结果
        filtered_df = df[
            (df["类型"].isin(selected_types)) &
            (df["相关度"] >= min_rel) &
            (df["潜力"] >= min_deal)
        ]
        
        st.success(f"🔍 当前条件过滤后剩余 candidate: **{len(filtered_df)}** 家 (总候选池 {len(df)} 家)")

        # 呈现最终表格 (支持在前端排序)
        st.dataframe(
            filtered_df.sort_values(by=['潜力', '相关度'], ascending=[False, False]), 
            use_container_width=True,
            column_config={
                "网址": st.column_config.LinkColumn("前往网站")
            }
        )
        
        import io
        buffer = io.BytesIO()
        output_df = filtered_df.sort_values(by=['潜力', '相关度'], ascending=[False, False])
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer: 
            output_df.to_excel(writer, index=False)
        st.download_button("📥 导出当前过滤结果 (Excel)", buffer.getvalue(), f"交互线索_{result_city}.xlsx")
    else:
        _f = st.session_state.get("funnel", {})
        _attempted = sum(_f.get(k, 0) for k in
                         ("crawl_fail", "too_short", "no_keyword", "ai_fail", "duplicate", "accepted"))
        if _ai_errors:
            st.error("🛑 这次没有线索，是 **AI 没能完成分析**，不是这个城市没有客户。原因和处理办法见页面顶部的红框。")
        elif _attempted and _f.get("crawl_fail", 0) >= _attempted * 0.6:
            st.error("未发现有效线索：**大部分网站抓取失败**，多半是网络不稳或对方站点拦截。过一会儿重试一次。")
        elif _attempted and _f.get("no_keyword", 0) >= _attempted * 0.5:
            st.warning("未发现有效线索：抓到的网站里**没有本行业关键词**，说明检索词和这个市场对不上。换一个目标行业，或换个城市再试。")
        else:
            st.error("未发现有效线索。展开上方「📊 分析漏斗」能看到是在哪一层被过滤掉的。")

    if st.button("🗑️ 清除结果", use_container_width=True):
        for key in ["leads_data", "raw_contexts", "search_urls", "result_city", "funnel", "skipped_details", "ai_errors"]:
            st.session_state.pop(key, None)
        st.rerun()

import os
import json
import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor


class BaseCrawler(ABC):
    """漫画爬虫基类，定义统一接口"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        """初始化爬虫基类"""
        self.PROXIES = proxies or {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}
        self.HEADERS = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive"
        }

        # 创建漫画保存目录和缓存目录
        self.MANGA_DIR = "manga"
        self.CACHE_DIR = "cache"
        os.makedirs(self.MANGA_DIR, exist_ok=True)
        os.makedirs(self.CACHE_DIR, exist_ok=True)

        # 设置并发限制
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrency)

    @abstractmethod
    async def search_manga(self, keyword, page=1):
        """搜索漫画并缓存结果

        Args:
            keyword: 搜索关键词
            page: 页数

        Returns:
            str: 格式化的搜索结果
        """
        pass

    @abstractmethod
    async def get_manga_chapters(self, index_or_url):
        """获取漫画章节列表并缓存

        Args:
            index_or_url: 索引或URL/path_word

        Returns:
            str: 格式化的章节列表
        """
        pass

    @abstractmethod
    async def download_manga(self, chapter_spec, index_or_url):
        """下载漫画章节，合并为PDF并删除图片

        Args:
            chapter_spec: 章节规格 (x 或 x-y 或 all)
            index_or_url: 索引或URL/path_word

        Returns:
            str: 下载结果
        """
        pass

    def save_to_cache(self, cache_type, identifier, data):
        """保存数据到缓存"""
        filename = f"{cache_type}_{identifier}.json"
        filepath = os.path.join(self.CACHE_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def load_from_cache(self, cache_type, identifier):
        """从缓存加载数据"""
        filename = f"{cache_type}_{identifier}.json"
        filepath = os.path.join(self.CACHE_DIR, filename)

        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)

        return None

    def get_latest_cache(self, cache_type):
        """获取最新的缓存文件"""
        cache_files = [f for f in os.listdir(self.CACHE_DIR)
                       if f.endswith('.json') and f.startswith(f"{cache_type}_")]

        if not cache_files:
            return None, None

        latest_cache = sorted(cache_files,
                              key=lambda x: os.path.getmtime(os.path.join(self.CACHE_DIR, x)))[-1]

        filepath = os.path.join(self.CACHE_DIR, latest_cache)

        with open(filepath, "r", encoding="utf-8") as f:
            return filepath, json.load(f)

import os
import json
import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

class BaseCrawler(ABC):
    """漫画爬虫基类，定义统一接口"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        """初始化爬虫基类，支持多站点缓存隔离
        
        Args:
            proxies: 代理设置，默认为None
            headers: 请求头设置，默认为None
            max_concurrency: 最大并发数，默认为10
        
        Returns:
            None
        """
        crawler_id = self.__class__.__name__.lower().replace("crawler", "")
        self.MANGA_DIR = os.path.join("./manga", crawler_id)
        self.CACHE_DIR = os.path.join("./cache", crawler_id)
        os.makedirs(self.MANGA_DIR, exist_ok=True)
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        self.PROXIES = proxies or {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"}
        self.HEADERS = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive"
        }
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

    def format_chapter_list(self, manga_name, chapters):
        """统一格式化章节列表的输出
        
        Args:
            manga_name: 漫画名称
            chapters: 章节数据列表
        
        Returns:
            str: 格式化后的章节列表字符串
        """
        if not chapters:
            return f"{manga_name}: 无可用章节"
        chapter_list = "\n".join(
            [f"{idx + 1}. {chap['name']} ({chap['url']})"
             for idx, chap in enumerate(chapters)]
        )
        return f"**{manga_name}** 章节列表:\n{chapter_list}"

    def clear_cache(self, cache_type):
        """删除指定类型的所有缓存文件
        
        Args:
            cache_type: 缓存类型
        
        Returns:
            None
        """
        for fname in os.listdir(self.CACHE_DIR):
            if fname.startswith(f"{cache_type}_") and fname.endswith(".json"):
                try:
                    os.remove(os.path.join(self.CACHE_DIR, fname))
                except Exception as e:
                    print(f"删除缓存文件 {fname} 失败: {e}")

    def load_from_cache(self, cache_type):
        """直接加载指定类型的唯一缓存文件
        
        Args:
            cache_type: 缓存类型
        
        Returns:
            dict: 缓存的数据，如果不存在则返回None
        """
        cache_file = os.path.join(self.CACHE_DIR, f"{cache_type}_latest.json")
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def save_to_cache(self, cache_type, data):
        """保存数据到指定类型的唯一缓存文件
        
        Args:
            cache_type: 缓存类型
            data: 要保存的数据
        
        Returns:
            None
        """
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        cache_file = os.path.join(self.CACHE_DIR, f"{cache_type}_latest.json")
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

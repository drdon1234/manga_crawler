import os
import re
import json
import asyncio
from curl_cffi.requests import AsyncSession
from PIL import Image
import img2pdf
from base_crawler import BaseCrawler


class CopyCrawler(BaseCrawler):
    """Copy漫画爬虫完整实现"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        """初始化爬虫实例"""
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.copy-manga.com/",
            "Connection": "keep-alive"
        }
        super().__init__(proxies, headers, max_concurrency)

    async def search_manga(self, keyword, page=1):
        """执行漫画搜索"""
        limit = 12
        cache_key = f"{keyword}_page{page}_limit{limit}"
        cached = self.load_from_cache("search", cache_key)

        if cached:
            print(f"加载缓存: {keyword} 第{page}页")
            return self._format_search(cached)

        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                url = f"https://www.copy-manga.com/api/kb/web/searchbd/comics?offset={(page - 1) * limit}&platform=2&limit={limit}&q={keyword}"
                response = await session.get(url)

                if response.status_code == 200:
                    data = json.loads(response.text)
                    self.save_to_cache("search", cache_key, data)
                    return self._format_search(data)
                return f"搜索失败 状态码: {response.status_code}"
        except Exception as e:
            return f"搜索异常: {e}"

    def _format_search(self, data):
        """格式化搜索结果"""
        if not data.get("results", {}).get("list"):
            return "无结果"

        output = [f"\n找到 {data['results']['total']} 个结果:"]
        for idx, item in enumerate(data["results"]["list"]):
            output.append(f"{idx + 1}. {item['name']}")
            output.append(f"   路径: {item['path_word']}")
            if item.get("author"):
                authors = "，".join([a["name"] for a in item["author"]])
                output.append(f"   作者: {authors}")
            if item.get("alias"):
                output.append(f"   别名: {item['alias']}")
            output.append("")
        return "\n".join(output)

    async def get_manga_chapters(self, identifier):
        """获取章节列表"""
        manga_info = await self._get_manga_metadata(identifier)
        if "error" in manga_info:
            return manga_info["error"]

        cached = self.load_from_cache("chapters", manga_info["path_word"])
        if cached:
            print(f"加载缓存章节: {manga_info['name']}")
            return self._format_chapters(cached["results"]["list"], manga_info["name"])

        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                url = f"https://www.copy-manga.com/api/v3/comic/{manga_info['path_word']}/group/default/chapters?limit=500"
                response = await session.get(url)

                if response.status_code == 200:
                    data = json.loads(response.text)
                    self.save_to_cache("chapters", manga_info["path_word"], data)
                    return self._format_chapters(data["results"]["list"], manga_info["name"])
                return f"获取失败 状态码: {response.status_code}"
        except Exception as e:
            return f"获取异常: {e}"

    async def _get_manga_metadata(self, identifier):
        """获取漫画元数据"""
        if identifier.isdigit():
            cache = self.get_latest_cache("search")
            if not cache:
                return {"error": "请先执行搜索"}

            idx = int(identifier) - 1
            items = cache[1]["results"]["list"]
            if not 0 <= idx < len(items):
                return {"error": "无效索引"}

            return {
                "path_word": items[idx]["path_word"],
                "name": items[idx]["name"]
            }
        else:
            return {
                "path_word": identifier,
                "name": "未知漫画"
            }

    def _format_chapters(self, chapters, name):
        """格式化章节列表"""
        if not chapters:
            return f"{name} 无章节"

        output = [f"\n{name} 章节列表({len(chapters)}):"]
        for idx, ch in enumerate(chapters):
            output.append(f"{idx + 1}. {ch['name']}")
        return "\n".join(output)

    async def download_manga(self, chapter_spec, identifier):
        """下载漫画主入口"""
        manga_info = await self._get_manga_metadata(identifier)
        if "error" in manga_info:
            return manga_info["error"]

        chapters = await self._fetch_chapters(manga_info["path_word"])
        if isinstance(chapters, str):
            return chapters

        selected = self._parse_chapter_spec(chapter_spec, chapters)
        if "error" in selected:
            return selected["error"]

        results = []
        for ch in selected["chapters"]:
            result = await self._download_chapter(
                manga_info["name"],
                ch["name"],
                manga_info["path_word"],
                ch["uuid"]
            )
            results.append(f"{ch['name']}: {result}")

        return f"\n{manga_info['name']} 下载结果:\n" + "\n".join(results)

    async def _fetch_chapters(self, path_word):
        """获取章节数据"""
        cached = self.load_from_cache("chapters", path_word)
        if cached:
            return cached["results"]["list"]

        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                url = f"https://www.copy-manga.com/api/v3/comic/{path_word}/group/default/chapters?limit=500"
                response = await session.get(url)

                if response.status_code == 200:
                    data = json.loads(response.text)
                    self.save_to_cache("chapters", path_word, data)
                    return data["results"]["list"]
                return f"获取失败 状态码: {response.status_code}"
        except Exception as e:
            return f"获取异常: {e}"

    def _parse_chapter_spec(self, spec, chapters):
        """解析章节规格"""
        if spec.lower() == "all":
            return {"chapters": chapters}

        if "-" in spec:
            try:
                start, end = map(int, spec.split("-"))
                if not (1 <= start <= end <= len(chapters)):
                    return {"error": f"无效范围 1-{len(chapters)}"}
                return {"chapters": chapters[start - 1:end]}
            except ValueError:
                return {"error": "格式错误 应为x-y"}

        try:
            idx = int(spec) - 1
            if not 0 <= idx < len(chapters):
                return {"error": f"无效索引 1-{len(chapters)}"}
            return {"chapters": [chapters[idx]]}
        except ValueError:
            return {"error": "格式错误 应为数字"}

    async def _download_chapter(self, manga_name, chapter_name, path_word, uuid):
        """下载单个章节"""
        dir_path = self._create_chapter_dir(manga_name, chapter_name)
        pdf_path = os.path.join(dir_path, f"{chapter_name}.pdf")

        image_urls = await self._get_image_urls(path_word, uuid)
        if isinstance(image_urls, str):
            return image_urls

        success = await self._download_images(image_urls, dir_path, path_word, uuid)
        if success == 0:
            return "无成功下载"

        self._generate_pdf(dir_path, pdf_path)
        return f"成功 {success}/{len(image_urls)}"

    def _create_chapter_dir(self, manga_name, chapter_name):
        """创建存储目录"""
        safe_manga = re.sub(r'[^\w\s.-]', '', manga_name).strip()
        safe_chapter = re.sub(r'[^\w\s.-]', '', chapter_name).strip()
        dir_path = os.path.join(self.MANGA_DIR, safe_manga, safe_chapter)
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    async def _get_image_urls(self, path_word, uuid):
        """获取图片URL列表"""
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                url = f"https://www.copy-manga.com/api/v3/comic/{path_word}/chapter/{uuid}?platform=1"
                response = await session.get(url)

                if response.status_code == 200:
                    data = json.loads(response.text)
                    return [c["url"] for c in data.get("results", {}).get("chapter", {}).get("contents", [])]
                return f"获取图片失败 状态码: {response.status_code}"
        except Exception as e:
            return f"获取异常: {e}"

    async def _download_images(self, urls, dir_path, path_word, uuid):
        """并发下载图片"""
        tasks = []
        for idx, url in enumerate(urls):
            filepath = os.path.join(dir_path, f"{idx + 1:04d}.jpg")
            if os.path.exists(filepath):
                continue
            tasks.append(self._download_image(url, filepath, path_word, uuid))

        results = await asyncio.gather(*tasks)
        return sum(results)

    async def _download_image(self, url, filepath, path_word, uuid, max_retries=3):
        """下载单张图片"""
        referer = f"https://www.copy-manga.com/comic/{path_word}/chapter/{uuid}"
        headers = self.HEADERS.copy()
        headers["Referer"] = referer

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await AsyncSession().get(url, headers=headers)

                    if response.status_code == 200:
                        self._save_image(response.content, filepath)
                        return True
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"下载异常: {e}")
                await asyncio.sleep(1)
        return False

    def _save_image(self, content, path):
        """保存图片文件"""
        temp_path = path + ".tmp"
        with open(temp_path, "wb") as f:
            f.write(content)

        try:
            img = Image.open(temp_path).convert("RGB")
            img.save(path, "JPEG", quality=85)
        finally:
            os.remove(temp_path)

    def _generate_pdf(self, dir_path, pdf_path):
        """生成PDF文件"""
        images = sorted([
            os.path.join(dir_path, f)
            for f in os.listdir(dir_path)
            if f.endswith(".jpg")
        ])

        if not images:
            return

        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert(images))

        for img in images:
            try:
                os.remove(img)
            except Exception as e:
                print(f"删除失败: {e}")

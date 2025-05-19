import os
import re
import json
import asyncio
from curl_cffi.requests import AsyncSession
from PIL import Image
import img2pdf
from .base_crawler import BaseCrawler


class CopyCrawler(BaseCrawler):
    def __init__(self, proxies=None, headers=None, max_concurrency=10, domains=None):
        self.domains = domains or [
            "www.copy20.com",
            "www.mangacopy.com"
        ]
        self.current_domain_index = 0
        self.domain_fail_count = 0
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"https://{self.get_current_domain()}/",
            "Connection": "keep-alive"
        }
        super().__init__(proxies, headers, max_concurrency)

    def get_current_domain(self):
        return self.domains[self.current_domain_index]

    def switch_to_next_domain(self):
        self.current_domain_index = (self.current_domain_index + 1) % len(self.domains)
        self.domain_fail_count = 0
        self.HEADERS["Referer"] = f"https://{self.get_current_domain()}/"
        return self.get_current_domain()

    async def search_manga(self, keyword, page=1):
        self.clear_cache("search")
        limit = 12
        total_attempts = 0
        max_attempts = 2 * len(self.domains)
        while total_attempts < max_attempts:
            try:
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    domain = self.get_current_domain()
                    url = f"https://{domain}/api/kb/web/searchbd/comics?offset={(page - 1) * limit}&platform=2&limit={limit}&q={keyword}"
                    response = await session.get(url, timeout=3)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        self.save_to_cache("search", data)
                        self.domain_fail_count = 0
                        return self._format_search(data)
                    self.domain_fail_count += 1
                    total_attempts += 1
                    if self.domain_fail_count >= 2:
                        self.switch_to_next_domain()
                    await asyncio.sleep(1)
            except Exception as e:
                self.domain_fail_count += 1
                total_attempts += 1
                if self.domain_fail_count >= 2:
                    self.switch_to_next_domain()
                await asyncio.sleep(1)
        return "搜索失败: 所有域名尝试均失败"

    def _format_search(self, data):
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
        self.clear_cache("chapters")
        manga_info = await self._get_manga_metadata(identifier)
        if "error" in manga_info:
            return manga_info["error"]
        total_attempts = 0
        max_attempts = 2 * len(self.domains)
        while total_attempts < max_attempts:
            try:
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    domain = self.get_current_domain()
                    url = f"https://{domain}/api/v3/comic/{manga_info['path_word']}/group/default/chapters?limit=500"
                    response = await session.get(url, timeout=3)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        self.save_to_cache("chapters", data)
                        self.domain_fail_count = 0
                        return self._format_chapters(data["results"]["list"], manga_info["name"])
                    self.domain_fail_count += 1
                    total_attempts += 1
                    if self.domain_fail_count >= 2:
                        self.switch_to_next_domain()
                    await asyncio.sleep(1)
            except Exception as e:
                self.domain_fail_count += 1
                total_attempts += 1
                if self.domain_fail_count >= 2:
                    self.switch_to_next_domain()
                await asyncio.sleep(1)
        return "获取章节失败: 所有域名尝试均失败"

    async def _get_manga_metadata(self, identifier):
        if identifier.isdigit():
            cache = self.load_from_cache("search")
            if not cache:
                return {"error": "请先执行搜索"}
            idx = int(identifier) - 1
            items = cache["results"]["list"]
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
        if not chapters:
            return f"{name} 无章节"
        output = [f"\n{name} 章节列表({len(chapters)}):"]
        for idx, ch in enumerate(chapters):
            output.append(f"{idx + 1}. {ch['name']}")
        return "\n".join(output)

    async def download_manga(self, chapter_spec, identifier):
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
        cached = self.load_from_cache("chapters")
        if cached:
            return cached["results"]["list"]
        total_attempts = 0
        max_attempts = 2 * len(self.domains)
        while total_attempts < max_attempts:
            try:
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    domain = self.get_current_domain()
                    url = f"https://{domain}/api/v3/comic/{path_word}/group/default/chapters?limit=500"
                    response = await session.get(url, timeout=3)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        self.save_to_cache("chapters", data)
                        self.domain_fail_count = 0
                        return data["results"]["list"]
                    self.domain_fail_count += 1
                    total_attempts += 1
                    if self.domain_fail_count >= 2:
                        self.switch_to_next_domain()
                    await asyncio.sleep(1)
            except Exception as e:
                self.domain_fail_count += 1
                total_attempts += 1
                if self.domain_fail_count >= 2:
                    self.switch_to_next_domain()
                await asyncio.sleep(1)
        return "获取章节失败: 所有域名尝试均失败"

    def _parse_chapter_spec(self, spec, chapters):
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
        safe_manga = re.sub(r'[^\w\s.-]', '', manga_name).strip()
        safe_chapter = re.sub(r'[^\w\s.-]', '', chapter_name).strip()
        dir_path = os.path.join(self.MANGA_DIR, safe_manga, safe_chapter)
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    async def _get_image_urls(self, path_word, uuid):
        total_attempts = 0
        max_attempts = 2 * len(self.domains)
        while total_attempts < max_attempts:
            try:
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    domain = self.get_current_domain()
                    url = f"https://{domain}/api/v3/comic/{path_word}/chapter/{uuid}?platform=1"
                    response = await session.get(url, timeout=3)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        self.domain_fail_count = 0
                        return [c["url"] for c in data.get("results", {}).get("chapter", {}).get("contents", [])]
                    self.domain_fail_count += 1
                    total_attempts += 1
                    if self.domain_fail_count >= 2:
                        self.switch_to_next_domain()
                    await asyncio.sleep(1)
            except Exception as e:
                self.domain_fail_count += 1
                total_attempts += 1
                if self.domain_fail_count >= 2:
                    self.switch_to_next_domain()
                await asyncio.sleep(1)
        return "获取图片URL失败: 所有域名尝试均失败"

    async def _download_images(self, urls, dir_path, path_word, uuid):
        tasks = []
        for idx, url in enumerate(urls):
            filepath = os.path.join(dir_path, f"{idx + 1:04d}.jpg")
            if os.path.exists(filepath):
                continue
            tasks.append(self._download_image(url, filepath, path_word, uuid))
        results = await asyncio.gather(*tasks)
        return sum(results)

    async def _download_image(self, url, filepath, path_word, uuid, max_retries=3):
        domain = self.get_current_domain()
        referer = f"https://{domain}/comic/{path_word}/chapter/{uuid}"
        headers = self.HEADERS.copy()
        headers["Referer"] = referer
        attempts = 0
        domain_fails = 0
        while attempts < max_retries * len(self.domains):
            try:
                async with self.semaphore:
                    response = await AsyncSession().get(url, headers=headers)
                    if response.status_code == 200:
                        self._save_image(response.content, filepath)
                        return True
                    domain_fails += 1
                    attempts += 1
                    if domain_fails >= 2:
                        domain = self.switch_to_next_domain()
                        referer = f"https://{domain}/comic/{path_word}/chapter/{uuid}"
                        headers["Referer"] = referer
                        domain_fails = 0
                    await asyncio.sleep(1)
            except Exception as e:
                domain_fails += 1
                attempts += 1
                if domain_fails >= 2:
                    domain = self.switch_to_next_domain()
                    referer = f"https://{domain}/comic/{path_word}/chapter/{uuid}"
                    headers["Referer"] = referer
                    domain_fails = 0
                await asyncio.sleep(1)
        return False

    def _save_image(self, content, path):
        temp_path = path + ".tmp"
        with open(temp_path, "wb") as f:
            f.write(content)
        try:
            img = Image.open(temp_path).convert("RGB")
            img.save(path, "JPEG", quality=85)
        finally:
            os.remove(temp_path)

    def _generate_pdf(self, dir_path, pdf_path):
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
                pass

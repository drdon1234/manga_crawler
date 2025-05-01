import os
import re
import json
import asyncio
from curl_cffi.requests import AsyncSession
from PIL import Image
import img2pdf
from base_crawler import BaseCrawler


class CopyCrawler(BaseCrawler):
    """Copy漫画爬虫实现"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        """初始化Copy漫画爬虫"""
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.copy-manga.com/",
            "Connection": "keep-alive"
        }
        super().__init__(proxies, headers, max_concurrency)

    async def search_manga(self, keyword, page=1):
        """搜索漫画并缓存结果"""
        # 每页显示数量
        limit = 12

        # 检查缓存
        cache_key = f"{keyword}_page{page}_limit{limit}"
        cached_results = self.load_from_cache("search", cache_key)

        if cached_results:
            print(f"从缓存加载搜索结果: {keyword}, 第{page}页")
            return self.format_search_results(cached_results)

        # 执行搜索
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                # 构建API URL
                url = f"https://www.copy-manga.com/api/kb/web/searchbd/comics?offset={(page - 1) * limit}&platform=2&limit={limit}&q={keyword}&q_type="

                response = await session.get(url)

                if response.status_code == 200:
                    search_results = json.loads(response.text)

                    # 保存到缓存
                    self.save_to_cache("search", cache_key, search_results)

                    return self.format_search_results(search_results)
                else:
                    return f"搜索失败，状态码: {response.status_code}"
        except Exception as e:
            return f"搜索失败: {e}"

    def format_search_results(self, search_results):
        """格式化搜索结果为字符串"""
        if not search_results or "results" not in search_results or len(search_results["results"]["list"]) == 0:
            return "未找到相关漫画"

        manga_list = search_results["results"]["list"]
        total = search_results["results"]["total"]

        result_str = f"\n找到 {total} 个相关漫画:\n"

        for i, manga in enumerate(manga_list):
            result_str += f"{i + 1}. {manga['name']}\n"
            result_str += f"   路径: {manga['path_word']}\n"

            if "author" in manga and manga["author"]:
                authors = ", ".join([author["name"] for author in manga["author"]])
                result_str += f"   作者: {authors}\n"

            if "alias" in manga and manga["alias"]:
                result_str += f"   别名: {manga['alias']}\n"

            if "popular" in manga:
                result_str += f"   人气: {manga['popular']}\n"

            result_str += "\n"

        return result_str

    async def get_manga_chapters(self, index_or_path):
        """获取漫画章节列表并缓存"""
        # 确定是索引还是path_word
        manga_path_word = ""
        manga_name = ""

        if str(index_or_path).isdigit():
            # 是索引，从最新的搜索缓存获取漫画信息
            filepath, search_results = self.get_latest_cache("search")

            if not search_results or "results" not in search_results:
                return "无搜索缓存，请先搜索漫画"

            idx = int(index_or_path) - 1
            manga_list = search_results["results"]["list"]

            if idx < 0 or idx >= len(manga_list):
                return f"无效的索引: {index_or_path}"

            manga = manga_list[idx]
            manga_path_word = manga["path_word"]
            manga_name = manga["name"]
        else:
            # 是path_word
            manga_path_word = index_or_path
            manga_name = "未知漫画"  # 尝试获取

        # 检查缓存
        cached_chapters = self.load_from_cache("chapters", manga_path_word)

        if cached_chapters:
            print(f"从缓存加载章节列表: {manga_name}")
            return self.format_chapters_list(manga_name, cached_chapters["results"]["list"])

        # 获取章节列表
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                # 构建API URL
                url = f"https://www.copy-manga.com/api/v3/comic/{manga_path_word}/group/default/chapters?limit=500&offset=0"

                response = await session.get(url)

                if response.status_code == 200:
                    chapters_data = json.loads(response.text)

                    # 获取漫画名称（如果之前未知）
                    if manga_name == "未知漫画":
                        # 获取漫画详情
                        detail_url = f"https://www.copy-manga.com/api/v3/comic/{manga_path_word}"
                        detail_response = await session.get(detail_url)

                        if detail_response.status_code == 200:
                            detail_data = json.loads(detail_response.text)
                            if "results" in detail_data and "comic" in detail_data["results"]:
                                manga_name = detail_data["results"]["comic"]["name"]

                    # 保存到缓存
                    self.save_to_cache("chapters", manga_path_word, chapters_data)

                    return self.format_chapters_list(manga_name, chapters_data["results"]["list"])
                else:
                    return f"获取章节列表失败，状态码: {response.status_code}"
        except Exception as e:
            return f"获取章节列表失败: {e}"

    def format_chapters_list(self, manga_name, chapters):
        """格式化章节列表为字符串"""
        if not chapters:
            return f"{manga_name}: 未找到章节"

        result_str = f"\n{manga_name} 共 {len(chapters)} 章:\n"

        for i, chapter in enumerate(chapters):
            result_str += f"{i + 1}. {chapter['name']}\n"

        return result_str

    async def download_manga(self, chapter_spec, index_or_path):
        """下载漫画章节，合并为PDF并删除图片"""
        try:
            # 获取漫画信息
            manga_path_word = ""
            manga_name = ""

            if str(index_or_path).isdigit():
                # 是索引，从最新的搜索缓存获取漫画信息
                filepath, search_results = self.get_latest_cache("search")

                if not search_results or "results" not in search_results:
                    return "无搜索缓存，请先搜索漫画"

                idx = int(index_or_path) - 1
                manga_list = search_results["results"]["list"]

                if idx < 0 or idx >= len(manga_list):
                    return f"无效的索引: {index_or_path}"

                manga = manga_list[idx]
                manga_path_word = manga["path_word"]
                manga_name = manga["name"]
            else:
                # 是path_word
                manga_path_word = index_or_path
                # 尝试从缓存获取名称
                cached_chapters = self.load_from_cache("chapters", manga_path_word)

                if cached_chapters and "results" in cached_chapters and "comic" in cached_chapters["results"]:
                    manga_name = cached_chapters["results"]["comic"]["name"]
                else:
                    manga_name = "未知漫画"

            # 获取章节列表
            cached_chapters = self.load_from_cache("chapters", manga_path_word)

            if cached_chapters and "results" in cached_chapters and "list" in cached_chapters["results"]:
                chapters = cached_chapters["results"]["list"]

                # 获取漫画名称（如果之前未知）
                if manga_name == "未知漫画" and "comic" in cached_chapters["results"]:
                    manga_name = cached_chapters["results"]["comic"]["name"]
            else:
                # 获取章节列表
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    # 构建API URL
                    url = f"https://www.copy-manga.com/api/v3/comic/{manga_path_word}/group/default/chapters?limit=500&offset=0"

                    response = await session.get(url)

                    if response.status_code == 200:
                        chapters_data = json.loads(response.text)

                        # 获取漫画名称（如果之前未知）
                        if manga_name == "未知漫画" and "results" in chapters_data and "comic" in chapters_data[
                            "results"]:
                            manga_name = chapters_data["results"]["comic"]["name"]

                        # 保存到缓存
                        self.save_to_cache("chapters", manga_path_word, chapters_data)

                        chapters = chapters_data["results"]["list"]
                    else:
                        return f"获取章节列表失败，状态码: {response.status_code}"

            if not chapters:
                return f"{manga_name}: 未找到章节"

            # 根据章节规格选择要下载的章节
            selected_chapters = []

            if chapter_spec.lower() == 'all':
                selected_chapters = chapters
            elif '-' in chapter_spec:
                # 范围下载
                try:
                    start, end = map(int, chapter_spec.split('-'))

                    if start < 1 or end > len(chapters) or start > end:
                        return f"无效的章节范围: {chapter_spec}，应在 1-{len(chapters)} 范围内"

                    selected_chapters = chapters[start - 1:end]
                except ValueError:
                    return f"无效的章节范围格式: {chapter_spec}"
            else:
                # 单章节下载
                try:
                    idx = int(chapter_spec) - 1

                    if idx < 0 or idx >= len(chapters):
                        return f"无效的章节索引: {chapter_spec}，应在 1-{len(chapters)} 范围内"

                    selected_chapters = [chapters[idx]]
                except ValueError:
                    return f"无效的章节索引格式: {chapter_spec}"

            # 下载选中的章节
            results = []
            for chapter in selected_chapters:
                print(f"\n开始下载章节: {chapter['name']}")

                # 下载章节
                result = await self.download_manga_chapter(
                    manga_name,
                    chapter['name'],
                    manga_path_word,
                    chapter['uuid']
                )
                results.append(f"{chapter['name']}: {result}")

            return f"\n{manga_name} 下载完成:\n" + "\n".join(results)

        except Exception as e:
            return f"下载过程中出错: {e}"

    async def download_manga_chapter(self, manga_name, chapter_name, manga_path_word, chapter_uuid):
        """下载单个章节"""
        # 安全化文件名
        safe_manga_name = re.sub(r'[^\w\s.-]', '', manga_name).strip()
        safe_chapter_name = re.sub(r'[^\w\s.-]', '', chapter_name).strip()

        # 创建章节目录
        chapter_dir = os.path.join(self.MANGA_DIR, safe_manga_name, safe_chapter_name)
        os.makedirs(chapter_dir, exist_ok=True)

        # PDF输出路径
        pdf_filepath = os.path.join(chapter_dir, f"{safe_chapter_name}.pdf")

        # 获取章节图片信息
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                # 构建API URL
                url = f"https://www.copy-manga.com/api/v3/comic/{manga_path_word}/chapter/{chapter_uuid}?platform=1"

                response = await session.get(url)

                if response.status_code == 200:
                    chapter_data = json.loads(response.text)

                    if "results" not in chapter_data or "chapter" not in chapter_data["results"] or "contents" not in \
                            chapter_data["results"]["chapter"]:
                        return "获取章节图片信息失败"

                    image_urls = []
                    for content in chapter_data["results"]["chapter"]["contents"]:
                        image_urls.append(content["url"])

                    if not image_urls:
                        return "章节不包含任何图片"

                    # 下载所有图片
                    image_paths = []
                    tasks = []

                    for i, image_url in enumerate(image_urls):
                        filepath = os.path.join(chapter_dir, f"{i + 1:04d}.jpg")
                        image_paths.append(filepath)

                        if os.path.exists(filepath):
                            print(f"第 {i + 1}/{len(image_urls)} 页已存在")
                            continue

                        task = asyncio.create_task(
                            self.download_image(
                                session,
                                image_url,
                                filepath,
                                f"https://www.copy-manga.com/comic/{manga_path_word}/chapter/{chapter_uuid}"
                            )
                        )
                        tasks.append((task, i + 1))

                    success_count = 0
                    for task, page in tasks:
                        result = await task
                        if result:
                            success_count += 1
                            print(f"第 {page} 页下载成功")

                    # 生成PDF
                    if success_count > 0 or any(os.path.exists(path) for path in image_paths):
                        print(f"正在生成PDF: {pdf_filepath}")

                        # 使用img2pdf合并图片为PDF
                        existing_images = [path for path in image_paths if os.path.exists(path)]
                        existing_images.sort()

                        if existing_images:
                            with open(pdf_filepath, "wb") as f:
                                f.write(img2pdf.convert(existing_images))

                            # 删除原始图片
                            for img_path in existing_images:
                                try:
                                    os.remove(img_path)
                                except Exception as e:
                                    print(f"删除图片失败: {img_path}, 错误: {e}")

                            return "PDF生成成功"
                        else:
                            return "没有找到可用的图片，PDF生成失败"
                    else:
                        return "没有成功下载任何图片"
                else:
                    return f"获取章节图片信息失败，状态码: {response.status_code}"
        except Exception as e:
            return f"下载章节过程中出错: {e}"

    async def download_image(self, session, url, filepath, referer, max_retries=3):
        """下载单张图片"""
        headers = self.HEADERS.copy()
        headers["Referer"] = referer

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await session.get(url, headers=headers)

                    if response.status_code == 200:
                        # 保存图片
                        temp_filepath = filepath + ".temp"
                        with open(temp_filepath, 'wb') as f:
                            f.write(response.content)

                        # 转换图片格式
                        img = Image.open(temp_filepath)
                        img = img.convert('RGB')
                        img.save(filepath, format="JPEG", quality=85)

                        # 删除临时文件
                        os.remove(temp_filepath)
                        return True
                    elif attempt < max_retries - 1:
                        print(f"  重试 ({attempt + 1}/{max_retries})...")
                        await asyncio.sleep(1)
                    else:
                        print(f"  下载失败，状态码: {response.status_code}")

            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"  出错: {e}，重试...")
                    await asyncio.sleep(1)
                else:
                    print(f"  下载失败: {e}")

        return False

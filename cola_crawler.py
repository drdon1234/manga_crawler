import os
import re
import asyncio
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
os.environ['PYPPETEER_CHROMIUM_REVISION'] = '1263111'
from pyppeteer import launch
import img2pdf
from PIL import Image
from base_crawler import BaseCrawler


class ColaCrawler(BaseCrawler):
    """Cola漫画爬虫实现"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        """初始化Cola漫画爬虫"""
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.colamanga.com/",
            "Connection": "keep-alive"
        }
        super().__init__(proxies, headers, max_concurrency)
        self.browser = None

    async def init_browser(self):
        """初始化浏览器"""
        if not self.browser:
            self.browser = await launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    f'--proxy-server={self.PROXIES["http"].replace("http://", "")}' if self.PROXIES.get("http") else ''
                ]
            )
        return self.browser

    async def close_browser(self):
        """关闭浏览器"""
        if self.browser:
            await self.browser.close()
            self.browser = None

    async def search_manga(self, keyword, page=1):
        """搜索漫画并缓存结果"""
        # 检查缓存
        cache_key = f"{keyword}_page{page}"
        cached_results = self.load_from_cache("search", cache_key)

        if cached_results:
            print(f"从缓存加载搜索结果: {keyword}, 第{page}页")
            return self.format_search_results(cached_results)

        # 执行搜索
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                await session.get("https://www.colamanga.com")

                params = {"type": 1, "searchString": keyword, "page": page}
                response = await session.get("https://www.colamanga.com/search", params=params)

                if response.status_code == 200:
                    search_results = self.html_to_json(response.text)

                    # 保存到缓存
                    self.save_to_cache("search", cache_key, search_results)

                    return self.format_search_results(search_results)
                else:
                    return f"搜索失败，状态码: {response.status_code}"
        except Exception as e:
            return f"搜索失败: {e}"

    def html_to_json(self, html):
        """解析HTML为JSON数据"""
        soup = BeautifulSoup(html, 'html.parser')
        result = {
            "搜索结果数量": soup.select_one('#fed-count').text if soup.select_one('#fed-count') else "0",
            "当前页数": soup.select_one('#fed-now').text if soup.select_one('#fed-now') else "1",
            "漫画列表": []
        }

        for dl in soup.select('dl.fed-deta-info'):
            manga = {}
            title = dl.select_one('h1 a')
            if title:
                manga['标题'] = title.text.strip()
                manga['链接'] = f"https://www.colamanga.com/{title.get('href', '')}"

            cover = dl.select_one('a.fed-list-pics')
            if cover:
                manga['封面链接'] = cover.get('data-original', '')

            field_mapping = {
                '别名': '别名',
                '作者': '作者',
                '状态': '状态',
                '更新': '更新',
                '最新': '最新'
            }

            for li in dl.select('li'):
                label = li.select_one('.fed-text-muted')
                if not label: continue
                key = label.text.strip().rstrip('：')
                value = li.get_text().replace(label.text, '').strip()

                if key == '类别':
                    manga[key] = [a.text.strip() for a in li.select('a')]
                elif key in field_mapping:
                    manga[field_mapping[key]] = value

            result['漫画列表'].append(manga)

        return result

    def format_search_results(self, search_results):
        """格式化搜索结果为字符串"""
        if not search_results or len(search_results["漫画列表"]) == 0:
            return "未找到相关漫画"

        result_str = f"\n找到 {search_results['搜索结果数量']} 个相关漫画:\n"

        for i, manga in enumerate(search_results["漫画列表"]):
            result_str += f"{i + 1}. {manga['标题']}\n"
            result_str += f"   画廊链接: {manga['链接']}\n"

            for key in ["作者", "状态", "最新", "更新"]:
                if key in manga:
                    result_str += f"   {key}: {manga[key]}\n"

            if '类别' in manga:
                result_str += f"   类别: {', '.join(manga['类别'])}\n"

            if '别名' in manga:
                result_str += f"   别名: {manga['别名']}\n"

            result_str += "\n"

        return result_str

    async def get_manga_chapters(self, index_or_url):
        """获取漫画章节列表并缓存"""
        # 确定是索引还是URL
        manga_url = ""
        manga_title = ""

        if str(index_or_url).isdigit():
            # 是索引，从最新的搜索缓存获取漫画信息
            filepath, search_results = self.get_latest_cache("search")

            if not search_results:
                return "无搜索缓存，请先搜索漫画"

            idx = int(index_or_url) - 1
            if idx < 0 or idx >= len(search_results["漫画列表"]):
                return f"无效的索引: {index_or_url}"

            manga = search_results["漫画列表"][idx]
            manga_url = manga["链接"]
            manga_title = manga["标题"]
        else:
            # 是URL
            manga_url = index_or_url
            manga_title = "未知漫画"  # 尝试从页面获取

        # 解析出漫画ID用于缓存
        manga_id = manga_url.strip('/').split('/')[-1]

        # 检查缓存
        cached_chapters = self.load_from_cache("chapters", manga_id)

        if cached_chapters:
            print(f"从缓存加载章节列表: {manga_title}")
            return self.format_chapters_list(manga_title, cached_chapters["chapters"])

        # 获取章节列表
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                response = await session.get(manga_url)

                if response.status_code == 200:
                    # 尝试从页面提取标题（如果之前未知）
                    if manga_title == "未知漫画":
                        soup = BeautifulSoup(response.text, 'html.parser')
                        title_elem = soup.select_one('.fed-part-eone h1')
                        if title_elem:
                            manga_title = title_elem.text.strip()

                    # 解析章节
                    chapters = self.parse_chapters_html(response.text)

                    # 保存到缓存
                    chapters_data = {
                        "manga_title": manga_title,
                        "manga_url": manga_url,
                        "chapters": chapters
                    }
                    self.save_to_cache("chapters", manga_id, chapters_data)

                    return self.format_chapters_list(manga_title, chapters)
                else:
                    return f"获取章节列表失败，状态码: {response.status_code}"
        except Exception as e:
            return f"获取章节列表失败: {e}"

    def parse_chapters_html(self, html):
        """解析章节HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []

        chapter_container = soup.select_one('.all_data_list')

        if chapter_container:
            chapter_links = chapter_container.select('a.fed-btns-info')

            for a in chapter_links:
                chapter_url = f"https://www.colamanga.com{a.get('href')}"
                chapter_title = a.get('title') or a.text.strip()
                chapters.append({
                    "标题": chapter_title,
                    "链接": chapter_url
                })
        else:
            all_links = soup.select('.fed-part-rows a')
            start_index = -1
            end_index = -1

            for i, a in enumerate(all_links):
                text = a.text.strip()
                if text == "更多":
                    start_index = i
                elif text == "展开":
                    end_index = i
                    break

            if start_index != -1 and end_index != -1 and start_index < end_index:
                for a in all_links[start_index + 1:end_index]:
                    href = a.get('href')
                    title = a.text.strip()
                    if re.match(r'^\d+\s+.+', title) or re.match(r'^第\d+[话章]', title):
                        chapter_url = f"https://www.colamanga.com{href}"
                        chapters.append({
                            "标题": title,
                            "链接": chapter_url
                        })

        if not chapters:
            chapter_links = soup.select('.fed-part-rows a')
            for a in chapter_links:
                title = a.text.strip()
                if re.match(r'^\d+\s+.+', title) or re.match(r'^第\d+[话章]', title):
                    if not any(word in title for word in ["魂爆", "深渊妖兽", "梦寐以求"]):
                        href = a.get('href')
                        chapter_url = f"https://www.colamanga.com{href}"
                        chapters.append({
                            "标题": title,
                            "链接": chapter_url
                        })
        chapters.reverse()
        return chapters

    def format_chapters_list(self, manga_title, chapters):
        """格式化章节列表为字符串"""
        if not chapters:
            return f"{manga_title}: 未找到章节"

        result_str = f"\n{manga_title} 共 {len(chapters)} 章:\n"

        for i, chapter in enumerate(chapters):
            result_str += f"{i + 1}. {chapter['标题']}\n"

        return result_str

    async def download_manga(self, chapter_spec, index_or_url):
        """下载漫画章节，合并为PDF并删除图片"""
        try:
            # 获取漫画信息
            manga_url = ""
            manga_title = ""

            if str(index_or_url).isdigit():
                # 是索引，从最新的搜索缓存获取漫画信息
                filepath, search_results = self.get_latest_cache("search")

                if not search_results:
                    return "无搜索缓存，请先搜索漫画"

                idx = int(index_or_url) - 1
                if idx < 0 or idx >= len(search_results["漫画列表"]):
                    return f"无效的索引: {index_or_url}"

                manga = search_results["漫画列表"][idx]
                manga_url = manga["链接"]
                manga_title = manga["标题"]
            else:
                # 是URL
                manga_url = index_or_url

                # 尝试从缓存中获取标题
                manga_id = manga_url.strip('/').split('/')[-1]
                cached_chapters = self.load_from_cache("chapters", manga_id)

                if cached_chapters and "manga_title" in cached_chapters:
                    manga_title = cached_chapters["manga_title"]
                else:
                    manga_title = "未知漫画"

            # 获取章节列表
            manga_id = manga_url.strip('/').split('/')[-1]
            cached_chapters = self.load_from_cache("chapters", manga_id)

            if cached_chapters:
                chapters = cached_chapters["chapters"]
                manga_title = cached_chapters["manga_title"]
            else:
                # 没有缓存，需要获取章节列表
                async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                    response = await session.get(manga_url)

                    if response.status_code == 200:
                        # 尝试从页面提取标题（如果之前未知）
                        if manga_title == "未知漫画":
                            soup = BeautifulSoup(response.text, 'html.parser')
                            title_elem = soup.select_one('.fed-part-eone h1')
                            if title_elem:
                                manga_title = title_elem.text.strip()

                        # 解析章节
                        chapters = self.parse_chapters_html(response.text)

                        # 保存到缓存
                        chapters_data = {
                            "manga_title": manga_title,
                            "manga_url": manga_url,
                            "chapters": chapters
                        }
                        self.save_to_cache("chapters", manga_id, chapters_data)
                    else:
                        return f"获取章节列表失败，状态码: {response.status_code}"

            if not chapters:
                return f"{manga_title}: 未找到章节"

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

            # 初始化浏览器
            await self.init_browser()

            # 下载选中的章节
            results = []
            for chapter in selected_chapters:
                print(f"\n开始下载章节: {chapter['标题']}")

                # 获取漫画图片信息
                manga_id, encrypted_string, total_pages = await self.get_manga_image_info(chapter['链接'])

                if not manga_id or not encrypted_string or total_pages == 0:
                    results.append(f"{chapter['标题']}: 获取图片信息失败")
                    continue

                # 下载章节
                result = await self.download_manga_chapter(
                    manga_title,
                    chapter['标题'],
                    chapter['链接'],
                    manga_id,
                    encrypted_string,
                    total_pages
                )
                results.append(f"{chapter['标题']}: {result}")

            # 关闭浏览器
            await self.close_browser()

            return f"\n{manga_title} 下载完成:\n" + "\n".join(results)

        except Exception as e:
            await self.close_browser()
            return f"下载过程中出错: {e}"

    async def get_manga_image_info(self, chapter_url):
        """获取漫画图片信息"""
        try:
            # 确保浏览器已初始化
            browser = await self.init_browser()

            # 创建新页面
            page = await browser.newPage()

            # 设置UserAgent
            await page.setUserAgent(self.HEADERS['User-Agent'])

            # 访问章节页面
            await page.goto(chapter_url, {'waitUntil': 'networkidle0', 'timeout': 60000})

            # 等待页面元素加载
            await page.waitForSelector('#mangalist', {'timeout': 15000})

            # 获取cookie数据
            cookies = await page.cookies()
            total_pages = 0

            # 从cookie中提取总页数
            for cookie in cookies:
                if cookie['name'].startswith('_tkb_'):
                    total_pages = int(cookie['value'])
                    break

            # 尝试获取第一张图片
            first_image = None
            try:
                first_image = await page.evaluate('__cr_getpice(1)')
            except:
                # 如果JS执行失败，尝试从DOM获取
                elements = await page.querySelectorAll('img.fed-list-imgs')
                if elements and len(elements) > 0:
                    first_image = await page.evaluate('(element) => element.src', elements[0])

            # 关闭页面
            await page.close()

            if not first_image:
                print("无法获取图片URL")
                return None, None, 0

            # 解析图片URL获取必要信息
            parts = first_image.split('/')
            manga_id = parts[-3]
            encrypted_string = parts[-2]

            return manga_id, encrypted_string, total_pages

        except Exception as e:
            print(f"获取漫画信息失败: {e}")
            return None, None, 0

    async def download_manga_chapter(self, manga_title, chapter_title, chapter_url, manga_id, encrypted_string,
                                     total_pages):
        """下载单个章节"""
        # 安全化文件名
        safe_manga_title = re.sub(r'[^\w\s.-]', '', manga_title).strip()
        safe_chapter_title = re.sub(r'[^\w\s.-]', '', chapter_title).strip()

        # 创建章节目录
        chapter_dir = os.path.join(self.MANGA_DIR, safe_manga_title, safe_chapter_title)
        os.makedirs(chapter_dir, exist_ok=True)

        # PDF输出路径
        pdf_filepath = os.path.join(chapter_dir, f"{safe_chapter_title}.pdf")

        # 构建图片URL和本地保存路径
        image_urls = []
        image_paths = []

        for page in range(1, total_pages + 1):
            page_str = f"{page:04d}.jpg"
            image_url = f"https://img.colamanga.com/comic/{manga_id}/{encrypted_string}/{page_str}"
            filepath = os.path.join(chapter_dir, page_str)

            image_urls.append(image_url)
            image_paths.append(filepath)

        # 下载所有图片
        tasks = []
        async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
            for i, (url, filepath) in enumerate(zip(image_urls, image_paths)):
                if os.path.exists(filepath):
                    print(f"第 {i + 1}/{total_pages} 页已存在")
                    continue

                task = asyncio.create_task(
                    self.download_image(session, url, filepath, chapter_url)
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

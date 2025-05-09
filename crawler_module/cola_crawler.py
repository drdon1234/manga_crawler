import os
import re
import asyncio
from urllib.parse import urlsplit
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from PIL import Image
import img2pdf
from datetime import datetime
from io import BytesIO
import pyaes

os.environ['PYPPETEER_CHROMIUM_REVISION'] = '1263111'
from pyppeteer import launch
from base_crawler import BaseCrawler


class ColaCrawler(BaseCrawler):
    """Cola漫画爬虫优化版"""

    def __init__(self, proxies=None, headers=None, max_concurrency=10):
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": "https://www.colamanga.com",
            "Referer": "https://www.colamanga.com",
            "Connection": "keep-alive"
        }
        super().__init__(proxies, headers, max_concurrency)
        self.browser = None

    async def init_browser(self):
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
        if self.browser:
            await self.browser.close()
            self.browser = None

    async def search_manga(self, keyword, page=1):
        self.clear_cache("search")
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                await session.get("https://www.colamanga.com")
                params = {"type": 1, "searchString": keyword, "page": page}
                response = await session.get("https://www.colamanga.com/search", params=params)
                if response.status_code == 200:
                    search_results = self.html_to_json(response.text)
                    self.save_to_cache("search", search_results)
                    return self.format_search_results(search_results)
                else:
                    return f"搜索失败，状态码: {response.status_code}"
        except Exception as e:
            return f"搜索失败: {e}"

    def html_to_json(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        result = {
            "results": {
                "total": soup.select_one('#fed-count').text if soup.select_one('#fed-count') else "0",
                "list": []
            }
        }
        for dl in soup.select('dl.fed-deta-info'):
            manga = {}
            title = dl.select_one('h1 a')
            if title:
                manga['name'] = title.text.strip()
                manga['path_word'] = title.get('href', '').strip('/')
                manga['url'] = f"https://www.colamanga.com/{title.get('href', '')}"
            for li in dl.select('li'):
                label = li.select_one('.fed-text-muted')
                if not label: continue
                key = label.text.strip().rstrip('：')
                value = li.get_text().replace(label.text, '').strip()
                if key == '作者':
                    manga['author'] = [{"name": value}]
                elif key == '别名':
                    manga['alias'] = value
                elif key == '状态':
                    manga['status'] = value
                elif key == '类别':
                    manga['categories'] = [a.text.strip() for a in li.select('a')]
            manga['popular'] = "未知"
            result['results']['list'].append(manga)
        return result

    def format_search_results(self, search_results):
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
            result_str += "\n"
        return result_str

    async def get_manga_chapters(self, index_or_path):
        self.clear_cache("chapters")
        manga_path_word = ""
        manga_name = ""
        # 先尝试从search缓存取漫画信息
        if str(index_or_path).isdigit():
            search_results = self.load_from_cache("search")
            if not search_results or "results" not in search_results:
                # 缓存不存在时，提示用户需要先搜索
                return "无搜索缓存，请先搜索漫画"
            idx = int(index_or_path) - 1
            manga_list = search_results["results"]["list"]
            if idx < 0 or idx >= len(manga_list):
                return f"无效的索引: {index_or_path}"
            manga = manga_list[idx]
            manga_path_word = manga["path_word"]
            manga_name = manga["name"]
        else:
            manga_path_word = index_or_path
            manga_name = "未知漫画"
        # 章节缓存
        cached_chapters = self.load_from_cache("chapters")
        if cached_chapters:
            return self.format_chapter_list(manga_name, cached_chapters)
        # 缓存不存在，重新请求
        manga_url = f"https://www.colamanga.com/{manga_path_word}"
        try:
            async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                response = await session.get(manga_url)
                if response.status_code == 200:
                    chapters = self.parse_chapters(response.text)
                    self.save_to_cache("chapters", chapters)
                    return self.format_chapter_list(manga_name, chapters)
                else:
                    return f"获取章节列表失败，状态码: {response.status_code}"
        except Exception as e:
            return f"获取章节列表失败: {e}"

    def parse_chapters(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        chapter_container = soup.select_one('.all_data_list')
        if chapter_container:
            chapter_links = chapter_container.select('a.fed-btns-info')
            for a in chapter_links:
                chapter_url = f"https://www.colamanga.com{a.get('href')}"
                chapter_title = a.get('title') or a.text.strip()
                chapters.append({"name": chapter_title, "url": chapter_url})
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
                        chapters.append({"name": title, "url": chapter_url})
        chapters.reverse()
        return chapters

    def format_chapters_list(self, manga_name, chapters):
        if not chapters:
            return f"{manga_name}: 未找到章节"
        result_str = f"\n{manga_name} 共 {len(chapters)} 章:\n"
        for i, chapter in enumerate(chapters):
            result_str += f"{i + 1}. {chapter['name']}\n"
        return result_str

    async def get_manga_image_info(self, chapter_url):
        """获取漫画图片信息，并返回图片完整文件名（如enc.webp）"""
        try:
            browser = await self.init_browser()
            page = await browser.newPage()
            await page.setUserAgent(self.HEADERS['User-Agent'])
            await page.goto(chapter_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            await page.waitForSelector('#mangalist', {'timeout': 15000})

            cookies = await page.cookies()
            total_pages = 0
            for cookie in cookies:
                if cookie['name'].startswith('_tkb_'):
                    total_pages = int(cookie['value'])
                    break

            first_image = None
            try:
                first_image = await page.evaluate('__cr_getpice(1)')
            except:
                elements = await page.querySelectorAll('img.fed-list-imgs')
                if elements and len(elements) > 0:
                    first_image = await page.evaluate('(element) => element.src', elements[0])

            await page.close()

            if not first_image:
                print("无法获取图片URL")
                return None, None, 0, "jpg"

            # 提取图片完整文件名（如enc.webp）
            parsed_url = urlsplit(first_image)
            full_filename = os.path.basename(parsed_url.path)
            clean_filename = full_filename.split('?')[0]
            # 例如 clean_filename = "enc.webp"
            # 兼容性处理，无扩展名时默认jpg
            if '.' in clean_filename:
                image_ext = clean_filename.split('.', 1)[1]  # "enc.webp" -> "enc.webp"
            else:
                image_ext = "jpg"
            # 还要带上前缀（如"enc.webp"），所以直接用clean_filename
            image_filename = clean_filename

            parts = first_image.split('/')
            manga_id = parts[-3]
            encrypted_string = parts[-2]
            return manga_id, encrypted_string, total_pages, image_filename
        except Exception as e:
            print(f"获取漫画信息失败: {e}")
            return None, None, 0, "jpg"

    async def download_manga(self, chapter_spec, index_or_path):
        try:
            manga_path_word = ""
            manga_name = ""
            manga_url = ""
            if str(index_or_path).isdigit():
                search_results = self.load_from_cache("search")
                if not search_results or "results" not in search_results:
                    return "无搜索缓存，请先搜索漫画"
                idx = int(index_or_path) - 1
                manga_list = search_results["results"]["list"]
                if idx < 0 or idx >= len(manga_list):
                    return f"无效的索引: {index_or_path}"
                manga = manga_list[idx]
                manga_path_word = manga["path_word"]
                manga_name = manga["name"]
                manga_url = manga["url"]
            else:
                manga_path_word = index_or_path
                manga_url = f"https://www.colamanga.com/{manga_path_word}"

            chapters = self.load_from_cache("chapters")
            if not chapters:
                try:
                    async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
                        response = await session.get(manga_url)
                        if response.status_code == 200:
                            chapters = self.parse_chapters(response.text)
                            soup = BeautifulSoup(response.text, 'html.parser')
                            title_elem = soup.select_one('.fed-part-eone h1')
                            if title_elem:
                                manga_name = title_elem.text.strip()
                            self.save_to_cache("chapters", chapters)
                        else:
                            return f"获取章节列表失败，状态码: {response.status_code}"
                except Exception as e:
                    return f"获取章节列表失败: {e}"
            if not chapters:
                return f"{manga_name}: 未找到章节"

            # 章节选择逻辑
            if chapter_spec.lower() == 'all':
                selected_chapters = chapters
            elif '-' in chapter_spec:
                try:
                    start, end = map(int, chapter_spec.split('-'))
                    if start < 1 or end > len(chapters) or start > end:
                        return f"无效的章节范围: {chapter_spec}"
                    selected_chapters = chapters[start - 1:end]
                except ValueError:
                    return f"无效的章节范围格式: {chapter_spec}"
            else:
                try:
                    idx = int(chapter_spec) - 1
                    if idx < 0 or idx >= len(chapters):
                        return f"无效的章节索引: {chapter_spec}"
                    selected_chapters = [chapters[idx]]
                except ValueError:
                    return f"无效的章节索引格式: {chapter_spec}"

            results = []
            try:
                await self.init_browser()
                for chapter in selected_chapters:
                    print(f"\n开始下载章节: {chapter['name']}")
                    manga_id, encrypted_string, total_pages, image_filename = await self.get_manga_image_info(
                        chapter['url'])
                    if not manga_id or total_pages == 0:
                        results.append(f"{chapter['name']}: 信息获取失败")
                        continue
                    success_count = await self.download_manga_chapter(
                        manga_name,
                        chapter['name'],
                        chapter['url'],
                        manga_id,
                        encrypted_string,
                        total_pages,
                        image_filename
                    )
                    results.append(f"{chapter['name']}: 成功下载 {success_count}/{total_pages} 页")
            finally:
                await self.close_browser()
            return f"\n{manga_name} 下载完成:\n" + "\n".join(results)
        except Exception as e:
            await self.close_browser()
            return f"下载过程中出错: {e}"

    # 新增：捕获AES密钥功能
    async def capture_crypto_key(self, url):
        """捕获网页中的AES密钥并保存到缓存"""
        # 如果浏览器未初始化，先初始化
        if not self.browser:
            await self.init_browser()

        page = await self.browser.newPage()

        # 注入JavaScript钩子来捕获AES密钥
        await page.evaluateOnNewDocument('''() => {
            window.__capturedCryptoKey = null;
            function installHook() {
                if (window.CryptoJS && window.CryptoJS.AES && window.CryptoJS.AES.decrypt) {
                    const originalDecrypt = window.CryptoJS.AES.decrypt;
                    window.CryptoJS.AES.decrypt = function(message, key, config) {
                        window.__capturedCryptoKey = key;
                        return originalDecrypt.apply(this, arguments);
                    };
                    return true;
                }
                return false;
            }
            if (!installHook()) {
                const checkInterval = setInterval(() => {
                    if (installHook()) clearInterval(checkInterval);
                }, 100);
            }
        }''')

        await page.goto(url)
        await asyncio.sleep(1)
        crypto_key = await page.evaluate('() => window.__capturedCryptoKey')
        await page.close()

        words = []
        if crypto_key and isinstance(crypto_key, dict):
            words = crypto_key.get('words', [])

        # 提取漫画ID和页码
        manga_id, page_num = self.extract_manga_info(url)

        # 保存密钥到缓存
        dir_path = os.path.join(self.CACHE_DIR, 'aes_key')
        os.makedirs(dir_path, exist_ok=True)

        today = datetime.now()
        date_str = today.strftime('%Y_%m_%d')
        file_prefix = f"{manga_id}_{page_num}"

        # 清理旧密钥
        self.cleanup_old_keys(dir_path)

        # 保存新密钥
        bin_path = os.path.join(dir_path, f'{file_prefix}_{date_str}.bin')
        with open(bin_path, 'wb') as f:
            for num in words:
                f.write(num.to_bytes(4, byteorder='big'))

        print(f"已保存密钥到: {bin_path}")
        return bin_path

    def extract_manga_info(self, url):
        """从URL中提取漫画ID和页码"""
        match = re.search(r'manga-([^/]+)/\d+/(\d+)\.html', url)
        if match:
            manga_id = match.group(1)
            page_num = match.group(2)
            return manga_id, page_num
        return "unknown", "unknown"

    def cleanup_old_keys(self, dir_path):
        """删除aes_key目录下所有早于当天的密钥文件"""
        today = datetime.now().date()
        pattern = re.compile(r'^\d+_\d+_\d{4}_\d{2}_\d{2}\.bin$')
        for filename in os.listdir(dir_path):
            if pattern.match(filename):
                try:
                    date_match = re.search(r'_(\d{4}_\d{2}_\d{2})\.bin$', filename)
                    if date_match:
                        file_date_str = date_match.group(1)
                        file_date = datetime.strptime(file_date_str, '%Y_%m_%d').date()
                        if file_date < today:
                            file_path = os.path.join(dir_path, filename)
                            os.remove(file_path)
                            print(f"已删除旧密钥: {file_path}")
                except Exception as e:
                    print(f"处理文件 {filename} 时出错: {e}")

    def read_key_from_cache(self, url):
        """从缓存中读取AES密钥"""
        # 提取漫画ID和页码
        manga_id, page_num = self.extract_manga_info(url)

        # 构造文件名前缀
        file_prefix = f"{manga_id}_{page_num}"
        cache_dir = os.path.join(self.CACHE_DIR, 'aes_key')

        if not os.path.exists(cache_dir):
            print(f"缓存目录 {cache_dir} 不存在")
            return None

        # 找到最新的对应文件
        candidate_files = []
        for filename in os.listdir(cache_dir):
            if filename.startswith(file_prefix) and filename.endswith('.bin'):
                # 提取日期
                date_match = re.search(r'_(\d{4}_\d{2}_\d{2})\.bin$', filename)
                if date_match:
                    file_date = datetime.strptime(date_match.group(1), '%Y_%m_%d')
                    candidate_files.append((file_date, filename))

        if not candidate_files:
            print(f"未找到匹配的密钥文件: {file_prefix}_*.bin")
            return None

        # 按日期降序排序，取最新的
        candidate_files.sort(reverse=True)
        latest_file = candidate_files[0][1]
        key_path = os.path.join(cache_dir, latest_file)

        # 读取二进制密钥
        try:
            with open(key_path, 'rb') as f:
                key_bytes = f.read()
            return key_bytes
        except Exception as e:
            print(f"读取密钥文件失败: {e}")
            return None

    async def decrypt_webp_image(self, input_path, output_path, key_bytes):
        """使用pyaes解密AES-CBC加密的图片并直接保存为JPEG格式"""
        # 使用与JavaScript相同的IV格式
        iv = "0000000000000000".encode("utf-8")

        try:
            # 读取加密的文件
            with open(input_path, "rb") as f:
                encrypted_data = f.read()

            # 创建AES解密器并解密
            aes_cbc = pyaes.AESModeOfOperationCBC(key_bytes, iv=iv)
            decrypter = pyaes.Decrypter(aes_cbc)

            # 使用feed+feed()组合来处理数据和移除PKCS7填充
            raw_decrypted = decrypter.feed(encrypted_data)
            raw_decrypted += decrypter.feed()  # 空feed调用自动移除PKCS7填充

            # 直接将解密后的数据转换为JPEG并保存
            image = Image.open(BytesIO(raw_decrypted))
            image.convert("RGB").save(output_path, "JPEG", quality=85)
            return True
        except Exception as e:
            print(f"解密或转换失败: {e}")
            return False

    # 修改：添加解密处理到下载方法
    async def download_image(self, session, url, filepath, referer, chapter_url, max_retries=3):
        """下载图片，对enc.webp格式进行AES解密处理"""
        headers = self.HEADERS.copy()
        headers["Referer"] = referer

        # 检查文件扩展名是否为enc.webp
        is_enc_webp = 'enc.webp' in filepath.lower()

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await session.get(url, headers=headers)
                    if response.status_code == 200:
                        # 先保存原始文件
                        temp_filepath = filepath + ".temp"
                        with open(temp_filepath, 'wb') as f:
                            f.write(response.content)

                        if is_enc_webp:
                            # 获取解密后的文件路径
                            decrypted_filepath = filepath.replace('.enc.webp', '.jpg')

                            # 读取AES密钥
                            key_bytes = self.read_key_from_cache(chapter_url)

                            # 如果找不到密钥，尝试获取新密钥
                            if key_bytes is None:
                                print("缓存中未找到密钥，获取新密钥...")
                                await self.capture_crypto_key(chapter_url)

                                # 重新尝试读取缓存
                                key_bytes = self.read_key_from_cache(chapter_url)
                                if key_bytes is None:
                                    print("即使获取了新密钥，仍然无法从缓存中读取")
                                    return False

                            # 解密文件
                            success = await self.decrypt_webp_image(temp_filepath, decrypted_filepath, key_bytes)

                            # 如果解密失败，尝试获取新密钥重试
                            if not success:
                                print("使用缓存密钥解密失败，尝试获取新密钥...")
                                await self.capture_crypto_key(chapter_url)

                                # 重新读取新密钥
                                key_bytes = self.read_key_from_cache(chapter_url)
                                if key_bytes is None:
                                    print("无法读取新生成的密钥")
                                    return False

                                # 再次尝试解密
                                success = await self.decrypt_webp_image(temp_filepath, decrypted_filepath, key_bytes)
                                if not success:
                                    print("使用新密钥解密仍然失败，可能是图片格式问题")
                                    return False

                            # 解密成功，删除临时文件
                            try:
                                os.remove(temp_filepath)
                            except Exception as e:
                                print(f"删除临时文件失败: {e}")

                            return True
                        else:
                            # 常规图片处理
                            img = Image.open(temp_filepath)
                            img = img.convert('RGB')
                            img.save(filepath, format="JPEG", quality=85)
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

    # 修改：添加加密图片处理到章节下载方法
    async def download_manga_chapter(self, manga_name, chapter_name, chapter_url, manga_id, encrypted_string,
                                     total_pages, image_filename="0001.jpg"):
        """下载一个章节的所有图片，对于enc.webp格式进行解密处理"""
        safe_manga_name = re.sub(r'[^\w\s.-]', '', manga_name).strip()
        safe_chapter_name = re.sub(r'[^\w\s.-]', '', chapter_name).strip()
        chapter_dir = os.path.join(self.MANGA_DIR, safe_manga_name, safe_chapter_name)
        os.makedirs(chapter_dir, exist_ok=True)
        pdf_filepath = os.path.join(chapter_dir, f"{safe_chapter_name}.pdf")

        # 检查图片文件名是否为enc.webp格式
        is_enc_webp = 'enc.webp' in image_filename.lower()

        # 如果是enc.webp格式，需要先获取AES密钥
        if is_enc_webp:
            # 检查是否已有缓存的密钥
            key_bytes = self.read_key_from_cache(chapter_url)
            if key_bytes is None:
                print(f"开始获取章节 {chapter_name} 的AES密钥...")
                await self.capture_crypto_key(chapter_url)

        # 获取扩展名，enc.webp格式最终保存为jpg
        if '.' in image_filename:
            ext = image_filename.split('.', 1)[1]
        else:
            ext = "jpg"

        async with AsyncSession(proxies=self.PROXIES, headers=self.HEADERS, verify=False) as session:
            tasks = []
            image_paths = []

            for page in range(1, total_pages + 1):
                # 保持和网站一致的图片命名
                page_str = f"{page:04d}.{ext}"
                image_url = f"https://img.colamanga.com/comic/{manga_id}/{encrypted_string}/{page_str}"

                # 设置输出文件路径
                filepath = os.path.join(chapter_dir, page_str)

                # 如果是加密格式，最终输出路径为jpg
                final_path = filepath
                if is_enc_webp:
                    final_path = filepath.replace('.enc.webp', '.jpg')
                    image_paths.append(final_path)
                else:
                    image_paths.append(filepath)

                if os.path.exists(final_path):
                    print(f"第 {page}/{total_pages} 页已存在")
                    continue

                tasks.append(asyncio.create_task(
                    self.download_image(session, image_url, filepath, chapter_url, chapter_url)
                ))

            success_count = sum(await asyncio.gather(*tasks))

            try:
                print(f"正在生成PDF文件: {pdf_filepath}")
                existing_images = [p for p in image_paths if os.path.exists(p)]
                if existing_images:
                    with open(pdf_filepath, "wb") as f:
                        f.write(img2pdf.convert(sorted(existing_images)))
                    print(f"PDF生成成功，删除临时图片...")
                    for img_path in existing_images:
                        try:
                            os.remove(img_path)
                        except Exception as e:
                            print(f"删除图片失败: {e}")
            except Exception as e:
                print(f"PDF生成失败: {e}")

            return success_count

    async def close(self):
        await self.close_browser()
        self.thread_pool.shutdown()
        await super().close() if hasattr(super(), 'close') else None

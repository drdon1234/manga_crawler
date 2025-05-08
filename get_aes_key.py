import asyncio
import os
import re
from datetime import datetime
os.environ['PYPPETEER_CHROMIUM_REVISION'] = '1263111'
from pyppeteer import launch


async def capture_crypto_key(url):
    browser = await launch(headless=True)
    page = await browser.newPage()
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
    await browser.close()

    words = []
    if crypto_key and isinstance(crypto_key, dict):
        words = crypto_key.get('words', [])
    manga_id, page_num = extract_manga_info(url)
    dir_path = 'cache/aes_key'
    os.makedirs(dir_path, exist_ok=True)
    today = datetime.now()
    date_str = today.strftime('%Y_%m_%d')
    file_prefix = f"{manga_id}_{page_num}"
    cleanup_old_keys(dir_path, file_prefix, today)
    bin_path = os.path.join(dir_path, f'{file_prefix}_{date_str}.bin')
    with open(bin_path, 'wb') as f:
        for num in words:
            f.write(num.to_bytes(4, byteorder='big'))
    print(f"已保存密钥到: {bin_path}")
    return words

def extract_manga_info(url):
    match = re.search(r'manga-([^/]+)/\d+/(\d+)\.html', url)
    if match:
        manga_id = match.group(1)
        page_num = match.group(2)
        return manga_id, page_num
    return "unknown", "unknown"

def cleanup_old_keys(dir_path, file_prefix, today):
    prefix_pattern = re.compile(f'^{file_prefix}_\\d{{4}}_\\d{{2}}_\\d{{2}}\\.bin$')

    for filename in os.listdir(dir_path):
        if prefix_pattern.match(filename):
            try:
                date_match = re.search(r'_(\d{4}_\d{2}_\d{2})\.bin$', filename)
                if date_match:
                    file_date_str = date_match.group(1)
                    file_date = datetime.strptime(file_date_str, '%Y_%m_%d')

                    # 只删除早于今天的文件
                    if file_date.date() < today.date():
                        file_path = os.path.join(dir_path, filename)
                        os.remove(file_path)
                        print(f"已删除旧文件: {file_path}")
            except Exception as e:
                print(f"处理文件 {filename} 时出错: {e}")


if __name__ == "__main__":
    url = "https://www.colamanga.com/manga-ct492855/1/97.html"
    key = asyncio.run(capture_crypto_key(url))
    print(f"捕获的密钥: {key}")

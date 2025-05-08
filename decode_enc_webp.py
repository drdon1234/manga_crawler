import os
import re
import subprocess
import pyaes
from datetime import datetime
from PIL import Image
from io import BytesIO


def read_key_from_cache(url, cache_dir='cache/aes_key'):
    """根据URL从缓存目录查找对应的最新密钥"""
    # 提取漫画ID和页码
    match = re.search(r'manga-([^/]+)/\d+/(\d+)\.html', url)
    if not match:
        print("URL格式不正确，无法提取漫画ID和页码")
        return None
    manga_id, page_num = match.group(1), match.group(2)

    # 构造文件名前缀
    file_prefix = f"{manga_id}_{page_num}"

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
        print(f"从缓存文件读取密钥: {key_path}")
        return key_bytes
    except Exception as e:
        print(f"读取密钥文件失败: {e}")
        return None


def decrypt_webp_image(input_path, output_path, key_bytes):
    """使用pyaes解密AES-CBC加密的图片并直接保存为JPEG格式"""
    # 使用与JavaScript相同的IV格式
    iv = "0000000000000000".encode("utf-8")

    try:
        # 读取加密的文件
        with open(input_path, "rb") as f:
            encrypted_data = f.read()

        print(f"使用密钥: {key_bytes}")

        # 创建AES解密器并解密（使用pyaes）
        aes_cbc = pyaes.AESModeOfOperationCBC(key_bytes, iv=iv)
        decrypter = pyaes.Decrypter(aes_cbc)

        # 使用feed+feed()组合来处理数据和移除PKCS7填充
        raw_decrypted = decrypter.feed(encrypted_data)
        raw_decrypted += decrypter.feed()  # 空feed调用自动移除PKCS7填充

        # 直接将解密后的数据转换为JPEG并保存
        image = Image.open(BytesIO(raw_decrypted))
        image.convert("RGB").save(output_path, "JPEG")
        print(f"解密成功！图片已保存为 {output_path}")
        return True
    except Exception as e:
        print(f"解密或转换失败: {e}")
        return False


def get_fresh_key(url):
    """调用get_aes_key.py脚本获取新密钥"""
    print(f"调用get_aes_key.py获取新密钥，URL: {url}")
    os.environ['PYTHONIOENCODING'] = 'utf-8'  # 解决编码问题
    try:
        result = subprocess.run(['python', 'get_aes_key.py', url],
                                capture_output=True, text=True,
                                encoding='utf-8', check=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"调用get_aes_key.py失败: {e}")
        print(f"错误输出: {e.stderr}")
        return False
    except Exception as e:
        print(f"调用get_aes_key.py时发生异常: {e}")
        return False


def main(url, input_file, output_file):
    """主函数：尝试使用缓存密钥，失败则获取新密钥"""
    # 首先尝试从缓存读取密钥
    key_bytes = read_key_from_cache(url)

    # 如果找不到缓存的密钥，调用get_aes_key.py获取
    if key_bytes is None:
        print("缓存中未找到密钥，获取新密钥...")
        if not get_fresh_key(url):
            print("获取新密钥失败，程序退出")
            return False

        # 重新尝试读取缓存
        key_bytes = read_key_from_cache(url)
        if key_bytes is None:
            print("即使获取了新密钥，仍然无法从缓存中读取，程序退出")
            return False

    # 尝试解密
    success = decrypt_webp_image(input_file, output_file, key_bytes)

    # 如果解密失败，尝试获取新密钥重试
    if not success:
        print("使用缓存密钥解密失败，尝试获取新密钥...")
        if not get_fresh_key(url):
            print("获取新密钥失败，程序退出")
            return False

        # 重新读取新密钥
        key_bytes = read_key_from_cache(url)
        if key_bytes is None:
            print("无法读取新生成的密钥，程序退出")
            return False

        # 再次尝试解密
        success = decrypt_webp_image(input_file, output_file, key_bytes)
        if not success:
            print("使用新密钥解密仍然失败，可能是图片格式问题")
            return False

    return True


if __name__ == "__main__":
    # 设置URL和文件路径
    url = 'https://www.colamanga.com/manga-ct492855/1/97.html'
    input_file = "cache/0001.enc.webp"
    output_file = "cache/0001_decrypted.jpg"

    # 执行主流程
    if main(url, input_file, output_file):
        print("处理完成")
    else:
        print("处理失败")

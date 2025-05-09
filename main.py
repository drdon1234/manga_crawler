import asyncio
from crawler_module.cola_crawler import ColaCrawler
from crawler_module.copy_crawler import CopyCrawler

PROXIES = None

'''PROXIES = {
    "http":"http://127.0.0.1:7897",
    "https":"http://127.0.0.1:7897"
}'''

async def main():
    print("漫画爬虫下载工具")
    print("================")

    # 选择漫画源
    print("\n请选择漫画源:")
    print("1. ColaManga (默认)")
    print("2. CopyManga")
    source_choice = input("请输入选项 [1/2]: ").strip() or "1"

    crawler = ColaCrawler(proxies=PROXIES) if source_choice == "1" else CopyCrawler(proxies=PROXIES)

    # 选择操作类型
    print("\n请选择操作类型:")
    print("1. 搜索漫画")
    print("2. 获取章节列表")
    print("3. 下载漫画")
    action_choice = input("请输入选项 [1/2/3]: ").strip()

    if action_choice == "1":
        # 搜索漫画
        keyword = input("\n请输入搜索关键词: ").strip()
        if not keyword:
            print("错误: 搜索操作需要提供关键词")
            return

        page = input("请输入页数 [默认1]: ").strip() or "1"
        try:
            page = int(page)
        except ValueError:
            print("无效的页数，使用默认值1")
            page = 1

        result = await crawler.search_manga(keyword, page)
        print(result)

    elif action_choice == "2":
        # 获取章节列表
        index_or_url = input("\n请输入漫画索引或URL/path_word: ").strip()
        if not index_or_url:
            print("错误: 获取章节操作需要提供索引或URL/path_word")
            return

        result = await crawler.get_manga_chapters(index_or_url)
        print(result)

    elif action_choice == "3":
        # 下载漫画
        index_or_url = input("\n请输入漫画索引或URL/path_word: ").strip()
        if not index_or_url:
            print("错误: 下载操作需要提供索引或URL/path_word")
            return

        print("\n章节选择格式说明:")
        print("- 单章节: 输入章节编号, 例如: 1")
        print("- 范围章节: 输入起始-结束, 例如: 1-5")
        print("- 全部章节: 输入 all")

        chapter_spec = input("请输入要下载的章节: ").strip()
        if not chapter_spec:
            print("错误: 下载操作需要提供章节范围")
            return

        result = await crawler.download_manga(chapter_spec, index_or_url)
        print(result)

    else:
        print("无效的操作选择")


if __name__ == '__main__':
    # 运行主函数
    asyncio.run(main())

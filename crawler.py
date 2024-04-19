from queue import Queue
from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ptt"]
movies_by_threads = db["movies_by_threads"]

class Push:
    def __init__(self, push_tag: str, push_userid: str, push_content: str, push_time: str):
        self.push_tag = push_tag
        self.push_userid = push_userid
        self.push_content = push_content
        self.push_time = datetime.strptime(f"{push_time}", "%m/%d %H:%M")

class Article:
    def __init__(self, article_id: int, author: str, title: str, article_time: datetime, content: str, pushes: list[Push]):
        self.article_id = article_id # 第幾篇文章
        self.author = author
        self.title = title
        self.article_time = article_time
        self.content = content
        self.pushes = pushes

article_queue: Queue[Article] = Queue()

class Crawler:
    def __init__(self):
        self.driver = webdriver.Chrome()
        # 隱性等待
        # 由webdriver提供的方法，設置後會在WebDriver實例的整個生命週期起作用
        # 非針對特定元素，而是全局元素的等待，定位元素時，需要等待頁面全部元素載入完成(瀏覽器左上角圈圈不再轉)才會執行下一步
        # 若提早載入完成會直接結束等待，超出等待時間則拋出異常
        self.driver.implicitly_wait(10)

        self.crawler_thread = Thread(target=self.run)
        self.crawler_thread.daemon = True
        self.crawler_thread.start()

    def scrape(self, href: str, title: str):
        self.driver.get(href)
        soup = BeautifulSoup(self.driver.page_source, "lxml")

        author_span = soup.find("span", class_="article-meta-tag", string="作者")
        article_time_span = soup.find("span", class_="article-meta-tag", string="時間")

        if author_span and article_time_span:
            author = author_span.next_sibling.text.strip()
            article_time = article_time_span.next_sibling.text.strip()

        else:
            # 文章格式不正確
            author = ""
            article_time = ""

        main_content = soup.find("div", id="main-content").text
        article = main_content.split("\n--\n")[0]
        lines = article.split("\n")
        if "標題" not in lines[0]:
            content = "\n".join(lines[:])

        else:
            content = "\n".join(lines[1:])

        # 回文
        push_objs: list[Push] = []
        url_span = soup.select("span.f2")[-1]
        pushes = url_span.find_all_next("div", class_="push")

        for push in pushes:
            spans = push.contents

            try:
                push_tag = spans[0].text.strip()
                push_userid = spans[1].text.strip()
                push_content = spans[2].text.strip()
                push_time = spans[3].text.strip()
        
            except IndexError:
                # 少數文章由於回文太多，會出現結構不同的提示訊息，直接跳過
                continue

            push_objs.append(
                Push(
                    push_tag=push_tag,
                    push_userid=push_userid,
                    push_content=push_content,
                    push_time=push_time
                ).__dict__
            )
        
        article_id = movies_by_threads.count_documents({}) + 1
        article_queue.put_nowait(
            Article(
                article_id=article_id,
                author=author,
                title=title,
                article_time=article_time,
                content=content,
                pushes=push_objs
            ).__dict__
        )

    def run(self):
        try:
            # 抓取文章連結
            # 最新頁
            self.driver.get(f"https://www.ptt.cc/bbs/movie/index.html")

            for a in self.driver.find_elements(By.CSS_SELECTOR, ".title a"):
                href = a.get_attribute("href")
                title = a.text.strip()

                # 抓取文章標題、內容、回文
                self.scrape(href, title)

                # 返回文章列表
                self.driver.back()

            # 往後1000頁
            for page in range(1000):
                self.driver.find_element(By.XPATH, "//a[@class='btn wide' and contains(text(), '‹ 上頁')]").click()

                for a in self.driver.find_elements(By.CSS_SELECTOR, ".title a"):
                    href = a.get_attribute("href")
                    title = a.text.strip()

                    # 抓取文章標題、內容、回文
                    self.scrape(href, title)

                    # 返回文章列表
                    self.driver.back()

        finally:
            self.driver.quit()

class Saver:
    def __init__(self):
        self.saver_thread = Thread(target=self.run)
        self.saver_thread.daemon = True
        self.saver_thread.start()

    def run(self):
        while True:
            if article_queue.empty():
                continue

            else:
                article = article_queue.get_nowait()
                movies_by_threads.update_one(
                    # 檢查重複
                    # 同一個作者不會同一時間發超過一篇文
                    {"author": article["author"], "article_time": article["article_time"]},
                    {"$set": article},
                    upsert=True
                )

class Main:
    def get_list(self):
        article_list = list(
            movies_by_threads.aggregate(
                [
                    # {"$sample": {"size": 15}},
                    {"$project": {"_id": 0, "article_id": 1, "title": 1}}
                ]
            )
        )

        print(f"article_id     title")

        # for article in article_list:
        #     print(f"{article['article_id'] : <15}{article['title']}")
        
        print(len(article_list))

    def get_article(self):

        def print_article(article):
            for k, v in article.items():
                if k == "pushes":
                    print("pushes\n")

                    for i in v:
                        push_time = datetime.strftime(i['push_time'], '%m-%d %H:%M')
                        push_string = f"{i['push_tag']} {i['push_userid']} {i['push_content']} {push_time}"
                        print(push_string, "\n")
                
                else:
                    print(k, "\n")
                    print(v, "\n")

        while True:
            try:
                query_str = input("Enter query string or exit to switch to other actions: ").strip()
                article = movies_by_threads.find_one({"title": query_str}, {"_id": 0})

                if query_str == "exit":
                    break

                else:
                    if article:
                        print_article(article)

                    else:
                        print("Article not found, try again or enter 'exit' to switch to other actions.")

            except ValueError:
                print("Only accept integers, try again.")

    def menu(self):
        while True:
            try:
                print("\n1: Get the list of 15 articles. 2: Get the specified article. 3: Exit.")
                action = int(input("Enter an action: "))
                
                if action == 1:
                    self.get_list()

                elif action == 2:
                    self.get_article()
                    
                elif action == 3:
                    print("Bye.")
                    break

            except ValueError as e:
                print("Only accept integers, try again.")

crawler = Crawler()
saver = Saver()
main = Main()
main.menu()
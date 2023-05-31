import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup


class StartCollectLinks:
    """Don't use any Proxy or Code will be Crash"""

    def __init__(self, keyword):
        """
        saving links of top 100 links of google search by given keyword
        :param keyword: Searching Keywords
        :type keyword: str
        """
        self.results = []
        self.searching = True
        self.visited_page = 0
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        self.wait = WebDriverWait(self.driver, 20)
        self.keyword = keyword
        self.next_path = '//*[@id="pnnext"]/span[2]'  # path of the next element on Google page we can change it late
        self.start_search()

    def start_search(self):
        self.driver.maximize_window()
        self.driver.get('https://google.com')
        search_filed = self.driver.find_element(By.TAG_NAME, 'textarea')
        search_filed.send_keys(self.keyword)
        search_filed.send_keys(Keys.ENTER)
        self.start_collect()

    def start_collect(self):
        if self.searching:

            time.sleep(5)  # on slow networks these sleeps are severeness for responding

            self.scroll()

            time.sleep(20)

            htmls = self.driver.page_source
            soup = BeautifulSoup(htmls, 'html.parser')
            links = soup.find_all(name='cite')

            for item in links:

                if item.text.split('›')[0] not in self.results:
                    self.results.append(item.text.split('›')[0])

            self.next_page()
            self.start_collect()
        else:

            return self.results

    def scroll(self):
        element = self.driver.find_element(By.XPATH, self.next_path)
        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)

    def next_page(self):
        print('next')
        self.visited_page += 1
        if self.visited_page > 10:
            self.searching = False
        next_btn = self.driver.find_element(By.XPATH, self.next_path)
        next_btn.click()
        time.sleep(10)




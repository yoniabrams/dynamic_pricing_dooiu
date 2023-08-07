import csv

import grequests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import logging

from tqdm import tqdm

# Create a logger
logger = logging.getLogger("scraper_logger")
logger.setLevel(logging.DEBUG)

# Create a file handler to write logs to the file
log_file = "scraper_logger.log"
file_handler = logging.FileHandler(log_file)

# Create a formatter with the desired log format
log_format = "%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)

# Attach the formatter to the file handler
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


UA = UserAgent()
TIMEOUT = 5
WAIT = 20
DEFAULT_CSV_FILENAME = 'saved_list.csv'

BASE_URL = 'https://clarity.fm'
BROWSE_BASE_URL = 'https://clarity.fm/browse'
DIV_CATEGORIES_WRAPPER = 'categories-wrapper'

URL_ATTRIBUTE = 'href'

REGEX_UL_PATTERN = r'<li class="member-item list-item" data-href="([^"]+)">'
REGEX_HREF_PATTERN = r'<a href="(.*?)"'
REGEX_CATEGORY_PATTERN = r'/([^/]+)-\d+$'
REGEX_MINUTE_RATE_PATTERN = r'\$(\d+(\.\d+)?)'

MAX_CLICKS = 100  # arbitrary
MAX_CLICK_FAILS = 2  # TODO CHANGE TO 5

DEBUG = False


def click_load_more_and_get_hrefs(url):
    """
    * From a page listing all consultants who use a particular category tag, click the "load more" button until it can't
    be clicked anymore.
    * Find the element in the html which stores the list of consultants.
    * Extract html code and then use regex to extract url for each consultant
    :param url: url for the list of consultants ina  particular category
    :return: (list) Hrefs for all consultants pages who fall in a category
    """
    driver = webdriver.Chrome()
    driver.get(url)
    wait = WebDriverWait(driver, WAIT)
    count = 0
    fails_in_a_row = 0

    while True:
        try:
            # get button, waiting until it's there
            button = wait.until(
            EC.element_to_be_clickable((By.XPATH, r'//*[@id="container"]/article/div[2]/article/div[1]/div/div/div/div/button'))
            )
            # click button
            button.click()
            count += 1  # if successful
            fails_in_a_row = 0  # if successful
            logger.debug(f"{count=}")
            # wait for it to load
            driver.implicitly_wait(WAIT)

            if count >= MAX_CLICKS:
                break
        except:
            fails_in_a_row += 1
            logger.debug(f"{fails_in_a_row=}")
            if fails_in_a_row >= MAX_CLICK_FAILS:
                break  # stop trying
            else:
                continue  # try again

    # find <ul> table element | extract html code into str
    ul_element = driver.find_element(By.CLASS_NAME, 'contacts-list.experts')
    ul_html = ul_element.get_attribute('outerHTML')
    driver.quit()

    href_attributes = re.findall(REGEX_HREF_PATTERN, ul_html)
    logger.debug(f'# links: {len(href_attributes)}')

    return href_attributes


def get_response_then_get_soup(url):
    """
    Param: url (str) url from a clarity webpage.
    Returns: a soup object of the BeautifulSoup library.

    [Motivation: Often, getting a response from a website using the grequests library can take a long time.
    It is more efficient to try again after a certain short time-period, especially with a website like Tripadvisor
    which has an unstable server.
    After getting a response, this function will then generate a soup object from the BeautifulSoup library.]
    """
    headers = {"User-Agent": UA.random}
    while True:
        req = grequests.get(url, headers=headers, timeout=TIMEOUT).send()
        response = grequests.map([req])[0]
        if response is not None and response.status_code == 200:
            break

    html = response.text
    soup = BeautifulSoup(html, features="html.parser")
    return soup


def get_all_topic_urls_from_file(dst='clarity_topic_uls.csv', src='ul_class.txt'):
    """
    The ul-table of all categories from the HTML code of clarity.com was copies into a text file.
    From that text file, we use regular expressions to grab the relative paths of the urls to all categories
    They are saves into a list and written to a csv.
    :param src: Source path (the ul-table element)
    :param dst: Destination of the csv file
    :return: None
    """
    with open(src, 'r') as file:
        ul_table = file.read()

    matches = re.findall(REGEX_UL_PATTERN, ul_table)
    topics_urls = [BASE_URL + match for match in matches]

    with open(dst, 'w') as csv_file:
        for url in topics_urls:
            csv_file.write(url + '\n')


def get_all_consultants_urls(safe_mode=False):
    """
    return all consultant urls for all the topics on clarity.com
    """
    topic_urls = list()
    with open('clarity_topic_uls.csv', 'r') as csv_file:
        reader = csv.reader(csv_file)
        for i, row in enumerate(reader):
            if row:
                if i <= 2145:  # TODO CHANGE BACK
                    continue
                url = row[0]
                topic_urls.append(url)

    all_urls_dict = dict()
    for i, url in enumerate(topic_urls):
        logger.debug(f"{i=} of {len(topic_urls)} | {url=}")
        href_list = click_load_more_and_get_hrefs(url)
        href_list = [BASE_URL + href for href in href_list]  # add the base url to the relative url path

        # extract category from the category url | use as key for dictionary
        match = re.search(REGEX_CATEGORY_PATTERN, url)
        if match:
            category = match.group(1)
        else:
            category = url
        logger.debug(f"{category=}")
        all_urls_dict[category] = href_list

        if safe_mode and i % 20 == 0:
            with open('save_urls.txt', 'w') as safety_net:
                safety_net.write(str(all_urls_dict))

    return all_urls_dict


def get_consultant_data(url):
    """
    From a consultant's url on clarity.com, gather all the relevant data
    :param url: The webpage of a consultant on clarity.com
    :return: dictionary of information
    """
    consultant_data = dict()

    soup = get_response_then_get_soup(url)

    # get name
    try:
        profile_div = soup.find('div', class_='profile')
        name_header1 = profile_div.find('h1')
        name = name_header1.text.strip()
        name = name.encode('ascii', 'ignore').decode('utf-8')  # sometimes the users use emojis
    except:
        name = None

    consultant_data['name'] = name
    logger.debug(f'{name=}')

    # get location:
    try:
        profile_div = soup.find('div', class_='profile')
        location_div = profile_div.find('div', class_='location')
        location = location_div.text.strip()
        location = location.encode('ascii', 'ignore').decode('utf-8')  # sometimes the users use emojis
    except:
        location = None

    consultant_data['location'] = location
    logger.debug(f'{location=}')

    # get price info from sidebar
    try:
        driver = webdriver.Chrome()
        driver.get(url)
        wait = WebDriverWait(driver, WAIT)
        sidebar = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, r'//*[@id="container"]/article/div[2]/article/div[1]/div[1]/div[1]/div/div[2]'))
        )
        hourly_rate_box_div = wait.until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, 'hourly-rate-box'))
        )
        match = re.search(REGEX_MINUTE_RATE_PATTERN, hourly_rate_box_div.text)
        if match:
            price = match.group(1)
        else:
            price = None
    except:
        price = None

    consultant_data['price'] = price
    logger.debug(f'{price=}')

    # get linkedin url:
    try:
        dropdown_menu_button = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, r'//*[@id="container"]/article/div[2]/article/div[1]/div[1]/div[1]/div/div[2]/div[2]/div[2]/div[1]'))
        )
        dropdown_menu_button.click()
        link_menu = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, r'//*[@id="container"]/article/div[2]/article/div[1]/div[1]/div[1]/div/div[2]/div[2]/div[2]/div[2]')
            )
        )
        linkedin_element = link_menu.find_element(By.CLASS_NAME, r'external.linkedin.linked.verified')
        linkedin_link = linkedin_element.get_attribute('href')
    except:
        linkedin_link = None

    consultant_data['linkedin'] = linkedin_link
    logger.debug(f'{linkedin_link=}')

    # twitter link:
    try:
        twitter_element = link_menu.find_element(By.CLASS_NAME, r'external.twitter.linked.verified')
        twitter_link = twitter_element.get_attribute('href')
    except:
        twitter_link = None

    consultant_data['twitter'] = twitter_link
    logger.debug(f'{twitter_link=}')

    # number of stats out of 5 | num reviews:
    try:
        stars_div = sidebar.find_element(By.CLASS_NAME, 'stars')
        stars = stars_div.find_elements(By.TAG_NAME, 'i')

        stars_count = 5  # start with 5 and subtract one per empty star
        for i, star in enumerate(stars):
            if star.get_attribute('class') == 'star empty':
                stars_count -= 1
        rating = stars_count
        reviews = stars_div.text.strip()[1:-1]  # the number of reviews | '(7)' --> '7'
    except:
        rating = None
        reviews = None

    consultant_data['rating'] = rating
    consultant_data['reviews'] = reviews
    logger.debug(f'{rating=} | {reviews=}')

    logger.debug(f'{consultant_data=}')

    return consultant_data


def get_all_topic_urls(src='clarity_topic_uls.csv'):
    topics_urls = list()
    with open(src, 'r') as topics_csv:
        reader = csv.reader(topics_csv)
        for row in reader:
            if row:
                url = row[0]
                topics_urls.append(url)
    return topics_urls


def get_all_consultant_data_for_all_topics():
    """
    This function collects all the consultant data from all the different categories on clarity.com.
    The data is stored in a csv.
    :return: None
    """
    all_urls_dict = get_all_consultants_urls(safe_mode=True)
    all_consultant_data = list()  # will be list of dictionaries

    for topic in list(all_urls_dict.keys()):  # TODO CHANGE THIS BACK!!!!!
        logger.debug(f'{topic=}')

        for i, consultant_url in enumerate(all_urls_dict[topic]):
            logger.debug(f'{consultant_url=}')

            consultant_data = get_consultant_data(consultant_url)
            consultant_data['category'] = topic
            all_consultant_data.append(consultant_data)

            if i % 20 == 0:
                logger.debug(f'{i=} of {len(all_urls_dict[topic])}')
                with open('just_in_case.txt', 'w') as safety_net:
                    safety_net.write(str(all_consultant_data))

    df = pd.DataFrame(all_consultant_data)
    df.to_csv('all_data.csv')


if __name__ == '__main__':
    get_all_consultant_data_for_all_topics()

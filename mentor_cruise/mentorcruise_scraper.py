import csv

import grequests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
import pandas as pd

UA = UserAgent()
TIMEOUT = 5
DEFAULT_CSV_FILENAME = 'saved_list.csv'

BASE_URL = 'https://mentorcruise.com'
MENTOR_SEARCH_URL = 'https://mentorcruise.com/mentor/browse/?return=minimal'
BROWSE_PATH = '/mentor/browse'

DIV = 'div'
A = 'a'
SPAN = 'span'

DIV_CLASS_MENTOR = 'relative box px-7 py-8 transition-all duration-150 mb-12 max-w-screen-lg mx-auto'
DIV_NEXT_PAGE = '-mt-px w-0 flex-1 flex justify-end'
DIV_BASIC_DATA = 'mt-5 font-normal text-slate-600'
DIV_MENTOR_DATA = 'w-full lg:w-1/2 xl:w-2/3 relative py-4 px-4 sm:px-8'
DIV_PRICE_ELEMENT = 'plan-details'
SPAN_BASIC_DATA = 'block mb-2'
SPAN_PRICE_ELEMENT = 'price-element'
INFO_H1 = 'text-slate-900 font-bold text-2xl mb-1'
INFO_H2 = INFO_H1
MT6 = 'mt-6'
URL_ATTRIBUTE = 'href'


def get_response_then_get_soup(url):
    """
    Param: url (str) url from a mentorcruise webpage.
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


def get_mentor_urls_from_page(soup_of_page):
    """
    From a soup object of a page of mentors on mentorcruise, get a list of all the mentors urls from the page
    :param soup_of_page: soup of the page of the mentor browse pages
    :return: urls of the mentors.
    """
    urls = list()
    all_speaker_divs = soup_of_page.find_all(DIV, class_=DIV_CLASS_MENTOR)
    for div_element in all_speaker_divs:
        a_element = div_element.find(A)
        mentor_path = a_element[URL_ATTRIBUTE]
        urls.append(BASE_URL + mentor_path)
    return urls


def get_next_page_soup(current_page_soup):
    """
    :param current_page_soup: a soup object of the BeautifulSoup library for a pg on mentor_cruise which lists mentors
    :return: Soup object for the NEXT page which lists mentors
    """
    # get div_element for the 'next' button
    next_element = current_page_soup.find(DIV, class_=DIV_NEXT_PAGE)

    # check if there is a next page element:
    if next_element is None:
        return None

    # get the url relative path to the next page
    next_page_path = next_element.a[URL_ATTRIBUTE]

    # construct the full url to the next page
    next_url = BASE_URL + BROWSE_PATH + next_page_path

    # get soup object for the next page url
    soup = get_response_then_get_soup(next_url)
    return soup


def get_all_mentor_urls(url):
    all_urls = list()
    soup = get_response_then_get_soup(url)
    page_count = 0
    while True:
        page_count += 1
        urls_from_page = get_mentor_urls_from_page(soup)
        print(urls_from_page)
        all_urls.extend(urls_from_page)

        soup = get_next_page_soup(soup)

        # if there's no next page, break from loop and return all_urls as is
        if soup is None:
            break

    print(page_count)
    return all_urls


def write_list_to_csv(lst, filename=DEFAULT_CSV_FILENAME):
    """
    save a list ot a csv, one element per row
    :param lst: a list of items
    :param filename: the filename by which to save the list
    :return: None
    """
    with open(filename, 'w') as csv_file:
        for item in lst:
            csv_file.write(item + '\n')
    return


def get_basic_data(soup):
    """
    :param soup: BeautifulSoup object of a particular mentor's page
    :return: (tuple) - country, rating (out of 5) and reviews (number of reviews)
    """
    div_basic_data = soup.find(DIV, class_=DIV_BASIC_DATA)
    span_basic_data = div_basic_data.find_all(SPAN, class_=SPAN_BASIC_DATA)

    # basic data ~ COUNTRY:
    country = span_basic_data[0].a.span.text

    # basic data ~ RATING and NUM REVIEWS:
    rating_num_reviews = span_basic_data[1].span.text
    pattern = r'([\d.]+)\s+\((\d+)\s+reviews\)'
    match = re.match(pattern, rating_num_reviews)
    if match:
        rating = float(match.group(1))  # Convert the rating to a float
        reviews = int(match.group(2))
    else:
        rating = None
        reviews = None

    return country, rating, reviews


def get_mentor_price(soup):
    # TODO Need to discuss with Ty on monday
    return


def get_mentor_skills(soup):
    """Get mentor skills from the mentor's webpage"""
    div_mentor_data = soup.find(DIV, class_=DIV_MENTOR_DATA)
    all_divs = div_mentor_data.find_all(DIV)

    skills = list()
    for div in all_divs:
        if div.h2 is not None and div.h2.text.strip() == 'Skills':
            div_mt6_class = div.find(DIV, class_=MT6)
            if div_mt6_class is not None:
                a_list = div_mt6_class.find_all(A)
                if len(a_list) > 0:
                    skills = [a_item.text.strip() for a_item in a_list]

    return skills


def get_mentor_name(soup):
    """Get mentor name from the mentor's webpage"""
    h1_element = soup.find('h1', class_=INFO_H1)
    name = h1_element.text.strip()
    return name


def get_mentor_data(mentor_page_url):
    """
    :param mentor_page_url:
    :return: dict of the data gathered from the mentor's page
    """
    mentor_data = dict()
    soup = get_response_then_get_soup(mentor_page_url)

    # get mentor name
    mentor_data['name'] = get_mentor_name(soup)

    # get mentor skills / category
    mentor_data['skills'] = get_mentor_skills(soup)
    mentor_data['category'] = None  # will fill in this data in post-collection phase

    # get country, rating and reviews
    country, rating, reviews = get_basic_data(soup)
    mentor_data['country'] = country
    mentor_data['rating'] = rating
    mentor_data['reviews'] = reviews

    # get PRICE!
    price = get_mentor_price(soup)
    mentor_data['price'] = price

    return mentor_data


def scrape_all_mentor_data(from_csv=True):
    """
    This function collects all mentor data from each mentor homepage on mentorcruise.com
    :param from_csv: (bool) This boolean will tell the function whether to get the mentor url list by scraping the cite,
    or if it can collect the list from a pre-scraped csv file.
    :return df: (pd.DataFrame) All the data (Name, Skills, Category, Country, Rating, # Reviews, Price)
    """
    if not from_csv:
        urls = get_all_mentor_urls(MENTOR_SEARCH_URL)
    else:
        urls = list()
        with open('mentor_cruise/mentor_urls_mentor_cruise.csv', 'r') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row:
                    url = row[0]
                    urls.append(url)

    data_list = list()
    for url in urls:
        data_dict = get_mentor_data(url)
        data_list.append(data_dict)

    df = pd.DataFrame(data_list)
    df.to_csv('mentor_cruise/mentor_cruise_data.csv')
    return df


if __name__ == '__main__':
    # Example mentor to scrape:
    # data = get_mentor_data('https://mentorcruise.com/mentor/LloydJacob/')

    scrape_all_mentor_data(from_csv=True)

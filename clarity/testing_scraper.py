# from multiprocessing import Pool
# import asyncio
# from playwright.async_api import async_playwright
# import json
import concurrent.futures
from playwright.sync_api import sync_playwright
import re
import time
import logging

USERNAME = 'brd-customer-hl_ba3e6f4f-zone-clarity_scraper'
PASSWORD = 'ihmvjo2tgn2r'
AUTH_CODE = f"{USERNAME}:{PASSWORD}"
PORT = '9222'
BROWSER_URL = f'https://{AUTH_CODE}@brd.superproxy.io:{PORT}'


"""
{
  "proxy_http": "127.0.0.1:ENTER PROXY PORT",
  "proxy_https": "127.0.0.1:ENTER PROXY PORT",
  "webpage_hashtags": "https://www.instagram.com/explore/tags/",
  "webpage_profile": "https://www.instagram.com/",
  "pic_link_xpath": "//div[@class=\"v1Nh3 kIKUG  _bz0w\"]//a",
  "chromedriver_path": "ENTER ABSOLUTE PATH TO CHROMEDRIVER"
}
PROXIES = {
    'http': parameters["proxy_http"],
    'https': parameters["proxy_https"]}
WEBPAGE_HASHTAGS = parameters['webpage_hashtags']
WEBPAGE_PROFILE = parameters['webpage_profile']
PIC_LINKS_XPATH = parameters['pic_link_xpath']
CHROMEDRIVER_PATH = parameters['chromedriver_path']
"""

USE_STREAM_HANDLER = True

# Create a logger
logger = logging.getLogger("scraper_logger")
logger.setLevel(logging.DEBUG)

log_file = "scraper_logger.log"
file_handler = logging.FileHandler(log_file)

log_format = "%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if USE_STREAM_HANDLER:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# SELECTORS for playwright
TITLE_SELECTOR = 'document.title'
LOCATION_SELECTOR = '.profile .location'
HOURLY_RATE_BOX_SELECTOR = '.hourly-rate-box'
LINKEDIN_SELECTOR = '.external.linkedin.linked.verified'
TWITTER_SELECTOR = '.external.twitter.linked.verified'
URL_ATTRIBUTE ='href'
STAR_SELECTOR = '.stars'
HALF_STAR_SELECTOR = 'i.star.half'
EMPTY_STAR_SELECTOR = 'i.star.empty'
BIO_SELECTOR = 'span.bio-content'

REGEX_MINUTE_RATE_PATTERN = r'\$(\d+(\.\d+)?)'


def scrape_url(url):
    logger.debug(f'scraping {url}')
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context()
        page = context.new_page()

        fails = 0
        while True:  # keep trying to connect to page, 5 times. Otherwise, return None
            try:
                page.goto(url, timeout=120000)
                break
            except:
                fails += 1
                logger.warning(f'{fails=} | Cannot connect to {url}')
            if fails >= 3:
                logger.warning('Failed too many times. Returning all "None"')
                return {'url': None, 'name': None,
                        'location': None, 'price': None,
                        'linkedin_link': None, 'twitter_link': None,
                        'rating': None, 'reviews': None, 'bio': None }

        # get name:
        title = page.evaluate(f"() => {TITLE_SELECTOR}")

        # get location:
        try:
            page.wait_for_selector(LOCATION_SELECTOR, timeout=200000)
            location_element = page.query_selector(LOCATION_SELECTOR)
            location = location_element.text_content().strip()
        except Exception as e:
            location = None

        try:
            page.wait_for_selector(HOURLY_RATE_BOX_SELECTOR, timeout=160000)
            hourly_rate_box_element = page.query_selector(HOURLY_RATE_BOX_SELECTOR)
            rate = hourly_rate_box_element.text_content().strip()
            match = re.search(REGEX_MINUTE_RATE_PATTERN, rate)
            if match:
                price = match.group(1)
            else:
                price = None
        except Exception as e:
            price = None

        # get linkedin url:
        try:
            linkedin_element = page.query_selector(LINKEDIN_SELECTOR)
            linkedin_link = linkedin_element.get_attribute(URL_ATTRIBUTE) if linkedin_element else None
        except Exception as e:
            linkedin_link = None

        # get twitter url:
        try:
            twitter_element = page.query_selector(TWITTER_SELECTOR)
            twitter_link = twitter_element.get_attribute(URL_ATTRIBUTE) if twitter_element else None
        except Exception as e:
            twitter_link = None

        # star count | num reviews:
        try:
            page.wait_for_selector(STAR_SELECTOR, timeout=120000)
            stars_element = page.query_selector(STAR_SELECTOR)
            num_star_reviews = stars_element.text_content().strip()[1:-1]
            try:
                half_stars = len(stars_element.query_selector_all(HALF_STAR_SELECTOR))
            except:
                half_stars = 0
            try:
                empty_stars = len(stars_element.query_selector_all(EMPTY_STAR_SELECTOR))
            except:
                empty_stars = 0
            star_count = 5 - (0.5 * half_stars) - empty_stars
        except:
            star_count = None
            num_star_reviews = None

        # bio:
        try:
            expanded_bio_element = page.query_selector(BIO_SELECTOR)
            expanded_bio_element.click()
            bio = expanded_bio_element.text_content().replace('\n', ' ').strip()
        except:
            bio = None
        # CLOSE BROWSER
        browser.close()

        consultant_data = {
            'url': url,
            'name': title,
            'location': location,
            'price': price,
            'linkedin_link': linkedin_link,
            'twitter_link': twitter_link,
            'rating': star_count,
            'reviews': num_star_reviews,
            'bio': bio
        }
        logger.debug(f'Num Nones: {[value for key, value in consultant_data.items()].count(None)}')
        logger.debug(str(consultant_data))
        return consultant_data


def main():
    urls = eval(open('save_urls.txt', 'r').read())

    all_results = list()
    cache= dict()

    num_topics = len(list(urls.keys()))
    for i, topic in enumerate(reversed(list(urls.keys()))):
        urls_for_topic = urls[topic]
        already_scraped = [url for url in urls_for_topic if url in cache]
        need_to_scrape = [url for url in urls_for_topic if url not in cache]
        logger.debug(f'{topic=} ({i+1} of {num_topics}) | To scrape: {len(need_to_scrape)} | From cache: {len(already_scraped)}')

        for url in already_scraped:
            consultant_data = cache[url]
            consultant_data['category'] = topic
            all_results.append(consultant_data)

        if len(already_scraped) > 0:
            logger.debug('Finished retrieving from cache')

        count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for consultant_data in executor.map(scrape_url, need_to_scrape):
                cache[consultant_data['url']] = consultant_data
                consultant_data['category'] = topic
                all_results.append(consultant_data)
                count += 1
                logger.debug(f'Scraped {count} of {len(need_to_scrape)}')

        with open('data.txt', 'w') as safety_net:
            safety_net.write(str(all_results))

        # Pause for a minute
        time.sleep(60)


if __name__ == '__main__':
    main()

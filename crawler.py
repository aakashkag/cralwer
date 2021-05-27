# -*- coding: utf-8 -*-
import click
import pandas as pd
import requests
import traceback
import urllib
import urllib3
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Comment
import tldextract
import metadata_parser as mdp
from timeit import default_timer as timer
import pathlib
from pathlib import Path
import multiprocessing.pool
import logging
logging.getLogger("tldextract").setLevel(logging.CRITICAL)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
request_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
}
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
chromeOptions = Options()
chromeOptions.headless = True
from trafilatura import extract

#  Change here output directory path
dir_path = str(pathlib.Path().absolute())
output_html_dirpath = dir_path+'/Data/Output/Html/'
output_text_dirpath = dir_path+'/Data/Output/Text/'
output_final_dirpath = dir_path+'/Data/Output/FinalResults/'
Path(output_html_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists
Path(output_text_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists
Path(output_final_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists

class WebsiteCrawler:

    def __init__(self, use_caching, parser, html_downloader_type):
        self.all_results = []
        self.use_caching = use_caching
        self.parser = parser
        self.html_downloader_type = html_downloader_type
        pass

    def prepare_file_name(self, text):
        try:
            if text:
                text = str(text)
                text = text.lower()
                text = text.replace('https://www.','')
                text = text.replace('http://www.','')
                text = text.replace('https://','')
                text = text.replace('http://','')
                text = text.replace('www.','')
                return text
            else:
                return text
        except:
            return text

    def prepare_url(self, url):
        try:
            if not((url.startswith('http://',0,7)) or (url.startswith('https://',0,8))):
                if not(url.startswith('www.',0,4)):
                    url = 'http://www.'+url
                else:
                    url = 'http://'+url
            url_ext = tldextract.extract(url)
            dom_url = url_ext.domain + "." + url_ext.suffix
            dom_url = 'http://www.'+dom_url
            return dom_url
        except:
            traceback.print_exc()
            return url

    def tag_visible(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def extract_domain(self, url):
        try:
            url_ext = tldextract.extract(url)
            dom_url = url_ext.domain + "." + url_ext.suffix
            return dom_url
        except:
            return None

    def BeautifulSoupParser(self, text):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            texts = soup.findAll(text=True)
            visible_texts = filter(self.tag_visible, texts)
            return u" ".join(t.strip() for t in visible_texts)
        except:
            traceback.print_exc()
            return None

    def TrafilaturaParser(self, text):
        try:
            return extract(text)
        except:
            traceback.print_exc()
            return None

    def save_html(self, fpath, text):
        if text:
            file = open(fpath, "w")
            file.write(text)
            file.close()

    def html_parser(self, html):
        if self.parser == 'BeautifulSoup':
            return self.BeautifulSoupParser(html)
        if self.parser == 'trafilatura':
            return self.TrafilaturaParser(html)
        else:
            return 'no parser available'

    def html_downloder(self, url, Crawler_Type):
        try:
            if url:
                if Crawler_Type == 'selenium':
                    browser = webdriver.Chrome(executable_path="./drivers/chromedriver", options=chromeOptions)
                    browser.get(url)
                    html = browser.page_source
                    browser.close()
                    return -1, html
                else:
                    response = requests.get(url, headers=request_headers, verify=False, timeout=15)
                    return response.status_code, response.text
            return None
        except:
            traceback.print_exc()
            return None

    def crawling_controller(self, fpath, url):
        result = {'status_code': '', 'homepage_text': ''}
        status_code = None
        if self.use_caching:
            if Path(fpath).is_file():
                with open(fpath, 'r') as f2:
                    html = f2.read()
            else:
                status_code, html = self.html_downloder(url, self.html_downloader_type)
                self.save_html(fpath, html)
        else:
            status_code, html = self.html_downloder(url, self.html_downloader_type)
            self.save_html(fpath, html)
        if html:
            homepage_text = self.html_parser(html)  # Change parse here
            if status_code:
                result['status_code'] = status_code
            if homepage_text:
                result['homepage_text'] = homepage_text
        return result

    def get_website_info(self, obj):
        try:
            start = timer()
            url = obj['website']
            url = self.prepare_url(url)             # Add protocol if missing
            file_name = self.prepare_file_name(url)  # Use to cache file for reuse
            domain = self.extract_domain(url)
            fpath = output_html_dirpath+file_name+'.html'
            result = self.crawling_controller(fpath, url)
            end = timer()
            total_time =  end - start
            output_result = {
                'domain': domain,
                'url': url,
                'status_code': result['status_code'],
                'homepage_text': result['homepage_text'],
                'parser': self.parser,
                'html_downloader': self.html_downloader_type,
                'time_taken': total_time
            }
            output_df = pd.DataFrame([output_result])
            output_df.to_csv(output_text_dirpath+str(file_name)+'.csv', index=False)
            return output_result
        except:
            return obj
            traceback.print_exc()

@click.command()
@click.option('--nprocesses', default=10, help='Mention number of processes to run in parallel(By default 10 processes)')
@click.option('--input_file', help='Input file name')
@click.option('--output_file', help='Output file name')
@click.option('--website_column', default='website', help='input column name')
@click.option('--use_caching', default=False, help='Should crawler use html cased result')
@click.option('--parser', type=click.Choice(['BeautifulSoup', 'trafilatura']))
@click.option('--html_downloader_type', default='get', type=click.Choice(['get', 'selenium']))

def start_crawler(nprocesses, input_file, output_file, website_column, use_caching, parser, html_downloader_type):
    try:
        start = timer()
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file)
        else:
            raise Exception(':( Please check input file format!')

        df[website_column] = df[website_column].str.strip()
        seeds = df.to_dict('records')
        print(f'Total Input unique seeds:{len(seeds)}')
        print(f'Crawling started! using parser:{parser} and HTML Downloder type:{html_downloader_type}')
        obj = WebsiteCrawler(use_caching, parser, html_downloader_type)
        pool = multiprocessing.pool.ThreadPool(processes=nprocesses)
        return_list = pool.map(obj.get_website_info, seeds, chunksize=1)
        pool.close()
        end = timer()
        print('<=========== Crawling done Now combining result:) ===============>')
        pd.DataFrame(return_list).to_excel(output_final_dirpath+output_file+'.xlsx')
        print('<=================  Everything Done :) ===================>')
        print("Time taken:", end - start)
    except:
        traceback.print_exc()

# Load and Start Crawler
if __name__ == '__main__':
    start_crawler()

# python3 crawler.py --input_file text_input.csv --output_file testoutput --nprocesses 10 --website_column website --parser BeautifulSoup
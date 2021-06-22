# -*- coding: utf-8 -*-
from warnings import simplefilter
simplefilter(action='ignore', category=DeprecationWarning)
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
import re, nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
regex = re.compile('[^a-zA-Z]')
meta_stop = ["com", "home", "inc", "llc", "welcome", "page", "ltd", "en", "www", "wordpress", "org", "click", "logo", "homepage", "amp", "powered"]
stops = set(stopwords.words('english'))
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
from dragnet import extract_content, extract_content_and_comments

#  Change here output directory path
dir_path = str(pathlib.Path().absolute())
output_html_dirpath = dir_path+'/Data/Output/Html/'
output_text_dirpath = dir_path+'/Data/Output/Text/'
output_final_dirpath = dir_path+'/Data/Output/FinalResults/'
Path(output_html_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists
Path(output_text_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists
Path(output_final_dirpath).mkdir(parents=True, exist_ok=True)  # Create dir if not exists

importent_link_footprint_dict = {
    'about_us_link': {
        'text_keywords': [
            'product', 'products', 'product-category', 'our-products', 'our-product', 'our product', 'our products',
            'product-tag'
        ],
        'link_tokens': [
            'allproduct', 'allproducts', 'product', 'products', 'products.html', 'product.html', 'product-category',
            'our-products', 'our-product', 'product-tag'
        ],
        'must_keyword': 'about'
    },
    'service_link': {
        'text_keywords': [
            'services', 'our-services', 'products-and-services', 'our products', 'product-tag'
        ],
        'link_tokens': [
            'service', 'services', 'services.html', 'our-services'
        ],
        'must_keyword': 'service'
    },
    'product_link': {
        'text_keywords': [
            'product', 'products', 'product-category', 'our-products', 'our-product', 'our product', 'our products',
            'product-tag'
        ],
        'link_tokens': [
            'allproduct', 'allproducts', 'product', 'products', 'products.html', 'product.html', 'product-category',
            'our-products', 'our-product', 'product-tag'
        ],
        'must_keyword': 'product'
    },
    'overview_link': {
        'text_keywords': [
            'overview'
        ],
        'link_tokens': [
            'corporate-overview', 'overview', 'overview.html', 'overview.php', 'company-overview'
        ],
        'must_keyword': 'overview'
    }
}

class WebsiteCrawler:

    def __init__(self, website_column, use_caching, parser, html_downloader_type, crawl_important_link, minimum_word_count):
        self.website_column = website_column
        self.all_results = []
        self.use_caching = use_caching
        self.parser = parser
        self.html_downloader_type = html_downloader_type
        self.crawl_important_link = crawl_important_link
        self.minimum_word_count = minimum_word_count
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
                text = text.rstrip('//')
                return text
            else:
                return text
        except:
            return text

    def prepare_url(self, url):
        try:
            url = url.lower()
            if not((url.startswith('http://',0,7)) or (url.startswith('https://',0,8))):
                if not(url.startswith('www.',0,4)):
                    url = 'http://www.'+url
                else:
                    url = 'http://'+url
                url_ext = tldextract.extract(url)
                dom_url = url_ext.domain + "." + url_ext.suffix
                dom_url = 'http://www.'+dom_url
                return dom_url
            else:
                return url
        except:
            traceback.print_exc()
            return url

    def relative_to_absolute(self, url, website):
        try:
            return urljoin(website, url)
        except:
            traceback.print_exc()
            return url

    def get_links(self, html, domain):
        result = {
            'navigation_links': [], 'internal_links': []
        }
        try:
            url = self.prepare_url(domain)
            internal_links_list = []
            processed = []
            # Case 2. Extract all links
            soup = BeautifulSoup(html, 'html.parser')
            for line in soup.find_all('a'):
                link = line.get('href')
                link_text = line.text
                if not link:
                    continue
                else:
                    full_link = link
                    if not (link.startswith('http:') or link.startswith('https:') or link.startswith('www.')):
                        full_link = self.relative_to_absolute(full_link, url)

                    if (full_link not in processed) and ('www' in full_link or 'http' in full_link):
                        processed.append(full_link)

                    if domain != self.extract_domain(full_link) or url.rstrip('//') == full_link.rstrip('//'):  # It's External links
                        print('external urls===>', domain, full_link, self.extract_domain(full_link))
                        full_link = full_link.lower()
                    else:
                        # Link belong to same website
                        if '.' in full_link and (full_link not in internal_links_list):
                            internal_links_list.append({'link': full_link.lower(), 'link_text': link_text.lower()})
            print('internal_links_list-->', internal_links_list)
            result['internal_links'] = internal_links_list
            return result
        except:
            traceback.print_exc()
            return result

    def tag_visible(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def clean_text(self, text):
        try:
            text = str(text)
            if text:
                text = text.lower().strip()
                text = re.sub(r'http\S+', '', text)  # remove hyper links
                text = re.sub('[\w\.]+@[\w\.]+',' ', text)  # Remove email address from text
                text = re.sub('\W', ' ', text)
                text = regex.sub(' ', text)
                token_list = []
                for token in text.split():
                    if token not in meta_stop and token not in stops and len(token) > 2:
                        token_list.append(token)
                text = ' '.join(token_list)
        except:
            print(text)
            traceback.print_exc()
        return text

    def text_word_count(self, text):
        try:
            tokens = text.split()
            return len([token for token in tokens if token and len(token) >= 2])
        except:
            return 0

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
            return text, u" ".join(t.strip() for t in visible_texts)
        except:
            traceback.print_exc()
            return None,None

    def TrafilaturaParser(self, text):
        try:
            return text, extract(text)
        except:
            traceback.print_exc()
            return None, None

    def DragnetParser(self, text):
        try:
            return text, extract_content(text)
        except:
            traceback.print_exc()
            return None, None

    def save_html(self, fpath, text):
        try:
            if text:
                file = open(fpath, "w")
                file.write(text)
                file.close()
        except:
            print('Failed to save text')
            traceback.print_exc()

    def html_parser(self, html):
        if self.parser == 'BeautifulSoup':
            return self.BeautifulSoupParser(html)
        elif self.parser == 'trafilatura':
            return self.TrafilaturaParser(html)
        elif self.parser == 'dragnet':
            return self.DragnetParser(html)
        else:
            return 'no parser available', ''

    def html_downloder(self, url, Crawler_Type):
        result = {'response_code': '', 'response_error': '', 'text': '', 'redirect_history': '', 'target_url':''}
        try:
            if url:
                if Crawler_Type == 'selenium':
                    browser = webdriver.Chrome(executable_path="./drivers/chromedriver", options=chromeOptions)
                    browser.get(url)
                    html = browser.page_source
                    browser.close()
                    result['code'] = -1
                    result['text'] = html
                    return result
                else:
                    try:
                        response = requests.get(url, headers=request_headers, verify=False, timeout=15)
                        if response:
                            history = []
                            for resp in response.history:
                                history.append(resp.url)
                            result['target_url'] = response.url
                            result['redirect_history'] = ','.join(history)
                            result['response_code'] = response.status_code
                            result['text'] = response.text
                        response.raise_for_status()
                    except requests.exceptions.HTTPError as errh:
                        error = f'"Http Error:", {errh}'
                        result['response_error'] = error
                    except requests.exceptions.ConnectionError as errc:
                        error = f'"Error Connecting:", {errc}'
                        result['response_error'] = error
                    except requests.exceptions.Timeout as errt:
                        error = f'"Timeout Error:", {errt}'
                        result['response_error'] = error
                    except requests.exceptions.RequestException as err:
                        error = f'"OOps: Something Else:", {err}'
                        result['response_error'] = error
                    return result
            else:
                return result
        except:
            traceback.print_exc()
            return result

    def crawl_internal_links(self, obj):
        try:
            html_downloaded_res = self.html_downloder(obj['link'], self.html_downloader_type)
            html = html_downloaded_res['text']
            original_text, parsed_text = self.html_parser(html)  # Change parse here
            obj['page_text'] = parsed_text
            return obj
        except:
            traceback.print_exc()
            return obj

    def importent_link_identifier(self, links_array, important_link_result):
        try:
            important_link_result_array = []
            for link_obj in links_array:
                for key in importent_link_footprint_dict.keys():
                    # if already not predicted
                    if important_link_result[key]['link_text'] == '':
                        l1 = set(link_obj['link_text'].split())
                        l2 = set(importent_link_footprint_dict[key]['text_keywords'])
                        is_selected_link = False
                        if len(l1.intersection(l2)) > 0:
                            is_selected_link = True
                        if is_selected_link:
                            link_tokens = link_obj['link'].split('/')  # breaks response into words
                            if any(s in importent_link_footprint_dict[key]['link_tokens'] for s in link_tokens) and (importent_link_footprint_dict[key]['must_keyword'] in link_obj['link_text']):
                                important_link_result[key]['link'] = link_obj['link']
                                important_link_result[key]['link_type'] = key
                                important_link_result[key]['link_text'] = link_obj['link_text']
                                important_link_result_array.append({'link': link_obj['link'], 'link_type': key})
                                break
            # Scrape its pages
            if len(important_link_result_array) > 0:
                pool1 = multiprocessing.pool.ThreadPool(processes=4)
                subpages_dict = pool1.map(self.crawl_internal_links, important_link_result_array, chunksize=1)
                pool1.close()
                for res_obj in subpages_dict:
                    important_link_result[res_obj['link_type']]['page_text'] = res_obj['page_text']
            return important_link_result
        except:
            traceback.print_exc()
            return important_link_result

    def crawling_controller(self, fpath, url, domain):
        result = {'status_code': '', 'parsed_text': '', 'original_text': '', 'response_error': '', 'redirect_history': '', 'target_url': '', 'importent_links_onj':'', 'html': None}
        status_code = None
        parsed_text = None
        original_text = None
        response_error = None
        redirect_history = None
        target_url = None
        if self.use_caching:
            if Path(fpath).is_file():
                with open(fpath, 'r') as f2:
                    html = f2.read()
            else:
                html_downloaded_res = self.html_downloder(url, self.html_downloader_type)
                status_code = html_downloaded_res['response_code']
                response_error = html_downloaded_res['response_error']
                html = html_downloaded_res['text']
                redirect_history = html_downloaded_res['redirect_history']
                target_url = html_downloaded_res['target_url']
                self.save_html(fpath, html)
        else:
            html_downloaded_res = self.html_downloder(url, self.html_downloader_type)
            status_code = html_downloaded_res['response_code']
            response_error = html_downloaded_res['response_error']
            html = html_downloaded_res['text']
            redirect_history = html_downloaded_res['redirect_history']
            target_url = html_downloaded_res['target_url']
            #self.save_html(fpath, html)
        if html:
            original_text, parsed_text = self.html_parser(html)  # Change parse here
            result['html'] = html
        if status_code:
            result['status_code'] = status_code
        if response_error:
            result['response_error'] = response_error
        if parsed_text:
            result['parsed_text'] = parsed_text
        if original_text:
            result['original_text'] = original_text
        if redirect_history:
            result['redirect_history'] = redirect_history
        if target_url:
            result['target_url'] = target_url
        return result

    def get_website_info(self, obj):
        try:
            important_link_result = {
                'about_us_link': {'link': None, 'link_text': '', 'page_text': '', 'page_clean_text': ''},
                'service_link': {'link': None, 'link_text': '', 'page_text': '', 'page_clean_text': ''},
                'product_link': {'link': None, 'link_text': '', 'page_text': '', 'page_clean_text': ''},
                'overview_link': {'link': None, 'link_text': '', 'page_text': '', 'page_clean_text': ''},
            }

            start = timer()
            url = obj[self.website_column]
            url = self.prepare_url(url)             # Add protocol if missing
            file_name = self.prepare_file_name(url)  # Use to cache file for reuse
            domain = self.extract_domain(url)
            fpath = output_html_dirpath+file_name+'.html'
            result = self.crawling_controller(fpath, url, domain)
            end = timer()
            total_time = end - start
            output_result = {
                'domain': domain,
                'url': url,
                'status_code': result['status_code'],
                'response_error': result['response_error'],
                'parsed_text': result['parsed_text'],
                'parsed_clean_text': self.clean_text(result['parsed_text']),
                'parsed_text_word_count': self.text_word_count(result['parsed_text']),
                'original_text': result['original_text'],
                'parser': self.parser,
                'html_downloader': self.html_downloader_type,
                'time_taken': total_time,
                'target_url': result['target_url'],
                'redirect_history': result['redirect_history'],
                'next_link': '',
                'next_link_text': '',
                'next_link_clean_text': '',
                'next_link_clean_text_wordcount': 0
            }
            output_result['parsed_clean_text_word_count'] = self.text_word_count(output_result['parsed_clean_text'])

            if self.crawl_important_link and result['html']:
                all_links_obj = self.get_links(result['html'], domain)
                # Get important link
                important_link_result = self.importent_link_identifier(all_links_obj['internal_links'], important_link_result)
                #print('important_link_result===>',important_link_result)
                #print('internal links====>',all_links_obj['internal_links'])
                for key in important_link_result.keys():
                    output_result[key] = important_link_result[key]['link']
                    output_result[key + 'link_text'] = important_link_result[key]['link_text']
                    output_result[key + 'page_text'] = important_link_result[key]['page_text']
                    output_result[key + 'page_clean_text'] = self.clean_text(important_link_result[key]['page_text'])
                    output_result[key + 'page_clean_text_word_count'] = self.text_word_count(output_result[key+'page_clean_text'])
                # Scrap other link pages if word count <30
                processed = []
                for next_link_obj in all_links_obj['internal_links']:
                    if next_link_obj['link'] not in processed:
                        processed.append(next_link_obj['link'])
                        next_link_res = self.crawl_internal_links({'link': next_link_obj['link'], 'page_text': None})
                        if next_link_res['page_text']:
                            next_link_res['next_link_clean_text'] = self.clean_text(next_link_res['page_text'])
                            next_link_res['next_link_clean_text_wordcount'] = self.text_word_count(next_link_res['next_link_clean_text'])
                            if(next_link_res['next_link_clean_text_wordcount'] >= self.minimum_word_count):
                                output_result['next_link'] = next_link_obj['link']
                                output_result['next_link_text'] = next_link_res['page_text']
                                output_result['next_link_clean_text'] = next_link_res['next_link_clean_text']
                                output_result['next_link_clean_text_wordcount'] = next_link_res['next_link_clean_text_wordcount']
                                break
            # Add key and value which provided in input put to output file
            for input_extra_key in obj.keys():
                if input_extra_key not in output_result.keys():
                    output_result[input_extra_key] = obj[input_extra_key]
            output_df = pd.DataFrame([output_result])
            output_df.to_csv(output_text_dirpath+str(file_name)+'.csv', index=False)
            return output_result
        except:
            traceback.print_exc()
            return obj

@click.command()
@click.option('--nprocesses', default=10, help='Mention number of processes to run in parallel(By default 10 processes)')
@click.option('--input_file', help='Input file name')
@click.option('--output_file', help='Output file name')
@click.option('--website_column', default='website', help='input column name')
@click.option('--use_caching', default=False, help='Should crawler use html cased result')
@click.option('--crawl_important_link', default=True, help='False if dont want to crawl important link also')
@click.option('--parser', type=click.Choice(['BeautifulSoup', 'trafilatura', 'dragnet']))
@click.option('--html_downloader_type', default='get', type=click.Choice(['get', 'selenium']))
@click.option('--crawl_first_n_website', default=-1)
@click.option('--minimum_word_count', default=30)

def start_crawler(nprocesses, input_file, output_file, website_column, use_caching, crawl_important_link, parser, html_downloader_type, crawl_first_n_website, minimum_word_count):
    try:
        start = timer()
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file)
        else:
            raise Exception(':( Please check input file format!')
        if crawl_first_n_website>-1:
            df = df[0:crawl_first_n_website]
        df[website_column] = df[website_column].str.strip()
        #df = df[df[website_column] == 'https://www.palmolive.co.uk/']
        seeds = df.to_dict('records')
        print(f'Total Input unique seeds:{len(seeds)}')
        #print('seeds==>',seeds)
        print(f'Crawling started! using parser:{parser} and HTML Downloder type:{html_downloader_type}')
        obj = WebsiteCrawler(website_column, use_caching, parser, html_downloader_type, crawl_important_link, minimum_word_count)
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

#  python3 crawler.py --input_file training_dataset_v2.2_trafilatura_LessText_input.csv --output_file training_dataset_v2.2_trafilatura_LessText_output --nprocesses 10 --website_column target_url_old --parser trafilatura --crawl_first_n_website 20
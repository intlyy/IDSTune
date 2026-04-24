import argparse
import hashlib
import json
import os
import pandas as pd
from googleapiclient.discovery import build
from urllib.request import urlopen
from web_util import extract_text
import configparser

config = configparser.ConfigParser()
config.read('../config.ini')

def google_query(query_one, api_key, cse_id):
    """ Uses specified search engine to query_one, returns results. 
    
    Returns:
        A list of search result items.
    """
    query_service = build(
        "customsearch", "v1", developerKey=api_key)
    all_results = []
    for start in range(1, 100, 10):
        print(f'Retrieving results starting from index {start}')
        query_results = query_service.cse().list(
            q=query_one, cx=cse_id, start=start, lr='lang_en', 
            dateRestrict='y1').execute()
        all_results += query_results['items']
    return all_results

def get_web_text(url):
    """ Extract text passages from given URL body. 
    
    Returns:
        Lines from Web site or None if not retrievable.
    """
    try:
        html_src = urlopen(url, timeout=5).read()
        print(f'Retrieved url {url}')
        return extract_text(html_src)
    except:
        return []

def can_parse(result):
    """ Returns true iff the search result can be used. """
    return True if not '.pdf' in result['link'] else False


def _resolve_cache_file(keyword, line_limit, cache_dir):
    """Build a deterministic cache file path for a keyword and line limit."""
    cache_key = f'{keyword}|{line_limit}'
    file_name = hashlib.md5(cache_key.encode('utf-8')).hexdigest() + '.json'
    return os.path.join(cache_dir, file_name)


def _load_cached_lines(cache_file):
    """Load cached lines if cache file exists and has valid format."""
    if not os.path.exists(cache_file):
        return None
    try:
        with open(cache_file, 'r', encoding='utf-8') as fp:
            cached = json.load(fp)
        if isinstance(cached, list):
            print(f'Cache hit: {cache_file}')
            return cached
    except Exception as err:
        print(f'Failed to load cache {cache_file}: {err}')
    return None


def _save_cached_lines(cache_file, lines):
    """Persist lines to cache file."""
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'w', encoding='utf-8') as fp:
            json.dump(lines, fp, ensure_ascii=False)
        print(f'Cache saved: {cache_file}')
    except Exception as err:
        print(f'Failed to save cache {cache_file}: {err}')

def search_lines(keyword, line_limit, api_key=None, cse_id=None):
    """Search pages via Google CSE and return extracted text lines.

    Args:
        keyword: Search keyword or query string.
        line_limit: Maximum number of lines to return (int).
        api_key: Optional Google API key. Falls back to env var GOOGLE_API_KEY.
        cse_id: Optional Programmable Search Engine ID. Falls back to env var GOOGLE_CSE_ID.

    Returns:
        A list of strings (extracted text lines), capped by line_limit.
    """
    api_key = api_key or config['configuration recommender']['google_api_key']
    cse_id = cse_id or config['configuration recommender']['google_cse_id']
    cache_dir = config['configuration recommender'].get('cache_dir', '').strip()
    line_limit = int(line_limit)

    if cache_dir:
        cache_file = _resolve_cache_file(keyword, line_limit, cache_dir)
        cached_lines = _load_cached_lines(cache_file)
        if cached_lines is not None:
            return cached_lines[:line_limit]
    else:
        cache_file = None

    if not api_key or not cse_id:
        raise ValueError('Provide api_key and cse_id or set env GOOGLE_API_KEY and GOOGLE_CSE_ID')

    items = google_query(keyword, api_key, cse_id)
    results = []
    for docid, result in enumerate(items):
        url = result.get('link')
        if not url:
            continue
        print(url)
        if can_parse(result):
            print('Processing document')
            lines = get_web_text(url)
            for line in lines:
                results.append(line)
                if len(results) >= line_limit:
                    if cache_file:
                        _save_cached_lines(cache_file, results)
                    return results
        else:
            print('Did not process document')

    if cache_file:
        _save_cached_lines(cache_file, results)
    return results


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Retrieve results of Google query_one')
    parser.add_argument('query_one', type=str, help='Specify Google search query_one')
    parser.add_argument('key', type=str, help='Specify the Google API key')
    parser.add_argument('cse', type=str, help='Specify SE ID (https://programmablesearchengine.google.com/)')
    parser.add_argument('out_path', type=str, help='Specify path to output file')
    args = parser.parse_args()
    print(args)

    # Write Google query_one results into file
    items = google_query(args.query_one, args.key, args.cse)
    rows = []
    for docid, result in enumerate(items):
        url = result['link']
        print(url)
        if can_parse(result):
            print('Processing document')
            lines = get_web_text(url)
            for line in lines:
                rows.append([docid, line])
        else:
            print('Did not process document')
    data = pd.DataFrame(rows, columns=['filenr', 'sentence'])
    data.to_csv(args.out_path, index=False)


if __name__ == '__main__':
    main()

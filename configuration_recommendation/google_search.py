import argparse
import os
import pandas as pd
from googleapiclient.discovery import build
from urllib.request import urlopen
from web_util import extract_text

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
    api_key = api_key or os.getenv('GOOGLE_API_KEY')
    cse_id = cse_id or os.getenv('GOOGLE_CSE_ID')
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
                if len(results) >= int(line_limit):
                    return results
        else:
            print('Did not process document')
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

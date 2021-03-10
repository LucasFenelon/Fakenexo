import requests

def get_pull_url_responses(urls):
    responses_content = []
    for url in urls:
        responses_content.append(requests.get(url, allow_redirects=True).content)
    return responses_content
import requests
from bs4 import BeautifulSoup


def extract_social_links(url: str) -> dict[str, str]:
    try:
        resp = requests.get(url, timeout=10)
    except requests.RequestException:
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    links = {a["href"] for a in soup.find_all("a", href=True)}
    fb = next((h for h in links if "facebook.com" in h), None)
    ig = next((h for h in links if "instagram.com" in h), None)
    return {"facebook_url": fb, "instagram_url": ig}

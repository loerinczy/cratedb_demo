import requests


class HackerNewsAPI:
    _base_url: str = "https://hacker-news.firebaseio.com/"
    _version: str = "v0"
    _items: str = "item"
    _users: str = "user"

    def fetch_item(self, id: int):
        url = f"{self._base_url}/{self._version}/{self._items}/{id}.json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_max_item_id(self):
        url = f"{self._base_url}/{self._version}/maxitem.json"
        response = requests.get(url, timeout=10)
        return response.json()

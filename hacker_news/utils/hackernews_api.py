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

    def fetch_stories(self, num_stories: int) -> dict:
        curr_item_id = self.get_max_item_id()
        stories = []
        num_fetched_stories = 0
        iteration = 1
        while True:
            item = self.fetch_item(curr_item_id)
            if iteration % 100 == 0:
                print("\ritem_id: " + str(iteration), end="")
            if item["type"] == "story":
                stories.append(item)
                num_fetched_stories += 1
            if num_fetched_stories == num_stories:
                break
        return stories

    def get_max_item_id(self):
        url = f"{self._base_url}/{self._version}/maxitem.json"
        response = requests.get(url, timeout=10)
        return response.json()

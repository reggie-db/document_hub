from typing import Optional

from common import utils



def get(knowledge_assistant_id: str) -> dict:
    url = utils.api_url(f"/api/2.0/knowledge-assistants/{knowledge_assistant_id}")
    return utils.http_request(url).json()


def get_by_name(name: str) -> Optional[dict]:
    url = utils.api_url("/api/2.0/tiles")
    params = {
        "filter": f"name_contains={name}"
    }
    response = utils.http_request(url, params=params)
    tiles = response.json().get("tiles", [])
    if len(tiles) == 0:
        return None
    elif len(tiles) > 1:
        raise ValueError(f"Multiple tiles found - name:{name} tiles:{tiles}")
    else:
        tile_id = tiles[0].get("tile_id")
        return get(tile_id)


def sync(knowledge_assistant_id: str):
    url = utils.api_url(f"/api/2.0/knowledge-assistants/{knowledge_assistant_id}/sync-knowledge-sources")
    return utils.http_request(url, method="POST").json()

import os, time, requests

class GenieError(RuntimeError):
    pass


def translate(nl_query: str, fqtn: str, data_min, data_max) -> str:
    host     = os.environ["DATABRICKS_HOST"]
    token    = os.environ["DATABRICKS_TOKEN"]
    space_id = os.environ["GENIE_SPACE_ID"]

    # 1. Start a conversation
    url = f"{host}api/2.0/genie/spaces/{space_id}/start-conversation"
    payload = {
        "content": nl_query,
        "hints": {
            "catalog": "main",
            "schemas": ["retail_gold"],
            "data_bounds": {
                "min_date": str(data_min),
                "max_date": str(data_max)
            },
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        raise GenieError(f"Genie start failed: {resp.status_code} {resp.text}")

    body = resp.json()
    conv_id = body.get("conversation_id")
    msg_id  = body.get("message_id")
    if not (conv_id and msg_id):
        raise GenieError(f"Genie start returned no IDs: {body}")

    # 2. Poll for results
    poll_url = f"{host}api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}"
    for _ in range(3):
        poll = requests.get(poll_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if poll.status_code != 200:
            raise GenieError(f"Genie poll failed: {poll.status_code} {poll.text}")
        body = poll.json()
        if body:
            status = body.get("status")
            if 'COMPLETED' in body.get("status"):
                sql_text = body["attachments"][0]['query']['query']
                return sql_text.strip()
                break

        time.sleep(3)

    raise GenieError("Genie did not return SQL in time")

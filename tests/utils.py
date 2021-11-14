import json
import re


def get_update_id_from_output(output):
    if "[" in output:
        update_ids = re.findall(r"{.*?}+", output)
        return [json.loads(x.replace("'", '"'))["updateId"] for x in update_ids]

    update_id = re.search(r"\d+", output)
    if update_id:
        return update_id.group()

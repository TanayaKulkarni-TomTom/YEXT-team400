import json
import requests as r
from definitions import SLACK_WEBHOOK_URL, PIPELINE_NAME


def send_slack_message(action: str, status: str, msg=""):
    """Send slack message to a webhook(aka channel url)

    Args:
        action (str): name of action
        status (str): 'success', 'error' or 'info'
        msg (str): message
    """
    try:
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": PIPELINE_NAME},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Action*: " + action},
                        {"type": "mrkdwn", "text": "*Status*: " + status},
                    ],
                },
            ]
        }

        if msg != "":
            msg_section = {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Message*: " + f"```{msg}```"},
            }

            payload["blocks"].append(msg_section)

        response = r.post(SLACK_WEBHOOK_URL, data=json.dumps(payload))
    except Exception as e:
        print(e)

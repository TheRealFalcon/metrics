import csv
import datetime
import json
import os
import sys
from collections import namedtuple
from itertools import count

import requests

START_DATE = datetime.datetime(2019, 10, 27)
TEAM = [
    "blackboxsw",
    "lucasmoura",
    "OddBloke",
    "paride",
    "powersj",
    "raharper",
    "smoser",
    "TheRealFalcon",
    "holmanb",
    "aciba90",
]
FETCH_DIR = "."
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", None)


def fetch_data():
    params = {
        "per_page": 100,
        "state": "all",
    }
    to_request = [
        ("prs", "https://api.github.com/repos/canonical/cloud-init/pulls"),
        (
            "issues",
            "https://api.github.com/repos/canonical/cloud-init/issues/comments",  # noqa: E501
        ),
    ]
    for name, req in to_request:
        for page in count(1):
            params["page"] = page
            prs_request = requests.get(
                req,
                params=params,
                headers={"Authorization": f"token {GITHUB_TOKEN}"},
            )

            print(prs_request.url)
            prs = prs_request.json()
            if not prs:
                break
            with open(f"{FETCH_DIR}/{name}_{page}.json", "w") as f:
                f.write(json.dumps(prs))


def load_data():
    data = {}
    for name in ["prs", "issues"]:
        data[name] = []
        for page in count(1):
            filename = f"{FETCH_DIR}/{name}_{page}.json"
            try:
                with open(filename) as f:
                    content = json.load(f)
            except FileNotFoundError:
                break
            data[name].extend(content)
    return data


pr_record = namedtuple(
    "pr_record", ["number", "user", "created_at", "merged_at", "closed_at"]
)
issue_record = namedtuple(
    "issue_record",
    [
        "created_at",
    ],
)
week_stat = namedtuple(
    "week_stat",
    [
        "week_start",
        "week_end",
        "prs_opened",
        "prs_opened_community",
        "prs_in_open_state_at_week_end",
        "pr_comments",
    ],
)


def string_to_date(field):
    # Fun fact...strptime doesn't take keyword arguments so you can use
    # a partial here
    return datetime.datetime.strptime(field, "%Y-%m-%dT%H:%M:%SZ")


def date_to_string(dt):
    return datetime.datetime.strftime(dt, "%Y-%m-%d")


def sanitize_pr_data(data):
    records = []
    for item in data["prs"]:
        created_at = string_to_date(item["created_at"])
        merged_at = (
            string_to_date(item["merged_at"]) if item["merged_at"] else None
        )
        closed_at = (
            string_to_date(item["closed_at"]) if item["closed_at"] else None
        )
        records.append(
            pr_record(
                item["number"],
                item["user"]["login"],
                created_at,
                merged_at,
                closed_at,
            )
        )
    return records


def sanitize_issues_data(data):
    records = []
    for item in data["issues"]:
        created_at = string_to_date(item["created_at"])
        records.append(issue_record(created_at))
    return records


def _was_open(pr, end_of_week):
    return (
        pr.created_at < end_of_week
        and (pr.merged_at is None or pr.merged_at > end_of_week)
        and (pr.closed_at is None or pr.closed_at > end_of_week)
    )


def parse_weekly_stats(pr_records, issue_records):
    week = datetime.timedelta(days=7)
    week_start = START_DATE

    weekly_stats = []
    while week_start < datetime.datetime.today():
        end_of_week = week_start + week
        prs_opened = [
            x for x in pr_records if week_start < x.created_at < end_of_week
        ]
        prs_opened_community = [x for x in prs_opened if x.user not in TEAM]
        prs_in_open_state = [
            x for x in pr_records if _was_open(x, end_of_week)
        ]
        comments_this_week = [
            x for x in issue_records if week_start < x.created_at < end_of_week
        ]

        weekly_stats.append(
            week_stat(
                date_to_string(week_start),
                date_to_string(end_of_week),
                len(prs_opened),
                len(prs_opened_community),
                len(prs_in_open_state),
                len(comments_this_week),
            )
        )

        week_start += week
    return weekly_stats


def write_stats(weekly_stats):
    writer = csv.DictWriter(sys.stdout, fieldnames=week_stat._fields)
    writer.writeheader()
    for stats in weekly_stats:
        writer.writerow(stats._asdict())

    # TODO...something like

    # data = [
    #     {
    #         "measurement": "pkg_cloudinit",
    #         "time": "2009-11-10T23:00:00Z",  # example date
    #         "fields": {
    #             "open_prs": ...,
    #             "open_prs_community": ...,
    #             "open_pr_comments": ...,
    #         },
    #     }
    # ]
    # util.influxdb_insert(data)


if __name__ == "__main__":
    if sys.argv[1] == "fetch_data":
        if not GITHUB_TOKEN:
            print("Provide GITHUB_TOKEN env var")
            sys.exit(1)
        fetch_data()
        sys.exit(0)
    data = load_data()
    if sys.argv[1] == "sanitize_pr_data":
        sanitize_pr_data(data)
    elif sys.argv[1] == "sanitize_issue_data":
        sanitize_issues_data(load_data())
    elif sys.argv[1] == "parse_weekly":
        parse_weekly_stats(sanitize_pr_data(data), sanitize_issues_data(data))
    elif sys.argv[1] == "write_stats":
        write_stats(
            parse_weekly_stats(
                sanitize_pr_data(data),
                sanitize_issues_data(data),
            )
        )
    else:
        print("nope")

# encoding=utf8

import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

API_BASEURL = "http://localhost:8000"

ROOT_ID = "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1"

IMPORT_BATCHES = [
    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Товары",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "parentId": None,
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Смартфоны",
                "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            },
            {
                "type": "OFFER",
                "name": "jPhone 13",
                "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "price": 79999,
            },
            {
                "type": "OFFER",
                "name": "Xomiа Readme 10",
                "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "price": 59999,
            },
        ],
        "updateDate": "2022-02-02T12:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Телевизоры",
                "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            },
            {
                "type": "OFFER",
                "name": 'Samson 70" LED UHD Smart',
                "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 32999,
            },
            {
                "type": "OFFER",
                "name": 'Phyllis 50" LED UHD Smarter',
                "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 49999,
            },
        ],
        "updateDate": "2022-02-03T12:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": 'Goldstar 65" LED UHD LOL Very Smart',
                "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 69999,
            }
        ],
        "updateDate": "2022-02-03T15:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": 'Goldstar 65" LED UHD LOL Very Very Smart',
                "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 69999,
            }
        ],
        "updateDate": "2022-02-04T15:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": 'Goldstar 65"',
                "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 50000,
            }
        ],
        "updateDate": "2022-02-05T15:00:00.000Z",
    },
]

WRONG_IMPORTS = [
    {
        "items": [],
        "updateDate": "2022-02-05T15:00:00.000Z",
    },
    {
        "items": 1,
        "updateDate": "2022-02-05T15:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": "Priceless",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "parentId": None,
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Wrong date",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "parentId": None,
            }
        ],
        "updateDate": "2022-02-99T12:00:00.000Z",
    },
    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Wrong uuid",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df",
                "parentId": None,
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z",
    },
]

IMPORT_CHANGE_TYPE = {
    "items": [
        {
            "type": "OFFER",
            "name": "Смартфоны",
            "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "price": 80000,
        },
    ],
    "updateDate": "2022-02-09T12:00:00.000Z",
}

EXPECTED_TREE = {
    "type": "CATEGORY",
    "name": "Товары",
    "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
    "price": 54599,
    "parentId": None,
    "date": "2022-02-05T15:00:00.000Z",
    "children": [
        {
            "type": "CATEGORY",
            "name": "Телевизоры",
            "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "price": 44332,
            "date": "2022-02-05T15:00:00.000Z",
            "children": [
                {
                    "type": "OFFER",
                    "name": 'Samson 70" LED UHD Smart',
                    "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 32999,
                    "date": "2022-02-03T12:00:00.000Z",
                    "children": None,
                },
                {
                    "type": "OFFER",
                    "name": 'Phyllis 50" LED UHD Smarter',
                    "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 49999,
                    "date": "2022-02-03T12:00:00.000Z",
                    "children": None,
                },
                {
                    "type": "OFFER",
                    "name": 'Goldstar 65"',
                    "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 50000,
                    "date": "2022-02-05T15:00:00.000Z",
                    "children": None,
                },
            ],
        },
        {
            "type": "CATEGORY",
            "name": "Смартфоны",
            "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
            "children": [
                {
                    "type": "OFFER",
                    "name": "jPhone 13",
                    "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                    "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                    "price": 79999,
                    "date": "2022-02-02T12:00:00.000Z",
                    "children": None,
                },
                {
                    "type": "OFFER",
                    "name": "Xomiа Readme 10",
                    "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                    "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                    "price": 59999,
                    "date": "2022-02-02T12:00:00.000Z",
                    "children": None,
                },
            ],
        },
    ],
}

EXPECTED_SALES = {
    "items": [
        {
            "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
            "name": 'Goldstar 65"',
            "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
            "type": "OFFER",
            "price": 50000,
            "date": "2022-02-05T15:00:00.000Z",
        }
    ]
}

EXPECTED_STATS = {
    "items": [
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": None,
            "date": "2022-02-01T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 55749,
            "date": "2022-02-03T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-03T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-04T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 54599,
            "date": "2022-02-05T15:00:00.000Z",
        },
    ]
}

EXPECTED_STATS_INTERVAL = {
    "items": [
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 55749,
            "date": "2022-02-03T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-03T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-04T15:00:00.000Z",
        },
    ]
}

EXPECTED_STATS_END = {
    "items": [
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": None,
            "date": "2022-02-01T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 55749,
            "date": "2022-02-03T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-03T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-04T15:00:00.000Z",
        },
    ]
}

EXPECTED_STATS_START = {
    "items": [
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 55749,
            "date": "2022-02-03T12:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-03T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 58599,
            "date": "2022-02-04T15:00:00.000Z",
        },
        {
            "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "name": "Товары",
            "parentId": None,
            "type": "CATEGORY",
            "price": 54599,
            "date": "2022-02-05T15:00:00.000Z",
        },
    ]
}


def request(path, method="GET", data=None, json_response=False):
    try:
        params = {
            "url": f"{API_BASEURL}{path}",
            "method": method,
            "headers": {},
        }

        if data:
            params["data"] = json.dumps(data, ensure_ascii=False).encode("utf-8")
            params["headers"]["Content-Length"] = len(params["data"])
            params["headers"]["Content-Type"] = "application/json"

        req = urllib.request.Request(**params)

        with urllib.request.urlopen(req) as res:
            res_data = res.read().decode("utf-8")
            if json_response:
                res_data = json.loads(res_data)
            return (res.getcode(), res_data)
    except urllib.error.HTTPError as e:
        return (e.getcode(), None)


def deep_sort_children(node):
    if node.get("children"):
        node["children"].sort(key=lambda x: x["id"])

        for child in node["children"]:
            deep_sort_children(child)


def print_diff(expected, response):
    with open("expected.json", "w") as f:
        json.dump(expected, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    with open("response.json", "w") as f:
        json.dump(response, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    subprocess.run(
        ["git", "--no-pager", "diff", "--no-index", "expected.json", "response.json"]
    )


def test_import():
    test_validate_import()
    for index, batch in enumerate(IMPORT_BATCHES):
        print(f"Importing batch {index}")
        status, _ = request("/imports", method="POST", data=batch)

        assert status == 200, f"Expected HTTP status code 200, got {status}"

    print("Test import passed.")


def test_validate_import():
    for batch in WRONG_IMPORTS:
        status, _ = request("/imports", method="POST", data=batch)
        assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Test validate import passed.")


def test_change_type_import():
    status, _ = request("/imports", method="POST", data=IMPORT_CHANGE_TYPE)
    assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Test change type import passed.")


def test_nodes():
    status, response = request(
        "/nodes/27972567-3d5d-41b1-9947-480745832234", json_response=True
    )
    assert status == 404, f"Expected HTTP status code 404, got {status}"
    print("Test with non-existed uuid passed.")

    status, response = request(
        "/nodes/27972567-3d5d-41b1-9947-48074583223", json_response=True
    )
    assert status == 400, f"Expected HTTP status code 400, got {status}"
    print("Test with wrong uuid passed.")

    status, response = request(f"/nodes/{ROOT_ID}", json_response=True)
    # print(json.dumps(response, indent=2, ensure_ascii=False))

    assert status == 200, f"Expected HTTP status code 200, got {status}"

    deep_sort_children(response)
    deep_sort_children(EXPECTED_TREE)
    if response != EXPECTED_TREE:
        print_diff(EXPECTED_TREE, response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Test nodes passed.")


def test_sales():
    params = urllib.parse.urlencode({"date": "2022-02-99T15:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 400, f"Expected HTTP status code 400, got {status}"
    print("Test salse with wrong date passed.")

    params = urllib.parse.urlencode({"date": "2022-02-05T15:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_SALES:
        print_diff(EXPECTED_SALES, response)
        print("Response sales doesn't match expected sales.")
        sys.exit(1)

    print("Test sales with the last passed.")
    test_sales_not_last_date()

    params = urllib.parse.urlencode({"date": "2021-02-05T15:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    expected_answer = {"items": []}
    if response != expected_answer:
        print_diff(EXPECTED_SALES, response)
        print("Response sales doesn't match expected sales.")
        sys.exit(1)
    print("Test sales passed.")


def test_sales_not_last_date():
    params = urllib.parse.urlencode({"date": "2022-02-04T15:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_SALES:
        print_diff(EXPECTED_SALES, response)
        print("Response sales doesn't match expected sales.")
        sys.exit(1)

    print("Test sales with date earlier than last update passed.")


def test_stats():
    test_stats_interval()
    test_stats_start()
    test_stats_end()
    test_stats_all()
    print("Test stats passed.")


def test_stats_all():
    status, response = request(f"/node/{ROOT_ID}/statistic", json_response=True)

    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_STATS:
        print_diff(EXPECTED_STATS, response)
        print("Response stats doesn't match expected stats.")
        sys.exit(1)
    print("Test stats without date parameters passed.")


def test_stats_interval():
    params = urllib.parse.urlencode(
        {"dateStart": "2022-02-02T12:00:00.000Z", "dateEnd": "2022-02-05T15:00:00.000Z"}
    )
    status, response = request(
        f"/node/{ROOT_ID}/statistic?{params}", json_response=True
    )

    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_STATS_INTERVAL:
        print_diff(EXPECTED_STATS_INTERVAL, response)
        print("Response stats doesn't match expected stats.")
        sys.exit(1)
    print("Test stats with dateStart and dateEnd passed.")


def test_stats_end():
    params = urllib.parse.urlencode({"dateEnd": "2022-02-05T15:00:00.000Z"})
    status, response = request(
        f"/node/{ROOT_ID}/statistic?{params}", json_response=True
    )

    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_STATS_END:
        print_diff(EXPECTED_STATS_END, response)
        print("Response stats doesn't match expected stats.")
        sys.exit(1)
    print("Test stats with only dateEnd passed.")


def test_stats_start():
    params = urllib.parse.urlencode({"dateStart": "2022-02-02T12:00:00.000Z"})
    status, response = request(
        f"/node/{ROOT_ID}/statistic?{params}", json_response=True
    )

    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response != EXPECTED_STATS_START:
        print_diff(EXPECTED_STATS_START, response)
        print("Response stats doesn't match expected stats.")
        sys.exit(1)
    print("Test stats with only dateStart passed.")


def test_delete():
    status, _ = request(f"/delete/{ROOT_ID}", method="DELETE")
    assert status == 200, f"Expected HTTP status code 200, got {status}"

    status, _ = request(f"/nodes/{ROOT_ID}", json_response=True)
    assert status == 404, f"Expected HTTP status code 404, got {status}"

    print("Test delete passed.")


def test_all():
    test_import()
    test_nodes()
    test_sales()
    test_stats()
    test_delete()


def main():
    global API_BASEURL
    test_name = None

    for arg in sys.argv[1:]:
        if re.match(r"^https?://", arg):
            API_BASEURL = arg
        elif test_name is None:
            test_name = arg

    if API_BASEURL.endswith("/"):
        API_BASEURL = API_BASEURL[:-1]

    if test_name is None:
        test_all()
    else:
        test_func = globals().get(f"test_{test_name}")
        if not test_func:
            print(f"Unknown test: {test_name}")
            sys.exit(1)
        test_func()


if __name__ == "__main__":
    main()

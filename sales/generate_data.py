import numpy as np
import pandas as pd
from functools import reduce
from datetime import datetime
from datetime import timedelta
import json


THRESHOLDS = {0: 0.1, 1: 0.2, 2: 0.2, 3: 0.4, 4: 0.8}

DELIVERY_LAT_MIN = 47.024117
DELIVERY_LAT_MAX = 47.382100
DELIVERY_LNG_MIN = 10.955862
DELIVERY_LNG_MAX = 11.747081

FAULTY_LAT_MIN = 48.152094
FAULTY_LAT_MAX = 48.400004
FAULTY_LNG_MIN = 14.129627
FAULTY_LNG_MAX = 14.542045

DELIVERY_PROB = 0.4
FAULTY_PROB = 0.4

EVENT_TIME_DELTA = 365


def random_by_pop(pop: int):
    size = pop // 1000
    values = np.random.normal(size=size) * 0.1
    return values


def generate_purchase_time(df):
    now = datetime.now().timestamp()
    start = (datetime.now() - timedelta(days=EVENT_TIME_DELTA)).timestamp()
    df["purchase_time"] = [
        np.random.randint(low=start, high=now) for _ in range(len(df))
    ]


def choose_review(df, random: list[str], delivery: list[str], faulty: list[str]) -> str:
    lng = df["lng"]
    lat = df["lat"]
    if (
        DELIVERY_LNG_MIN < lng
        and DELIVERY_LNG_MAX > lng
        and DELIVERY_LAT_MIN < lat
        and DELIVERY_LAT_MAX > lat
    ):
        if np.random.uniform() < DELIVERY_PROB:
            review = delivery.pop()
        else:
            review = random.pop()
    elif (
        FAULTY_LNG_MIN < lng
        and FAULTY_LNG_MAX > lng
        and FAULTY_LAT_MIN < lat
        and FAULTY_LAT_MAX > lat
    ):
        if np.random.uniform() < FAULTY_PROB:
            review = faulty.pop()
        else:
            review = random.pop()
    else:
        review = random.pop()

    return review


def filter_outliers(df: pd.DataFrame, lat: float, lng: float, rad: float):
    df = df[
        ((df["lat"] > (lat - rad)) & (df["lat"] < (lat + rad)))
        & ((df["lng"] > (lng - rad)) & (df["lng"] < (lng + rad)))
    ]
    return df


def flatten(input: list[list[str]]) -> list[str]:
    output = reduce(lambda x, y: x + y, input)
    return output


def generate_reviews(return_data=False):
    df = pd.read_csv("data/random.csv")
    reviews_random = flatten(pd.read_csv("data/reviews_random.csv").values.tolist())
    reviews_delivery = flatten(pd.read_csv("data/reviews_delivery.csv").values.tolist())
    reviews_faulty = flatten(pd.read_csv("data/reviews_faulty.csv").values.tolist())
    delivery_common = set(reviews_delivery).intersection(set(reviews_random))
    faulty_common = set(reviews_faulty).intersection(set(reviews_random))
    reviews_delivery = [
        review for review in reviews_delivery if review not in delivery_common
    ]
    reviews_faulty = [
        review for review in reviews_faulty if review not in faulty_common
    ]
    reviews_delivery.reverse()
    reviews_faulty.reverse()
    apply = lambda df: choose_review(
        df, reviews_random, reviews_delivery, reviews_faulty
    )
    df["review"] = df.apply(apply, axis=1)
    # generate_event_time(df)
    if return_data:
        return df
    df.to_csv("data/random_with_reviews.csv", index=False)


def generate_random():
    df = pd.read_csv("data/austria_cities.csv")
    cities = set(df["city_ascii"].values)
    coords_list = []
    for city in cities:
        df_city = df[df["city_ascii"] == city]
        lat = df_city["lat"].values[0]
        lng = df_city["lng"].values[0]
        pop = df_city["population"].values[0]
        # filter out some small cities
        if pop < 50000:
            threshold = THRESHOLDS[pop // 10000]
            if threshold < np.random.uniform(0, 1):
                continue
        lats = random_by_pop(pop) + lat
        lngs = random_by_pop(pop) + lng
        coords = pd.DataFrame({"lat": lats, "lng": lngs})
        rad = pop / 500_000
        coords = filter_outliers(coords, lat, lng, rad)
        coords_list.append(coords)
    # lats = np.random.uniform(-1, 1, 1000) * 0.1 + lat
    # lngs = np.random.uniform(-1, 1, 1000) * 0.1 + lng
    output = pd.concat(coords_list)
    print("length of table: ", str(len(output)))
    output.to_csv("data/random.csv", index=False)


def generate_sales_with_review():
    data = []
    with open("data/sales.json") as fp:
        for s in fp.readlines():
            curr_entry = json.loads(s)
            del curr_entry["geo"]
            data.append(curr_entry)

    np.random.shuffle(data)

    sales_with_review = pd.DataFrame(data)
    sales_with_review.drop(["purchase_time"], inplace=True)
    sales_with_review["review"] = [None for _ in range(len(sales_with_review))]
    sales_with_review["review_date"] = [None for _ in range(len(sales_with_review))]
    generate_purchase_time(sales_with_review)
    reviews = generate_reviews(return_data=True)
    for idx, review in enumerate(reviews.itertuples()):
        sales_with_review.loc[idx, "longitude"] = review.lng
        sales_with_review.loc[idx, "latitude"] = review.lat
        sales_with_review.loc[idx, "review"] = review.review
        sales_with_review.loc[idx, "review_date"] = sales_with_review.iloc[idx][
            "purchase_date"
        ] + np.random.randint(low=5000, high=360000)
    print(sales_with_review.columns)
    purchases = sales_with_review.copy()
    purchases.drop(["review", "review_date"], inplace=True)
    reviews = sales_with_review[sales_with_review["review"] is not None]
    reviews.drop(
        [
            "platform",
            "sales",
            "purchase_date",
            "items",
            "traffic_source",
            "longitude",
            "latitude",
        ],
        inplace=True,
    )
    sales_with_review.to_csv("data/sales_with_reviews.csv", index=False)
    purchases.to_csv("data/purchases.csv", index=False)
    reviews.to_csv("data/reviews.csv", index=False)


generate_sales_with_review()

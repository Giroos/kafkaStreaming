from datetime import datetime
from pyspark import RDD
from pyspark.streaming.dstream import TransformedDStream
from functools import reduce

system_start_time = datetime.now()


def sort_events(flight_events_rdd=RDD) -> RDD:
    flight_events_rdd. \
        sortBy(lambda e: e['time']). \
        map(lambda e: (e['plane_code'], e))


def create_combiner(e):
    return {
        'time': datetime.now(),
        'plane_code': '2124',
        'last_events': e,
        'handled_events': [e],
        'nav_x_hist': [],
        'nav_y_hist': [],
        'nav_z_hist': [],
        'last5Samples': dict({}),
        'counter': 0,
        'nav_total_error': 0,
        'nav_max_error_10sec': 0
    }


def handle_event(combiner, e):
    return combiner


def merge_combiners(comb1, comb2):
    return comb1


def merge_states(combs: list):
    return reduce(merge_combiners, combs)


def add_features(key_with_combiner):
    (key, combiner) = key_with_combiner
    current_time = datetime.now()
    handled_events = combiner['handled_events']
    if current_time - system_start_time >= 30000:
        max_error = max(handled_events, key=lambda e: e['nav_total_error'], default=0)
        for e in handled_events:
            e['nav_max_error_10sec'] = max_error
    return handled_events


def handle(flight_events_stream=TransformedDStream) -> TransformedDStream:
    nav_errors_stream = flight_events_stream. \
        filter(lambda e: True). \
        transform(sort_events). \
        combineByKey(create_combiner, handle_event, merge_combiners)

    return nav_errors_stream. \
        updateStateByKey(merge_states). \
        window(60000). \
        flatMap(add_features)

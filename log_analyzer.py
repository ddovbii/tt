#!/usr/bin/python

from collections import Counter
import datetime


def process_log_string(log_string, results=None):
    states = [
        "got message from Queue=0",
        "sends message to Queue=1",
        "got message from Queue=1",
        "sends message to Queue=0"
    ]
    frm = "%H:%M:%S,%f"

    if results is None:
        results = dict()

    try:
        parts = log_string.split(" - ")
        time_str = parts[0].split(" ")[0]
        msg_id = parts[-1].split(", ")[-1].split("=")[-1]
        msg_id = msg_id[:-1]
        flow = " ".join(parts[-1].split(", ")[0].split()[1:])

    except Exception:
        print "Wrong format of log string ", log_string
        raise

    try:
        current_time = datetime.datetime.strptime(time_str, frm)
    except ValueError:
        print "Wrong data format ", time_str
        raise

    if msg_id in results:
        if results[msg_id].get("Failed", False):
            return results

        last_time = results[msg_id]["LastTime"]
        delta = current_time - last_time
        results[msg_id]["Duration"] += delta.microseconds/1000
        results[msg_id]["LastTime"] = last_time
        if results[msg_id]["Duration"] > 100:
            results[msg_id]["Failed"] = True
            return results

        existing_states = results[msg_id]["Flow"]
        existing_states.append(flow)
        compare = lambda x, y: Counter(x) == Counter(y)
        if compare(existing_states, states[:len(existing_states)]):
            results[msg_id]["Flow"] = existing_states
        else:
            results[msg_id]["Failed"] = True
            return results

    else:
        results[msg_id] = {
            "Duration": 0,
            "LastTime": current_time,
            "FirstAlert": current_time,
            "Flow": [flow],
            "Failed": False
        }
        if flow != states[0]:
            results[msg_id]["Failed"] = True
            return results

        return results


if __name__ == '__main__':
    results = dict()
    with open('log.txt') as f:
        for line in f:
            process_log_string(line, results)

    print results

    # After that we can count items using data from
    # ["FirstAlert"] and itertools.groupby

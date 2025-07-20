import time

def check_expiry(data):
    print(f"Checking expiry for... {data} with current time {get_current_time()} and expiry time {data[1]}")
    if data[1] < 0:
        return data[0]
    elif data[1] >= get_current_time():
        return data[0]
    else:
        return None

def get_current_time():
    return time.time() * 1000
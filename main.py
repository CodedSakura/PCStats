import subprocess
import time

import psutil
from pymongo import MongoClient

time_between = 5  # seconds
megabyte = 1024 * 1024  # bytes

client = MongoClient("mongodb://root:root@localhost/", serverSelectionTimeoutMS=1000)
db = client.PCStats
db.main.create_index("timestamp", unique=True)

nvidia_smi_query = "nvidia-smi --query-gpu=index,temperature.gpu,memory.used,memory.total,utilization.gpu,fan.speed " \
                   "--format=csv,noheader,nounits"


class GPU:
    def __init__(self, line):
        values = line.split(", ")
        self.index = int(values[0])
        self.temperature = int(values[1])
        self.ram_usage = int(values[2])
        self.ram_max = int(values[3])
        self.utilization = int(values[4])
        self.fan_percent = int(values[5])

    def __repr__(self):
        return f"GPU(index={self.index},temperature={self.temperature},ram={self.ram_usage}/{self.ram_max}," \
               f"utilization={self.utilization},fan_percent={self.fan_percent})"

    @staticmethod
    def get_all():
        res = subprocess.check_output(nvidia_smi_query, stderr=subprocess.STDOUT, shell=True)
        return [GPU(i) for i in res.decode("ascii").splitlines()]


def store_readings():
    global last_network_stats

    timestamp = int(time.time())

    core_usage_percent = psutil.cpu_percent(interval=None, percpu=True)
    cpu_package_percent = sum(core_usage_percent) / len(core_usage_percent)
    load = psutil.getloadavg()[0]  # 1 min average
    ram_data = psutil.virtual_memory()  # bytes
    swap_data = psutil.swap_memory()  # bytes
    partitions = [
        (p.mountpoint, psutil.disk_usage(p.mountpoint)) for p in psutil.disk_partitions() if p.fstype != 'squashfs']
    network = psutil.net_io_counters()  # bytes, total
    temperatures = psutil.sensors_temperatures()  # ???
    core_temps = {d.label: d.current for d in temperatures['coretemp']}
    gpus = GPU.get_all()
    network_io = {
        "up": (network.bytes_sent - last_network_stats.bytes_sent) // time_between,
        "down": (network.bytes_recv - last_network_stats.bytes_recv) // time_between
    }
    last_network_stats = network

    db.main.insert_one({
        "timestamp": timestamp,
        "cpu_package_percent": cpu_package_percent,
        "cpu_package_temperature": core_temps['Package id 0'],
        "cpu_cores": [{
            "index": core_index,
            "temperature": core_temps[f'Core {core_index}'],
            "usage_percent": core_usage_percent[core_index]
        } for core_index in range(len(core_usage_percent))],
        "ram_usage": ram_data.used // megabyte,
        "ram_max": ram_data.total // megabyte,
        "swap_usage": swap_data.used // megabyte,
        "swap_max": swap_data.total // megabyte,
        "load": load,
        "network_up": network_io['up'],
        "network_down": network_io['down'],
        "gpus": [{
            "index": gpu.index,
            "temperature": gpu.temperature,
            "ram_usage": gpu.ram_usage,
            "ram_max": gpu.ram_max,
            "utilization_percent": gpu.utilization,
            "fan_percent": gpu.fan_percent,
        } for gpu in gpus],
        "partitions": [{
            "path": partition[0],
            "size": partition[1].total // megabyte,
            "used": partition[1].used // megabyte,
        } for partition in partitions],
    })


if __name__ == '__main__':
    psutil.cpu_percent(interval=None)
    last_network_stats = psutil.net_io_counters()
    time.sleep(time_between)
    while True:
        before_call = time.time()
        store_readings()
        time.sleep(time_between - (time.time() - before_call))

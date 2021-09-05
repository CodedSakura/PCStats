import subprocess
import time
import liquidctl
import psutil
from clickhouse_driver import Client

time_between = 5  # seconds
megabyte = 1024 * 1024  # bytes

client = Client(host='localhost')
client.execute("CREATE DATABASE IF NOT EXISTS `PCStats`;")
client.execute('''CREATE TABLE IF NOT EXISTS `PCStats`.`main` (
    `timestamp` DateTime,
    `cpu_package_percent` Decimal(4, 1),
    `cpu_package_temperature` Decimal(4, 1),
    `cpu_cores` Nested (
        `index` UInt16,
        `temperature` Decimal(4, 1),
        `usage_percent` Decimal(4, 1)
    ),
    `ram_usage` UInt32,
    `ram_max` UInt32,
    `swap_usage` UInt32,
    `swap_max` UInt32,
    `load` Decimal(4, 2),
    `network_up` UInt32,
    `network_down` UInt32,
    `gpus` Nested (
        `index` UInt16,
        `temperature` Decimal(4, 1),
        `ram_usage` UInt32,
        `ram_max` UInt32,
        `utilization_percent` Decimal(4, 1),
        `fan_percent` Decimal(4, 1)
    ),
    `partitions` Nested (
        `path` String,
        `size` UInt32,
        `used` UInt32
    ),
    `psu_temperature` Decimal(4, 1),
    `psu_fan_speed` UInt32,
    `psu_total_power` Float32,
    `psu_output` Nested (
        `line` String,
        `voltage` Float32,
        `current` Float32,
        `power` Float32
    )
) ENGINE = MergeTree()
ORDER BY `timestamp`;''')

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
    _partitions = [
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

    cpu_cores = [{
        "index": core_index,
        "temperature": core_temps[f'Core {core_index}'],
        "usage_percent": core_usage_percent[core_index]
    } for core_index in range(len(core_usage_percent))]
    partitions = [{
        "path": partition[0],
        "size": partition[1].total // megabyte,
        "used": partition[1].used // megabyte,
    } for partition in _partitions]

    _psu_output = {}
    for dev in liquidctl.find_liquidctl_devices():
        if dev.description != 'NZXT E650':
            continue
        with dev.connect():
            for key, value, unit in dev.get_status():
                if key == 'Temperature':
                    psu_temperature = value
                elif key == 'Fan speed':
                    psu_fan_speed = value
                elif key.startswith('+'):
                    line = key[:key.rindex(' ')]
                    value_type = key[key.rindex(' ') + 1:]
                    if line not in _psu_output:
                        _psu_output[line] = {'line': line}
                    _psu_output[line][value_type] = value

    psu_output = _psu_output.values()
    psu_total_power = sum(map(lambda x: x['power'], psu_output))

    client.execute("INSERT INTO `PCStats`.`main` VALUES", [(
        timestamp,
        cpu_package_percent,
        core_temps['Package id 0'],
        [cpu['index'] for cpu in cpu_cores],
        [cpu['temperature'] for cpu in cpu_cores],
        [cpu['usage_percent'] for cpu in cpu_cores],
        ram_data.used // megabyte,
        ram_data.total // megabyte,
        swap_data.used // megabyte,
        swap_data.total // megabyte,
        load,
        network_io['up'],
        network_io['down'],
        [gpu.index for gpu in gpus],
        [gpu.temperature for gpu in gpus],
        [gpu.ram_usage for gpu in gpus],
        [gpu.ram_max for gpu in gpus],
        [gpu.utilization for gpu in gpus],
        [gpu.fan_percent for gpu in gpus],
        [partition['path'] for partition in partitions],
        [partition['size'] for partition in partitions],
        [partition['used'] for partition in partitions],
        psu_temperature,
        psu_fan_speed,
        psu_total_power,
        [v['line'] for v in psu_output],
        [v['voltage'] for v in psu_output],
        [v['current'] for v in psu_output],
        [v['power'] for v in psu_output],
    )], types_check=True)


if __name__ == '__main__':
    psutil.cpu_percent(interval=None)
    last_network_stats = psutil.net_io_counters()
    time.sleep(time_between)
    while True:
        before_call = time.time()
        store_readings()
        time.sleep(time_between - (time.time() - before_call))

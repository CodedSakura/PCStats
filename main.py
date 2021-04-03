import time
import pymysql.cursors
from pymysql.constants import CLIENT
import psutil
import subprocess

time_between = 5  # seconds
megabyte = 1024 * 1024  # bytes

# must be set up prior, tables will be created automatically
db_credentials = {
    "host": "localhost",
    "username": "PCStats",
    "password": "PCStatsP4ss",
    "database": "PCStats",
}
connection = pymysql.connect(
    host=db_credentials["host"],
    user=db_credentials["username"],
    password=db_credentials["password"],
    database=db_credentials["database"],
    cursorclass=pymysql.cursors.DictCursor,
    client_flag=CLIENT.MULTI_STATEMENTS
)

# language=MySQL
tables_def = """CREATE TABLE `data_store` (
    `timestamp` BIGINT NOT NULL PRIMARY KEY UNIQUE,  # unix timestamp
    `cpu_package_percent` TINYINT UNSIGNED, # percentage 0-100
    `cpu_package_temperature` DECIMAL(4, 1),   # -999.9 - 999.9 °C
    `ram_usage` INT UNSIGNED,   # mb
    `ram_max` INT UNSIGNED,     # mb
    `swap_usage` INT UNSIGNED,  # mb
    `swap_max` INT UNSIGNED,    # mb
    `load` DECIMAL(4, 2),  # 0 - 99.99
    `network_up` INT UNSIGNED,  # bytes
    `network_down` INT UNSIGNED # bytes
);

CREATE TABLE `cpu_cores` (
    `timestamp` BIGINT NOT NULL, # unix timestamp
    `index` TINYINT UNSIGNED NOT NULL,
    `temperature` DECIMAL(4, 1),   # -999.9 - 999.9 °C
    `usage_percent` TINYINT UNSIGNED,   # percentage 0-100
    PRIMARY KEY (`timestamp`, `index`)
);

CREATE TABLE `gpu` (
    `timestamp` BIGINT NOT NULL, # unix timestamp
    `index` TINYINT UNSIGNED NOT NULL,
    `temperature` DECIMAL(4, 1),   # -999.9 - 999.9 °C
    `ram_usage` INT UNSIGNED,   # mb
    `ram_max` INT UNSIGNED,     # mb
    `utilization_percent` TINYINT UNSIGNED, # percentage 0-100
    `fan_percent` TINYINT UNSIGNED, # percentage 0-100
    PRIMARY KEY (`timestamp`, `index`)
);

CREATE TABLE `partitions` (
    `timestamp` BIGINT NOT NULL, # unix timestamp
    `path` VARCHAR(64) NOT NULL,
    `size` INT UNSIGNED,    # mb
    `used` INT UNSIGNED,    # mb 
    PRIMARY KEY (`timestamp`, `path`)
);"""
table_insert_queries = {
    'data_store':
    # language=MySQL
        """INSERT INTO data_store (`timestamp`, `cpu_package_percent`, `cpu_package_temperature`, `ram_usage`, 
        `ram_max`, `swap_usage`, `swap_max`, `load`, `network_up`, `network_down`) VALUES (%s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s);""",
    'cpu_cores':
    # language=MySQL
        """INSERT INTO cpu_cores (`timestamp`, `index`, `temperature`, `usage_percent`) VALUES (%s, %s, %s, %s);""",
    'gpu':
    # language=MySQL
        """INSERT INTO gpu (`timestamp`, `index`, `temperature`, `ram_usage`, `ram_max`, `utilization_percent`, 
        `fan_percent`) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
    'partitions':
    # language=MySQL
        """INSERT INTO partitions (`timestamp`, `path`, `size`, `used`) VALUES (%s, %s, %s, %s)""",
}
tables_list = {'data_store', 'cpu_cores', 'gpu', 'partitions'}

nvidia_smi_query = "nvidia-smi --query-gpu=index,temperature.gpu,memory.used,memory.total,utilization.gpu,fan.speed " \
                   "--format=csv,noheader,nounits"


def init_db():
    print("Initializing database")
    needs_creation = False
    with connection:
        with connection.cursor() as cursor:
            # language=MySQL
            cursor.execute("SHOW TABLES")
            res = cursor.fetchall()
            tables = {list(i.values())[0] for i in res}
            if len(res) == 0 or not any(x in tables for x in tables_list):
                needs_creation = True
            elif not all(x in tables for x in tables_list):
                print("partial table structure!")
                exit(-1)

        if needs_creation:
            print("Creating tables")
            with connection.cursor() as cursor:
                cursor.execute(tables_def)
            connection.commit()


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

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(table_insert_queries['data_store'],
                           (timestamp, cpu_package_percent, core_temps['Package id 0'], ram_data.used // megabyte,
                            ram_data.total // megabyte, swap_data.used // megabyte, swap_data.total // megabyte, load,
                            network_io['up'], network_io['down']))

            for core_index in range(len(core_usage_percent)):
                cursor.execute(table_insert_queries['cpu_cores'],
                               (timestamp, core_index, core_temps[f'Core {core_index}'],
                                core_usage_percent[core_index]))

            for gpu in gpus:
                cursor.execute(table_insert_queries['gpu'],
                               (timestamp, gpu.index, gpu.temperature, gpu.ram_usage, gpu.ram_max, gpu.utilization,
                                gpu.fan_percent))

            for partition in partitions:
                cursor.execute(table_insert_queries['partitions'],
                               (timestamp, partition[0], partition[1].total // megabyte, partition[1].used // megabyte))
        connection.commit()


if __name__ == '__main__':
    init_db()
    psutil.cpu_percent(interval=None)
    last_network_stats = psutil.net_io_counters()
    time.sleep(time_between)
    while True:
        before_call = time.time()
        store_readings()
        time.sleep(time_between - (time.time() - before_call))

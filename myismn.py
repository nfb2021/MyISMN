from typing import Any, Optional, TypeVar
import os
import sys

sys.path.append("/home/nbader/Documents/ISMN_data_opener_trial/")
from ismn.interface import ISMN_Interface  # noqa: E402
import json  # noqa: E402
from collections import defaultdict  # noqa: E402
from natsort import natsorted  # noqa: E402
from tqdm import trange  # noqa: E402
import datetime  # noqa: E402
from functools import wraps  # noqa: E402
import time  # noqa: E402
import polars as pl  # noqa: E402
from multiprocessing import Pool  # noqa: E402
from glob import iglob  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from screeninfo import get_monitors  # noqa: E402


def flag_reader(sensor_path):
    return pl.read_csv(
        source=sensor_path,
        has_header=True,
        columns=[3],
        separator=" ",
        dtypes=[pl.Utf8],
        try_parse_dates=True,
        use_pyarrow=True,
    )


def timeit(func):

    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f"\n\n\tFunction {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds\n\n"
        )
        return result

    return timeit_wrapper


class MyDataTypes:
    """Definition of my own data types for variables"""

    IsmnDataBase = TypeVar("IsmnDataBase")  # loaded ISMN database


class Tools:
    """Small collection of tools around the ISMN database"""

    def __init__(self) -> None:
        self.root = os.getcwd()

    def get_monitor_resolution(self) -> dict:
        smallest_height, smallest_width = 1e6, 1e6
        for m in get_monitors():
            if m.height < smallest_height:
                smallest_height = int(m.height)
            if m.width < smallest_width:
                smallest_width = int(m.width)

        return {
            'monitor_height': smallest_height,
            'monitor_width': smallest_width
        }

    def check_database(self, database_path: str) -> bool:
        """Checks if the specified diectory containing the database exists.
        :param database_path: A string containing the path (absolute or relative) \
              to the database
        :type database_path: str
        :return: True if the database exists, False otherwise
        :rtype: bool
        """

        if os.path.isdir(database_path):
            return True

        else:
            return False

    def get_database(self, database_path: str) -> tuple[ISMN_Interface, str]:
        """Loads the ISMN database using the ISMN module from the specified path.
        :param database_path: A string containing the path (absolute or relative) to the database
        :type database_path: str
        :return: An ISMN database object and the name of the database
        :rtype: tuple[ISMN_Interface, str]
        """  # noqa: E501

        __database_name: str = os.path.basename(
            os.path.normpath(database_path))
        __database = ISMN_Interface(__database_name, parallel=True)

        return __database, __database_name

    def __get_networks(self) -> tuple[list, list]:
        __root = os.getcwd()
        __path = os.path.join(__root, self.database_name)
        os.chdir(__path)
        __networks = natsorted([
            x for x in os.listdir(__path)
            if os.path.isdir(x) and x not in ["python_metadata", "json_dicts"]
        ])
        os.chdir(__root)
        return __networks, len(__networks)

    def get_all_sensors(self) -> list:
        return natsorted([
            os.path.normpath(os.path.join(self.root, f))
            for f in iglob(os.path.join(self.database_name, "**", "*"),
                           recursive=True)
            if os.path.isfile(f) and f.endswith(".stm")
        ])

    def get_sensor_geo_info_dict_from_header(self, pth: str) -> dict:
        with open(pth, "r") as ff:
            _as_read = ff.readlines(1)[0]

        temp = [x for x in _as_read.split(' ') if not x == ""]

        def converter(var):
            try:
                return float(var)
            except ValueError:
                return False

        _file_header_vals = [
            converter(x) for x in temp if converter(x) is not False
        ]
        _file_header_keys = [
            'latitude', 'longitude', 'elevation', 'depthfrom', 'depthto'
        ]

        if 'WEGENERNET' not in pth:  # all stations have names of type string, except for WEGENERNET, who choose numbers
            return {
                key: val
                for key, val in zip(_file_header_keys, _file_header_vals)
            }
        else:
            return {
                key: val
                for key, val in zip(_file_header_keys, _file_header_vals[1:])
            }

    def get_path_segments(self, path) -> list:
        segments = []
        pth, last = os.path.split(path)
        segments.append(last)
        while last:
            pth, last = os.path.split(pth)
            segments.append(last)

        return segments[::-1]

    def get_size_in_megabytes(self, var: Any) -> None:
        print(
            f'This variable "{str(var)}" of type {type(var)} occupies {sys.getsizeof(var) / 1e3} MB of memory'
        )

    def get_network_from_filename(self, filename: str) -> str:
        return filename.split("/")[-1].split("_")[0]

    def get_station_from_filename(self, filename: str) -> str:
        return filename.split("/")[-1].split("_")[2]

    def get_sensor_from_filename(self, filename: str) -> str:
        splitter = filename.split("/")[-1].split("_")
        if splitter[-6] == "sm":
            return f"{splitter[-3]}_soil_moisture_{splitter[-5]}_{splitter[-4]}"
        else:
            print("This is not an exclusive soil moisture data set")

    def get_all_numbers(
        self, database: MyDataTypes.IsmnDataBase
    ) -> tuple[int, int, int, dict, dict]:
        """Counts the total number of networks, stations, and sensors in the database \
            and creates a dictionary.
        :param database: An ISMN database object
        :type database: MyDataTypes.IsmnDataBase
        :return: Number of networks, number of stations, number of sensors, \
            a dictionary containing which network contains how many stations and \ 
            a dictionary containing which network and station contains how many sensors.
        :rtype: tuple[int, int, int, dict, dict]
        """

        __network_check = "go"

        network_lst, no_of_networks = self.__get_networks()
        # print(network_lst, no_of_networks)
        no_of_stations: int = 0
        no_of_sensors: int = 0
        stations_dict: dict = {}
        sensors_dict: dict = {}

        for ii in trange(no_of_networks, desc="iterating over networks:"):
            network_name = network_lst[ii]
            try:
                network = database[network_name]
            except KeyError:
                print(
                    f"\n\n\t The network {network_name} was not loaded by the \
                        ISMN module\n\n")
                continue

            # print(network_name, network)

            for s, _ in enumerate(network):
                if ValueError:
                    no_of_stations += 1

                station = network[s]
                for ss, _ in enumerate(station):
                    if ValueError:
                        no_of_sensors += 1

                sensors_dict[f"{network.name}:{station.name}"] = ss + 1

            stations_dict[network.name] = s + 1

        self._func_get_all_numbers_ran = True

        self.no_of_networks = no_of_networks
        self.no_of_stations = no_of_stations
        self.no_of_sensors = no_of_sensors
        self.stations_dict = stations_dict
        self.sensors_dict = sensors_dict

        return (
            self.no_of_networks,
            self.no_of_stations,
            self.no_of_sensors,
            self.stations_dict,
            self.sensors_dict,
        )

    def make_json(
        self,
        data: dict,
        json_name: str,
        where_to_be_saved: Optional[str] = os.getcwd()) -> None:
        """Creates a JSON file with the specified data.
        :param data: The data to be saved in the JSON file
        :type data: dict
        :param json_name: The name of the JSON file
        :type json_name: str
        :param where_to_be_saved: A string containing the path (absolute or relative)\
              to where the JSON is to be saved, by default the current working directory
        :type where_to_be_saved: Optional[str]
        :return: None
        :rtype: None
        """

        with open(os.path.join(where_to_be_saved, json_name),
                  "w") as __outfile:
            json.dump(data, __outfile, indent=2)

        print(f'The JSON file "{json_name}" has been created in the directory \
                "{os.path.join(where_to_be_saved)}".')

    def read_json(self, json_name: str,
                  path: Optional[str] = os.getcwd()) -> Any:
        """Readd the specified JSON file and returns the data.
        :param json_name: The name of the JSON file
        :type json_name: str
        :param path: The path to the JSON file, by default the current working directory
        :type path: Optional[str]
        :return: The read data from the JSON file
        :rtype: Any
        """

        with open(os.path.join(path, json_name)) as __json_file:
            __read_json = json.load(__json_file)

        return __read_json

    def file_exists(self, file_name: str,
                    path: Optional[str] = os.getcwd()) -> bool:
        """Checks if the specified file exists in the specified path.
        :param file_name: The name of the file
        :type file_name: str
        :param path: The path to the file, by default the current working directory
        :type path: Optional[str]
        :return: True if the file exists, False otherwise
        :rtype: bool
        """
        return os.path.isfile(os.path.join(path, file_name))

    def directory_exist_status(
        self, dir_name: str, path: Optional[str] = os.getcwd()) -> bool:
        """Checks if the specified directory exists in the specified path.
        :param dir_name: The name of the directory
        :type dir_name: str
        :param path: The path to the directory, by default the current working directory
        :type path: Optional[str]
        :return: True if the directory exists, False otherwise
        :rtype: bool
        """
        return os.path.isdir(os.path.join(path, dir_name))

    def multi_dict(self, K: int, type: Any) -> defaultdict:
        """Create a multi-dimensional dictionary, based on a default dictionary.
        :param K: The number of dimensions
        :type K: int
        :param type: The type of the item to be contained in the default dictionary, \
            i.e. int, string, list or dict
        :type type: type
        :return: A multi-dimensional default dictionary
        :rtype: defaultdict
        """
        if K == 1:
            return defaultdict(type)
        else:
            return defaultdict(lambda: self.multi_dict(K - 1, type))

    def c_prettyprint(self,
                      dictionary: dict,
                      indent: Optional[int] = 2) -> None:
        """Prints the dictionary in a pretty format.
        :param dictionary: The dictionary to be printed
        :type dictionary: dict
        :param indent: The indentation used to print the dictionary, by default 2
        :type indent: Optional[int]
        :return: None
        :rtype: None"""
        print(json.dumps(dictionary, indent=indent))


class Geography(Tools):
    """Special class for everything Geography"""

    def __init__(self):
        super().__init__()

    def get_country_from_coords(self, latitude: float,
                                longitude: float) -> str:
        """Get a country for input latitude and longitude. \
            Based on https://github.com/che0/countries and https://thematicmapping.org/downloads/world_borders.php

        :param latitude: The latitude of the location
        :type latitude: float
        :param longitude: The longitude of the location
        :type longitude: float
        :return: The country name
        :rtype: str"""

        from CoordPy import countries

        cc = countries.CountryChecker(
            os.path.join("CoordPy", "TM_WORLD_BORDERS",
                         "TM_WORLD_BORDERS-0.3.shp"))

        try:
            return cc.getCountry(countries.Point(latitude, longitude)).iso

        except AttributeError:
            return "None"

    def sort_stations_to_countries(self) -> tuple[dict, dict]:
        if not os.path.isfile(
                os.path.join(self.database_name, "json_dicts",
                             "countries.json")) and not os.path.isfile(
                                 os.path.join(self.database_name, "json_dicts",
                                              "locations.json")):
            print("if")
            locations_dict = {}
            sorted_countries_dict = {}

            for key, item in self.sensors_dict.items():
                _network, _station = key.split(":")[0], key.split(":")[1]
                _no_of_sensors = int(item)

                counter = 0
                _country = []
                while counter < _no_of_sensors:
                    lat = (self.database[_network][_station]
                           [counter].metadata["latitude"].val)
                    long = (self.database[_network][_station]
                            [counter].metadata["longitude"].val)

                    _country.append(self.get_country_from_coords(lat, long))

                    counter += 1

                _country = list(set(_country))
                if len(_country) == 1:
                    if _station not in locations_dict:
                        locations_dict[f"{key}"] = _country[0]

                    try:
                        if _country[0] not in sorted_countries_dict:
                            sorted_countries_dict[_country[0]] = [_network]
                        if (_country[0] in sorted_countries_dict and _network
                                not in sorted_countries_dict[_country[0]]):
                            _temp = list(sorted_countries_dict[_country[0]])
                            # print(_temp)
                            _temp.append(_network)
                            # print(_temp)
                            sorted_countries_dict[_country[0]] = _temp

                    except TypeError:
                        continue
                        if "Unknown" not in sorted_countries_dict:
                            sorted_countries_dict["Unknown"] = [_network]
                        else:
                            sorted_countries_dict["Unknown"] = list(
                                sorted_countries_dict["Unknown"]).append(
                                    _network)

            sorted_countries_dict = dict(sorted(sorted_countries_dict.items()))
            self.countries_dict = sorted_countries_dict
            self.locations_dict = locations_dict

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "locations.json"), "w") as outfile:
                json.dump(locations_dict, outfile)

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "countries.json"), "w") as outfile:
                json.dump(sorted_countries_dict, outfile)

        else:
            print("else")
            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "countries.json")) as json_file:
                self.countries_dict = json.load(json_file)

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "locations.json")) as json_file:
                self.locations_dict = json.load(json_file)

        print("now i return")
        return self.countries_dict, self.locations_dict

    def sort_stations_to_countries2(self) -> tuple[dict, dict]:
        if not os.path.isfile(
                os.path.join(self.database_name, "json_dicts",
                             "countries.json")) and not os.path.isfile(
                                 os.path.join(self.database_name, "json_dicts",
                                              "locations.json")):
            print("if")
            locations_dict = {}
            sorted_countries_dict = {}

            for key, item in self.sensors_dict.items():
                _network, _station = key.split(":")[0], key.split(":")[1]
                _no_of_sensors = int(item)

                counter = 0
                _country = []
                while counter < _no_of_sensors:
                    lat = (self.database[_network][_station]
                           [counter].metadata["latitude"].val)
                    long = (self.database[_network][_station]
                            [counter].metadata["longitude"].val)

                    _country.append(self.get_country_from_coords(lat, long))

                    counter += 1

                _country = list(set(_country))
                if len(_country) == 1:
                    if _station not in locations_dict:
                        locations_dict[f"{key}"] = _country[0]

                    try:
                        if _country[0] not in sorted_countries_dict:
                            sorted_countries_dict[_country[0]] = [_network]
                        if (_country[0] in sorted_countries_dict and _network
                                not in sorted_countries_dict[_country[0]]):
                            _temp = list(sorted_countries_dict[_country[0]])
                            # print(_temp)
                            _temp.append(_network)
                            # print(_temp)
                            sorted_countries_dict[_country[0]] = _temp

                    except TypeError:
                        continue
                        if "Unknown" not in sorted_countries_dict:
                            sorted_countries_dict["Unknown"] = [_network]
                        else:
                            sorted_countries_dict["Unknown"] = list(
                                sorted_countries_dict["Unknown"]).append(
                                    _network)

            sorted_countries_dict = dict(sorted(sorted_countries_dict.items()))
            self.countries_dict = sorted_countries_dict
            self.locations_dict = locations_dict

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "locations.json"), "w") as outfile:
                json.dump(locations_dict, outfile)

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "countries.json"), "w") as outfile:
                json.dump(sorted_countries_dict, outfile)

        else:
            print("else")
            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "countries.json")) as json_file:
                self.countries_dict = json.load(json_file)

            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "locations.json")) as json_file:
                self.locations_dict = json.load(json_file)

        print("now i return")
        return self.countries_dict, self.locations_dict


class DataReader(Tools):

    def __init__(self,
                 database: Any,
                 process_parallel: Optional[bool] = True) -> None:
        super().__init__()

        if self.check_database(database):
            self.database, self.database_name = self.get_database(database)
        else:
            raise ValueError(
                f'The specified database "{database}" does not exist \
                    in the current working directory: "{os.getcwd()}".')

        if not self.directory_exist_status(
                "json_dicts", os.path.join(os.getcwd(), self.database_name)):
            os.mkdir(
                os.path.join(os.getcwd(), self.database_name, "json_dicts"))

        if self.file_exists("numbers.json",
                            os.path.join(os.getcwd(), "json_dicts")):
            self.numbers_dict = self.read_json("numbers.json",
                                               path=os.path.join(
                                                   os.getcwd(), "json_dicts"))
            self.no_of_networks, self.no_of_stations, self.no_of_sensors = (
                self.numbers_dict["Networks"],
                self.numbers_dict["Stations"],
                self.numbers_dict["Sensors"],
            )
        else:
            (
                self.no_of_networks,
                self.no_of_stations,
                self.no_of_sensors,
                self.stations_dict,
                self.sensors_dict,
            ) = self.get_all_numbers(self.database)
            self.numbers_dict = {
                "Networks": self.no_of_networks,
                "Stations": self.no_of_stations,
                "Sensors": self.no_of_sensors,
            }
            self.make_json(
                self.numbers_dict,
                "numbers.json",
                os.path.join(self.database_name, "json_dicts"),
            )

        if self.file_exists("stations.json",
                            os.path.join(os.getcwd(), "json_dicts")):
            self.stations_dict = self.read_json("stations.json",
                                                path=os.path.join(
                                                    os.getcwd(), "json_dicts"))
        elif not self._func_get_all_numbers_ran:
            self.make_json(
                self.stations_dict,
                "stations.json",
                os.path.join(self.database_name, "json_dicts"),
            )
        else:
            (
                self.no_of_networks,
                self.no_of_stations,
                self.no_of_sensors,
                self.stations_dict,
                self.sensors_dict,
            ) = self.get_all_numbers(self.database)
            self.make_json(
                self.stations_dict,
                "stations.json",
                os.path.join(self.database_name, "json_dicts"),
            )

        if self.file_exists("sensors.json",
                            os.path.join(os.getcwd(), "json_dicts")):
            self.stations_dict = self.read_json("sensors.json",
                                                path=os.path.join(
                                                    os.getcwd(), "json_dicts"))
        elif not self._func_get_all_numbers_ran:
            self.make_json(
                self.stations_dict,
                "sensors.json",
                os.path.join(self.database_name, "json_dicts"),
            )
        else:
            (
                self.no_of_networks,
                self.no_of_stations,
                self.no_of_sensors,
                self.stations_dict,
                self.sensors_dict,
            ) = self.get_all_numbers(self.database)
            self.make_json(
                self.sensors_dict,
                "sensors.json",
                os.path.join(self.database_name, "json_dicts"),
            )


class Flags(DataReader):
    """Defines flags used by the ISMN data quality control"""

    def __init__(self,
                 database: Any,
                 process_parallel: Optional[bool] = True) -> None:
        self.available_soil_moisture_flags = [
            "C01",
            "C02",
            "C03",
            "D01",
            "D02",
            "D03",
            "D04",
            "D05",
            "D06",
            "D07",
            "D08",
            "D09",
            "D10",
            "G",
        ]
        self.faulty_soil_moisture_flags = ["M", "OK"]

        super().__init__(database, process_parallel)

    @timeit
    def make_flag_dict(self):
        faulty_flag_file = os.path.join(self.database_name, "faulty_flags.txt")
        if os.path.isfile(faulty_flag_file):
            os.remove(faulty_flag_file)
            with open(faulty_flag_file, "w") as fff:
                fff.write(
                    "flag_string\tfaulty_part\tnetwork\tstation\tsensor\n")

        if self.file_exists(
                os.path.join(self.database_name, "json_dicts",
                             "flag_dict.json")):
            print("flag_dict.json exists")
            with open(
                    os.path.join(self.database_name, "json_dicts",
                                 "flag_dict.json")) as json_file:
                self.flag_dict = json.load(json_file)

        else:
            print("flag_dict.json does not exist")
            self.flag_dict = self.multi_dict(4, dict)
            for network, station, sensor in self.database.collection.iter_sensors(
            ):
                # print(network.name, station.name, sensor.name)

                sensor_flag_dict_entangled = (
                    sensor.data["soil_moisture_flag"].value_counts(
                        ascending=False).to_dict())
                sensor_flag_dict_disentangled = defaultdict(int)

                for key, item in sensor_flag_dict_entangled.items():
                    if ("," not in key and " " not in key
                            and key in self.available_soil_moisture_flags):
                        sensor_flag_dict_disentangled[key] += int(item)

                    elif "," in key and " " not in key:
                        for splitter in key.split(","):
                            if splitter.strip(
                            ) in self.available_soil_moisture_flags:
                                sensor_flag_dict_disentangled[
                                    splitter.strip()] += int(item)

                            elif (splitter.strip()
                                  not in self.faulty_soil_moisture_flags):
                                with open(faulty_flag_file, "a") as fff:
                                    fff.write(
                                        f"{key}\t{splitter}\t{network.name}\t{station.name}\t{sensor.name}\n"
                                    )

                    elif " " in key and "," not in key:
                        for splitter in key.split(" "):
                            if splitter in self.available_soil_moisture_flags:
                                sensor_flag_dict_disentangled[splitter] += int(
                                    item)

                            elif splitter not in self.faulty_soil_moisture_flags:
                                with open(faulty_flag_file, "a") as fff:
                                    fff.write(
                                        f"{key}\t{splitter}\t{network.name}\t{station.name}\t{sensor.name}\n"
                                    )

                    else:
                        with open(faulty_flag_file, "a") as fff:
                            fff.write(
                                f"{key}\t{key}\t{network.name}\t{station.name}\t{sensor.name}\n"
                            )

                sensor_flag_dict_disentangled = dict(
                    sorted(
                        sensor_flag_dict_disentangled.items(),
                        key=lambda item: item[1],
                        reverse=True,
                    ))

                self.flag_dict[network.name][station.name][
                    sensor.name] = sensor_flag_dict_disentangled

            self.make_json(
                dict(self.flag_dict),
                "flag_dict.json",
                os.path.join(self.database_name, "json_dicts"),
            )

        if not os.path.isfile(faulty_flag_file):
            print(
                "\n\n\There were no faulty flags identified in the database\n\n"
            )

    @timeit
    def get_flag_df(self,
                    n_cores: Optional[int] = 8,
                    save_as_csv: Optional[bool] = False) -> pd.DataFrame:
        if self.file_exists(
                os.path.join(self.root, self.database_name, "json_dicts",
                             "flag_df.pkl")):
            print("flag_df.pkl exists")
            self.flag_df = pd.read_pickle(
                os.path.join(self.root, self.database_name, "json_dicts",
                             "flag_df.pkl"))

        else:
            print(os.getcwd())
            print(
                "\nConstructing and subsequently pickling the dataframe. This might take some time, but is only done once."
            )

            self.make_sensor_ids()

            def multi_reader() -> dict:
                pool = Pool(n_cores)  # number of cores you want to use

                sensor_list = self.get_all_sensors()
                all_flags_list = [
                    flags.to_series(0).to_list()
                    for flags in pool.map(flag_reader, sensor_list)
                ]  # creates a list of the loaded df's
                all_flags_lst = []

                for flags, filename in zip(all_flags_list, sensor_list):
                    _temp_dict = {
                        key: 0
                        for key in self.available_soil_moisture_flags
                    }
                    for flag in flags:
                        if "," in flag:
                            split_flag = flag.split(",")
                            for splitter in split_flag:
                                _temp_dict[splitter] += 1
                        else:
                            _temp_dict[flag] += 1

                    # filename = filename.split(self.root)[1][1:]
                    _network = self.get_path_segments(filename)[-3]
                    _station = self.get_path_segments(filename)[-2]
                    _temp_lst = [x for x in _temp_dict.values()]
                    sensor_id = self.sensor_path_to_id_dict[filename]
                    _temp_lst += [_network, _station, sensor_id]
                    all_flags_lst.append(_temp_lst)

                flags_df = pd.DataFrame(all_flags_lst)
                cols = self.available_soil_moisture_flags + [
                    "network", "station", "sensor_id"
                ]  # both are mutable objects, DONT USE _iadd__!!
                flags_df.columns = cols
                flags_df = flags_df.set_index(
                    ["network", "station", "sensor_id"])

                return flags_df

            self.flag_df = multi_reader()
            self.flag_df.to_pickle(
                os.path.join(self.database_name, "json_dicts", "flag_df.pkl"))

        if save_as_csv:
            print('Additionally saving dataframe to "flag_df.csv".')
            self.flag_df.to_csv(
                os.path.join(self.database_name, "json_dicts", "flag_df.csv"))

        return self.flag_df

    @timeit
    def get_flag_normalization_df(self,
                                  save_as_csv: Optional[bool] = False
                                  ) -> pd.DataFrame:
        if self.file_exists(
                os.path.join(self.root, self.database_name, "json_dicts",
                             "flag_normalization_df.pkl")):
            print("flag_normalization_df.pkl exists")
            self.flag_normalization_df = pd.read_pickle(
                os.path.join(
                    self.root,
                    self.database_name,
                    "json_dicts",
                    "flag_normalization_df.pkl",
                ))

        else:
            print(os.getcwd())
            print(
                "\nConstructing and subsequently pickling the dataframe. This might take some time, but is only done once."
            )

            _temp_list = []
            self.make_sensor_ids()
            for idx, pth in self.sensor_id_to_path_dict.items():
                _network_name = self.get_path_segments(pth)[-3]
                _station_name = self.get_path_segments(pth)[-2]

                _length_timeseries = np.sum(self.flag_df.loc[_network_name,
                                                             _station_name,
                                                             idx].to_numpy())
                # _network_name = self.sensor_df.loc[idx]['network']
                # _station_name = self.sensor_df.loc[idx]['station']
                _sensors_per_station = int(
                    self.sensors_dict[f"{_network_name}:{_station_name}"])
                _stations_per_network = int(self.stations_dict[_network_name])

                _temp_list.append([
                    _network_name,
                    _station_name,
                    idx,
                    _length_timeseries,
                    _sensors_per_station,
                    _stations_per_network,
                    self.no_of_networks,
                    1 / np.product([
                        _length_timeseries,
                        _sensors_per_station,
                        _stations_per_network,
                        # self.no_of_networks,
                    ]),
                ])

            self.flag_normalization_df = pd.DataFrame(_temp_list)
            cols = [
                "network",
                "station",
                "sensor_id",
                "length_timeseries",
                "sensors_per_station",
                "stations_per_network",
                "no_of_networks",
                "norm_factor",
            ]
            self.flag_normalization_df.columns = cols
            self.flag_normalization_df = self.flag_normalization_df.set_index(
                ["network", "station", "sensor_id"])

            self.flag_normalization_df.to_pickle(
                os.path.join(self.database_name, "json_dicts",
                             "flag_normalization_df.pkl"))

        if save_as_csv:
            print(
                'Additionally saving dataframe to "flag_normalization_df.csv".'
            )
            self.flag_normalization_df.to_csv(
                os.path.join(self.database_name, "json_dicts",
                             "flag_normalization_df.csv"))

        return self.flag_normalization_df

    @timeit
    def normalize_flag_df(self,
                          save_as_csv: Optional[bool] = False) -> pd.DataFrame:
        self.get_flag_df(save_as_csv=save_as_csv)
        self.get_flag_normalization_df(save_as_csv=save_as_csv)
        self.make_sensor_ids()
        normalizer_df = np.array([
            self.flag_normalization_df["norm_factor"]
            for flag_tag in self.available_soil_moisture_flags
        ])
        normalizer_df = pd.DataFrame(normalizer_df).T
        cols = self.available_soil_moisture_flags + [
            "sensor_id", "network", "station"
        ]  # both are mutable objects, DONT USE _iadd__!!
        normalizer_df["sensor_id"] = list(self.sensor_id_to_path_dict.keys())
        normalizer_df["network"] = [
            self.get_path_segments(self.sensor_id_to_path_dict[sensor_id])[-3]
            for sensor_id in list(normalizer_df["sensor_id"])
        ]
        normalizer_df["station"] = [
            self.get_path_segments(self.sensor_id_to_path_dict[sensor_id])[-2]
            for sensor_id in list(normalizer_df["sensor_id"])
        ]

        normalizer_df.columns = cols
        normalizer_df = normalizer_df.set_index(
            ["network", "station", "sensor_id"])

        self.normalized_flag_df = self.flag_df.multiply(normalizer_df)

        self.normalized_flag_df.to_pickle(
            os.path.join(self.database_name, "json_dicts",
                         "normalized_flag_df.pkl"))

        if save_as_csv:
            print('Additionally saving dataframe to "normalized_flag_df.csv".')
            self.flag_normalization_df.to_csv(
                os.path.join(self.database_name, "json_dicts",
                             "normalized_flag_df.csv"))

        return self.normalized_flag_df

    def make_sensor_ids(self) -> tuple[dict, dict, pd.DataFrame]:
        _sensor_ids_arr = list(np.zeros(len(self.get_all_sensors())))
        _network_counter, _station_counter, _sensor_counter = [], [], []
        sensor_filename_segments = [
            "network",
            "station",
            "variablename",
            "latitude",
            "longitude",
            "elevation",
            "depthfrom",
            "depthto",
            "sensorname",
            "startdate",
            "enddate",
            "path",
            "sensor_id",
        ]
        for p, pth in enumerate(self.get_all_sensors()):
            segments = self.get_path_segments(pth)
            _network, _station = (
                segments[-3],
                segments[-2],
            )

            _sensor = segments[-1]
            _sensor_enddate = _sensor.split("_")[-1].split(".stm")[0]
            _sensor_startdate = _sensor.split("_")[-2]
            _sensor_name = _sensor.split("_")[-3]
            _sensor_dephto = _sensor.split("_")[-4]
            _sensor_dephfrom = _sensor.split("_")[-5]
            _sensor_variablename = _sensor.split("_")[-6]

            _network_counter.append(_network)
            _station_counter.append(f"{_network}_{_station}")
            _sensor_counter.append(f"{_network}_{_station}_{_sensor}")

            _network_counter_set, _station_counter_set, _sensor_counter_set = (
                set(_network_counter),
                set(_station_counter),
                set(_sensor_counter),
            )

            sensor_geo_info_dict = self.get_sensor_geo_info_dict_from_header(
                pth)

            _sensor_ids_arr[p] = {
                sensor_filename_segments[0]:
                _network,
                sensor_filename_segments[1]:
                _station,
                sensor_filename_segments[2]:
                _sensor_variablename,
                sensor_filename_segments[3]:
                sensor_geo_info_dict['latitude'],
                sensor_filename_segments[4]:
                sensor_geo_info_dict['longitude'],
                sensor_filename_segments[5]:
                sensor_geo_info_dict['elevation'],
                sensor_filename_segments[6]:
                float(_sensor_dephfrom),
                sensor_filename_segments[7]:
                float(_sensor_dephto),
                sensor_filename_segments[8]:
                _sensor_name,
                sensor_filename_segments[9]:
                datetime.datetime.strptime(_sensor_startdate,
                                           "%Y%m%d").strftime("%Y/%m/%d"),
                sensor_filename_segments[10]:
                datetime.datetime.strptime(_sensor_enddate,
                                           "%Y%m%d").strftime("%Y/%m/%d"),
                # sensor_filename_segments[8]: os.path.join(
                #     *self.get_path_segments(pth)[-4:]
                # ),
                sensor_filename_segments[11]:
                pth,
                sensor_filename_segments[12]:
                f"n{str(len(_network_counter_set)).zfill(3)}s{str(len(_station_counter_set)).zfill(4)}d{str(len(_sensor_counter_set)).zfill(5)}",
            }

        self.sensor_id_to_path_dict = {
            sdict["sensor_id"]: sdict["path"]
            for sdict in _sensor_ids_arr
        }

        self.make_json(
            self.sensor_id_to_path_dict,
            "sensor_id_to_path_dict.json",
            os.path.join(self.database_name, "json_dicts"),
        )

        self.sensor_path_to_id_dict = {
            sdict["path"]: sdict["sensor_id"]
            for sdict in _sensor_ids_arr
        }

        self.make_json(
            self.sensor_path_to_id_dict,
            "sensor_path_to_id_dict.json",
            os.path.join(self.database_name, "json_dicts"),
        )

        _temp = [[*item.values()] for item in _sensor_ids_arr]

        self.sensor_df = pd.DataFrame(data=_temp)
        self.sensor_df.columns = sensor_filename_segments
        self.sensor_df = self.sensor_df.set_index(
            ["network", "station", "sensor_id"])

        self.sensor_df.to_pickle(
            os.path.join(self.database_name, "json_dicts", "sensor_df.pkl"))

        self.sensor_df.to_csv(
            os.path.join(self.database_name, "json_dicts", "sensor_df.csv"))

        return self.sensor_path_to_id_dict, self.sensor_id_to_path_dict, self.sensor_df


class GroupDynamicVariable(Flags):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "dynamic variables"


class GroupC(Flags):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "reported value exceeds output format field size"


class GroupD(Flags):
    category = "D"

    def __init__(self):
        super().__init__()
        # self.category = 'D'

    def __str__(self):
        return "questionable/dubious"


class GeophyscialBased(GroupD):
    sub_category = "geophysical based"

    def __init__(self):
        super().__init__()
        # self.sub_category = 'geophysical based'

    def __str__(self):
        return "geophysical based"


class SpectrumBased(GroupD):
    # def __init__(self):
    #     super().__init__()

    def __str__(self):
        return "spectrum based"


class G(GroupDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "Good"


class M(GroupDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "Parameter value missing"


class C01(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture < 0.0 m^3/m^3"


class C02(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture > 0.6 m^3/m^3"


class C03(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture > saturation point (derived from HWSD parameter values)"


class D01(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "in situ soil temperature (at corresponding depth layer) < 0°C"


class D02(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "in situ air temperature < 0°C"


class D03(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "GLDAS soil temperatureat corresponding depth layer) < 0°C"


class D04(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture shows peaks without precipitation event (in situ) \
            in the preceding 24 hours"


class D05(GeophyscialBased):
    kind = "D05"

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture shows peaks without precipitation event (GLDAS) \
            in the preceding 24 hours"


class D06(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a spike is detected in soil moisture spectrum"


class D07(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a negative jump is detected in soil moisture spectrum"


class D08(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a positive jump is detected in soil moisture spectrum"


class D09(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "low constant values (for a minimum time of 12 hours) occur in \
            soil moisture spectrum"


class D10(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "saturated plateau (for a minimum time length of 12 hours) occurs in \
            soil moisture spectrum"


class FunWithFlags(Flags, DataReader):
    """Small class centered around flags of the ISMN databse.
    Concerning soil moisture, following flags exist:

    Dynamic variables
    -----------------
    G:   Good
    M:   Parameter value missing

    C - reported value exceeds output format field size
    ---------------------------------------------------
    C01: soil moisture < 0.0 m^3/m^3
    C02: soil moisture > 0.6 m^3/m^3
    C03: soil moisture > saturation point (derived from HWSD parameter values)

    D - questionable/dubious - geophysical based
    --------------------------------------------
    D01: in situ soil temperature(*) < 0°C
    D02: in situ air temperature < 0°C
    D03: GLDAS soil temperature(*) < 0°C
    D04: soil moisture shows peaks without precipitation event (in situ) \
        in the preceding 24 hours
    D05: soil moisture shows peaks without precipitation event (GLDAS) \
        in the preceding 24 hours

    D - questionable/dubious - spectrum based
    -----------------------------------------
    D06: a spike is detected in soil moisture spectrum
    D07: a negative jump is detected in soil moisture spectrum
    D08: a positive jump is detected in soil moisture spectrum
    D09: low constant values (for a minimum time of 12 hours) occur in \
        soil moisture spectrum
    D10: saturated plateau (for a minimum time length of 12 hours) occurs \
        in soil moisture spectrum

    (*) at corresponding depth layer
    """

    def __init__(self):
        super().__init__()

    # def __repr__(self):
    #     print(f'Small class centered around flags of the ISMN databse.
    #     Conserning soil moisture, there could be following flags:\n \

    #     Dynamic variables
    #     -----------------
    #     G\tGood
    #     M\tParameter value missing

    #     C - reported value exceeds output format field size
    #     ---------------------------------------------------
    #     C01\tsoil moisture < 0.0 m^3/m^3
    #     C02\tsoil moisture > 0.6 m^3/m^3
    #     C03\tsoil moisture > saturation point (derived from HWSD parameter values)

    #     D - questionable/dubious - geophysical based
    #     --------------------------------------------
    #     D01\tin situ soil temperature(*) < 0°C
    #     D02\tin situ air temperature < 0°C
    #     D03\tGLDAS soil temperature(*) < 0°C
    #     D04\tsoil moisture shows peaks without precipitation event (in situ) \
    # in the preceding 24 hours
    #     D05\tsoil moisture shows peaks without precipitation event (GLDAS) \
    # in the preceding 24 hours

    #     D - questionable/dubious - spectrum based
    #     -----------------------------------------
    #     D06\ta spike is detected in soil moisture spectrum
    #     D07\ta negative jump is detected in soil moisture spectrum
    #     D08\ta positive jump is detected in soil moisture spectrum
    #     D09\tlow constant values (for a minimum time of 12 hours) occur \
    # in soil moisture spectrum
    #     D10\tsaturated plateau (for a minimum time length of 12 hours) occurs \
    # in soil moisture spectrum

    #     (*) at corresponding depth layer
    #     ')

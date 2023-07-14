import os
from screeninfo import get_monitors
from natsort import natsorted
from glob import iglob
from tqdm import trange
from typing import Optional, Any
import json
from collections import defaultdict
from ismn.interface import ISMN_Interface
from .decorators import sizeit, timeit
from .datatypes import MyDataTypes

import logging
from autologging import traced, TRACE, logged
logging.basicConfig(level=TRACE, filename = 'logger.log', format='%(asctime)s - %(levelname)s:%(name)s:%(funcName)s:%(message)s"')


# @logged
# @traced
class Tools:
    """Small collection of tools around the ISMN database"""

    def __init__(self) -> None:
        self.root: str = os.getcwd()

    def get_monitor_resolution(self) -> dict[int, int]:
        """Identifies the resolution of the smallest connected monitor

        Returns
        -------
        dict[str, str]
            Dictionary containing the monitor's height and width in pixels
        """
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
        """Checks if the specified diectory containing the database exists

        Parameters
        ----------
        database_path : str
            String/path to database

        Returns
        -------
        bool
            True if the database exists, False otherwise
        """

        if os.path.isdir(database_path):
            self.database_path = database_path
            return True

        else:
            return False

    @sizeit
    # @property
    def get_database(self) -> tuple[ISMN_Interface, str]:
        """Loads the ISMN database using the ISMN module from the specified path

        Parameters
        ----------
        database_path : str
            String/path to database

        Returns
        -------
        tuple[ISMN_Interface, str]
            An ISMN database object and the name of the database
        """

        self.database_name: str = os.path.basename(
            os.path.normpath(self.database_path))

        try:
            __database = ISMN_Interface(self.database_name, parallel=True)
        except OSError as e:
            __database = ISMN_Interface(self.database_path, parallel=True)


        return __database, self.database_name


    def __get_networks(self) -> tuple[list, list]:
        r"""Returns list containing all networks present in the database \
            and the amount of networks
        """
        __networks = natsorted([
            x for x in natsorted(os.listdir(self.database_path)) if os.path.isdir(os.path.join(self.database_path, x))
            and x not in ["python_metadata", "json_dicts", "graphics"]
        ])
        return __networks, len(__networks)

    def get_all_sensors(self) -> list:
        """Returns list of all sensors present in the database"""
        return natsorted([
            os.path.normpath(os.path.join(self.root, f))
            for f in iglob(os.path.join(self.database_path, "**", "*"),
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

        if 'WEGENERNET' not in pth:  # all stations have names of type string, except for WEGENERNET, who choose numbers  # noqa: E501
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

    @timeit
    def get_all_numbers(
        self
    ) -> tuple[int, int, int, dict, dict]:
        """Counts the total number of networks, stations, and sensors in the database \
            and creates a dictionary.
        :return: Number of networks, number of stations, number of sensors, \
            a dictionary containing which network contains how many stations and \ 
            a dictionary containing which network and station contains how many sensors.
        :rtype: tuple[int, int, int, dict, dict]
        """

        __network_check = "go"

        network_lst, no_of_networks = self.__get_networks()
        no_of_stations: int = 0
        no_of_sensors: int = 0
        stations_dict: dict = {}
        sensors_dict: dict = {}

        for ii in trange(no_of_networks, desc="iterating over networks:"):
            network_name = network_lst[ii]
            try:
                network = self.database[network_name]
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
        # logging.debug(f"no_of_networks: {no_of_networks}")
        self.no_of_stations = no_of_stations
        # logging.debug(f"no_of_stations: {no_of_stations}")
        self.no_of_sensors = no_of_sensors
        # logging.debug(f"no_of_sensors: {no_of_sensors}")
        self.stations_dict = stations_dict
        # logging.debug(f"stations_dict: {stations_dict}")
        self.sensors_dict = sensors_dict
        # logging.debug(f"sensors_dict: {sensors_dict}")

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
        ) -> None:
        """Creates a JSON file with the specified data.
        :param data: The data to be saved in the JSON file
        :type data: dict
        :param json_name: The name of the JSON file
        :type json_name: str
        :return: None
        :rtype: None
        """

        path = os.path.join(self.database_path, 'json_dicts', json_name)
        with open(path,
                  "w") as __outfile:
            json.dump(data, __outfile, indent=2)

        print(f'The JSON file "{json_name}" has been created in the directory \
                "{path}".')

    def read_json(self, json_name: str,
                  path: Optional[str] = os.getcwd()) -> Any:
        """Read the specified JSON file and returns the data.
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
        path = os.path.join(path, dir_name)
        logging.debug(f"path: {path}")
        return os.path.isdir(path)

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


@logged
@traced
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

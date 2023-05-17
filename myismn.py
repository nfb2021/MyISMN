from typing import Any, Optional, TypeVar, Generic
import os
from ismn.interface import ISMN_Interface
import json
import time
from dataclasses import dataclass, fields


class MyDataTypes:
    """Definition of my own data types for variables"""

    IsmnDataBase = TypeVar("IsmnDataBase")  # loaded ISMN database


class Tools:

    """Small collection of tools around the ISMN database"""

    def check_database(self, database_path: str) -> bool:
        """Checks if the specified diectory containing the database exists.
        :param database_path: A string containing the path (absolute or relative) to the database
        :type database_path: str
        :return: True if the database exists, False otherwise
        :rtype: bool''
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
        """

        __database_name: str = os.path.basename(os.path.normpath(database_path))
        __database = ISMN_Interface(__database_name, parallel=True)

        return __database, __database_name

    def get_all_numbers(
        self, database: MyDataTypes.IsmnDataBase
    ) -> tuple[int, int, int, dict, dict]:
        """Counts the total number of networks, stations, and sensors in the database and creates a dictionary.
        :param database: An ISMN database object
        :type database: MyDataTypes.IsmnDataBase
        :return: Number of networks, number of stations, number of sensors, a dictionary containing which network contains how many stations and a dictionary containing which network and station contains how many sensors.
        :rtype: tuple[int, int, int, dict, dict]
        """

        __network_check = "go"
        no_of_networks: int = 0
        no_of_stations: int = 0
        no_of_sensors: int = 0
        stations_dict: dict = {}
        sensors_dict: dict = {}

        while __network_check == "go":
            try:
                network = database[no_of_networks]

                for s, _ in enumerate(network):
                    if ValueError:
                        no_of_stations += 1

                    station = network[s]
                    for ss, _ in enumerate(station):
                        if ValueError:
                            no_of_sensors += 1

                    sensors_dict[f"{network.name}:{station.name}"] = ss + 1

                stations_dict[network.name] = s + 1
                no_of_networks += 1

            except IndexError:
                __network_check = "stop"

        self._func_get_all_numbers_ran = True

        return (
            no_of_networks,
            no_of_stations,
            no_of_sensors,
            stations_dict,
            sensors_dict,
        )

    def make_json(
        self, data: dict, json_name: str, where_to_be_saved: Optional[str] = os.getcwd()
    ) -> None:
        """Creates a JSON file with the specified data.
        :param data: The data to be saved in the JSON file
        :type data: dict
        :param json_name: The name of the JSON file
        :type json_name: str
        :param where_to_be_saved: A string containing the path (absolute or relative) to where the JSON is to be saved, by default the current working directory
        :type where_to_be_saved: Optional[str]
        :return: None
        :rtype: None
        """

        with open(os.path.join(where_to_be_saved, json_name), "w") as __outfile:
            json.dump(data, __outfile)

        print(
            f'The JSON file "{json_name}" has been created in the directory "{os.path.join(where_to_be_saved)}".'
        )

    def read_json(self, json_name: str, path: Optional[str] = os.getcwd()) -> Any:
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

    def file_exist_status(
        self, file_name: str, path: Optional[str] = os.getcwd()
    ) -> bool:
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
        self, dir_name: str, path: Optional[str] = os.getcwd()
    ) -> bool:
        """Checks if the specified directory exists in the specified path.
        :param dir_name: The name of the directory
        :type dir_name: str
        :param path: The path to the directory, by default the current working directory
        :type path: Optional[str]
        :return: True if the directory exists, False otherwise
        :rtype: bool
        """
        return os.path.isdir(os.path.join(path, dir_name))


class Geography(Tools):
    """Special class for everything Geography"""

    def __init__(self):
        super.__init__()

    def get_country_from_coords(self, latitude: float, longitude: float) -> str:
        """Get a country for input latitude and longitude. Based on https://github.com/che0/countries and https://thematicmapping.org/downloads/world_borders.php

        :param latitude: The latitude of the location
        :type latitude: float
        :param longitude: The longitude of the location
        :type longitude: float
        :return: The country name
        :rtype: str"""

        from CoordPy import countries

        cc = countries.CountryChecker(
            os.path.join("CoordPy", "TM_WORLD_BORDERS", "TM_WORLD_BORDERS-0.3.shp")
        )

        try:
            return cc.getCountry(countries.Point(latitude, longitude)).iso

        except AttributeError:
            return "None"

    def sort_stations_to_countries(self) -> tuple[dict, dict]:
        if not os.path.isfile(
            os.path.join(self.database_name, "json_dicts", "countries.json")
        ) and not os.path.isfile(
            os.path.join(self.database_name, "json_dicts", "locations.json")
        ):
            print("if")
            locations_dict = {}
            sorted_countries_dict = {}

            for key, item in self.sensors_dict.items():
                _network, _station = key.split(":")[0], key.split(":")[1]
                _no_of_sensors = int(item)

                counter = 0
                _country = []
                while counter < _no_of_sensors:
                    lat = (
                        self.database[_network][_station][counter]
                        .metadata["latitude"]
                        .val
                    )
                    long = (
                        self.database[_network][_station][counter]
                        .metadata["longitude"]
                        .val
                    )

                    _country.append(self.get_country_from_coords(lat, long))

                    counter += 1

                _country = list(set(_country))
                if len(_country) == 1:
                    if _station not in locations_dict:
                        locations_dict[f"{key}"] = _country[0]

                    try:
                        if _country[0] not in sorted_countries_dict:
                            sorted_countries_dict[_country[0]] = [_network]
                        if (
                            _country[0] in sorted_countries_dict
                            and _network not in sorted_countries_dict[_country[0]]
                        ):
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
                                sorted_countries_dict["Unknown"]
                            ).append(_network)

            sorted_countries_dict = dict(sorted(sorted_countries_dict.items()))
            self.countries_dict = sorted_countries_dict
            self.locations_dict = locations_dict

            with open(
                os.path.join(self.database_name, "json_dicts", "locations.json"), "w"
            ) as outfile:
                json.dump(locations_dict, outfile)

            with open(
                os.path.join(self.database_name, "json_dicts", "countries.json"), "w"
            ) as outfile:
                json.dump(sorted_countries_dict, outfile)

        else:
            print("else")
            with open(
                os.path.join(self.database_name, "json_dicts", "countries.json")
            ) as json_file:
                self.countries_dict = json.load(json_file)

            with open(
                os.path.join(self.database_name, "json_dicts", "locations.json")
            ) as json_file:
                self.locations_dict = json.load(json_file)

        print("now i return")
        return self.countries_dict, self.locations_dict

    def sort_stations_to_countries2(self) -> tuple[dict, dict]:
        if not os.path.isfile(
            os.path.join(self.database_name, "json_dicts", "countries.json")
        ) and not os.path.isfile(
            os.path.join(self.database_name, "json_dicts", "locations.json")
        ):
            print("if")
            locations_dict = {}
            sorted_countries_dict = {}

            for key, item in self.sensors_dict.items():
                _network, _station = key.split(":")[0], key.split(":")[1]
                _no_of_sensors = int(item)

                counter = 0
                _country = []
                while counter < _no_of_sensors:
                    lat = (
                        self.database[_network][_station][counter]
                        .metadata["latitude"]
                        .val
                    )
                    long = (
                        self.database[_network][_station][counter]
                        .metadata["longitude"]
                        .val
                    )

                    _country.append(self.get_country_from_coords(lat, long))

                    counter += 1

                _country = list(set(_country))
                if len(_country) == 1:
                    if _station not in locations_dict:
                        locations_dict[f"{key}"] = _country[0]

                    try:
                        if _country[0] not in sorted_countries_dict:
                            sorted_countries_dict[_country[0]] = [_network]
                        if (
                            _country[0] in sorted_countries_dict
                            and _network not in sorted_countries_dict[_country[0]]
                        ):
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
                                sorted_countries_dict["Unknown"]
                            ).append(_network)

            sorted_countries_dict = dict(sorted(sorted_countries_dict.items()))
            self.countries_dict = sorted_countries_dict
            self.locations_dict = locations_dict

            with open(
                os.path.join(self.database_name, "json_dicts", "locations.json"), "w"
            ) as outfile:
                json.dump(locations_dict, outfile)

            with open(
                os.path.join(self.database_name, "json_dicts", "countries.json"), "w"
            ) as outfile:
                json.dump(sorted_countries_dict, outfile)

        else:
            print("else")
            with open(
                os.path.join(self.database_name, "json_dicts", "countries.json")
            ) as json_file:
                self.countries_dict = json.load(json_file)

            with open(
                os.path.join(self.database_name, "json_dicts", "locations.json")
            ) as json_file:
                self.locations_dict = json.load(json_file)

        print("now i return")
        return self.countries_dict, self.locations_dict


class DataReader(Tools):

    """Small collection of tools around the ISMN database"""

    def __init__(self, database: Any, process_parallel: Optional[bool] = True) -> None:
        super().__init__()

        if self.check_database(database):
            self.database, self.database_name = self.get_database(database)
        else:
            raise ValueError(
                f'The specified database "{database}" does not exist in the current working directory: "{os.getcwd()}".'
            )

        if not self.directory_exist_status(
            "json_dicts", os.path.join(os.getcwd(), self.database_name)
        ):
            os.mkdir(os.path.join(os.getcwd(), self.database_name, "json_dicts"))

        if self.file_exist_status(
            "numbers.json", os.path.join(os.getcwd(), "json_dicts")
        ):
            self.numbers_dict = self.read_json(
                "numbers.json", path=os.path.join(os.getcwd(), "json_dicts")
            )
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
            self.make_json(
                {
                    "Networks": self.no_of_networks,
                    "Stations": self.no_of_stations,
                    "Sensors": self.no_of_sensors,
                },
                "numbers.json",
                os.path.join(self.database_name, "json_dicts"),
            )

        if self.file_exist_status(
            "stations.json", os.path.join(os.getcwd(), "json_dicts")
        ):
            self.stations_dict = self.read_json(
                "stations.json", path=os.path.join(os.getcwd(), "json_dicts")
            )
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

        if self.file_exist_status(
            "sensors.json", os.path.join(os.getcwd(), "json_dicts")
        ):
            self.stations_dict = self.read_json(
                "sensors.json", path=os.path.join(os.getcwd(), "json_dicts")
            )
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

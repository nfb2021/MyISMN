from tools import Tools
import os
from typing import Any, Optional


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

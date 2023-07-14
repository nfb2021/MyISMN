from .tools import Tools
import os
from typing import Any, Optional

import logging
from autologging import traced, TRACE, logged
logging.basicConfig(level=TRACE, filename = 'logger.log', format='%(asctime)s - %(levelname)s:%(name)s:%(funcName)s:%(message)s"')


# @logged
# @traced
class DataReader(Tools):

    def __init__(self,
                 database: Any,
                 process_parallel: Optional[bool] = True) -> None:
        super().__init__()

        if self.check_database(database):
            self.database, _ = self.get_database()
        else:
            raise ValueError(
                f'The specified database "{self.database_name}" does not exist \
                    in the  directory: "{os.path.basename(self.database_path)}".')

        if not self.directory_exist_status(
                "json_dicts", self.database_path):
            os.mkdir(
                os.path.join(self.database_path, "json_dicts"))

        if self.file_exists("numbers.json",
                            os.path.join(self.database_path, "json_dicts")):
            self.numbers_dict = self.read_json("numbers.json",
                                               path=os.path.join(
                                                   self.database_path, "json_dicts"))
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
            ) = self.get_all_numbers()
            self.numbers_dict = {
                "Networks": self.no_of_networks,
                "Stations": self.no_of_stations,
                "Sensors": self.no_of_sensors,
            }
            self.make_json(
                self.numbers_dict,
                "numbers.json"
            )

        if self.file_exists("stations.json",
                            os.path.join(self.database_path, "json_dicts")):
            self.stations_dict = self.read_json("stations.json",
                                                path=os.path.join(
                                                    self.database_path, "json_dicts"))
        elif not self._func_get_all_numbers_ran:
            self.make_json(
                self.stations_dict,
                "stations.json"
            )
        else:
            (
                self.no_of_networks,
                self.no_of_stations,
                self.no_of_sensors,
                self.stations_dict,
                self.sensors_dict,
            ) = self.get_all_numbers()
            self.make_json(
                self.stations_dict,
                "stations.json"
            )

        if self.file_exists("sensors.json",
                            os.path.join(self.database_path, "json_dicts")):
            self.stations_dict = self.read_json("sensors.json",
                                                path=os.path.join(
                                                    self.database_path, "json_dicts"))
        elif not self._func_get_all_numbers_ran:
            self.make_json(
                self.stations_dict,
                "sensors.json"
            )
        else:
            (
                self.no_of_networks,
                self.no_of_stations,
                self.no_of_sensors,
                self.stations_dict,
                self.sensors_dict,
            ) = self.get_all_numbers()
            self.make_json(
                self.sensors_dict,
                "sensors.json"
            )



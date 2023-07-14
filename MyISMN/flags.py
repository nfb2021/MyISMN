import polars as pl
import pandas as pd
import numpy as np
import datetime
from typing import Any, Optional
import os
import json
from collections import defaultdict
from multiprocessing import Pool
from datahandler import DataReader
from decorators import sizeit, timeit


def flag_reader(sensor_path: str) -> pl.DataFrame:
    """Takes a string/path of a sensor file as input and returns the parsed file as a polars DataFrame"""  # noqa: E501
    return pl.read_csv(
        source=sensor_path,
        has_header=True,
        columns=[3],
        separator=" ",
        dtypes=[pl.Utf8],
        try_parse_dates=True,
        use_pyarrow=True,
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
            print("\nConstructing and subsequently pickling the dataframe. \
                    This might take some time, but is only done once.")

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
            print("\nConstructing and subsequently pickling the dataframe. \
                    This might take some time, but is only done once.")

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

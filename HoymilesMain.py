import json
import logging
import os
from datetime import datetime
from hoymilesapi import Hoymiles

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("HoymilesAdd-on")
logger.setLevel(logging.INFO)

def get_secrets() -> dict:
    """Getting data from config json file

    Returns:
        dict: Config data in dict shape
    """
    json_path = ""
    if os.path.isfile("./config.json"):
        json_path = "config.json"
    elif os.path.isfile("/config.json"):
        json_path = "/config.json"
    else:
        json_path = "/data/options.json"
    with open(json_path, "r", encoding="utf-8") as json_file:
        config = json.load(json_file)
        if "options" in config.keys():
            config = config["options"]
        log_level = config["LOG_LEVEL"]
        if log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"]:
            logger.setLevel(log_level)
        if config["DEVELOPERS_MODE"]:
            logger.setLevel(logging.DEBUG)
    return config

def getHoymilesData(Data) -> float:
    """Main function of script"""
    g_envios = {
        "last_time": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
        "load_cnt": 0,
        "load_time": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
    }

    config = get_secrets()

    plant_list = {}
    for id in config["HOYMILES_PLANT_ID"].split(","):
        id = id.strip()
        if int(id) < 100:
            logger.warning(f"Wrong plant ID {id}")

        plant_list[id] = Hoymiles(plant_id=int(id), config=config, g_envios=g_envios)

        if plant_list[id].connection.token == "":
            logger.error("I can't get access token")
            quit()

        if not plant_list[id].verify_plant():
            logger.error("User has no access to plant")
            quit()

        plant_list[id].get_solar_data()
        return float(plant_list[config['HOYMILES_PLANT_ID']].solar_data[Data])
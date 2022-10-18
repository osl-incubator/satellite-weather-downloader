"""
Copernicus API requires user credentials to connect and download
weather data from their Database. These credentials can be found
in your profile page, after creating an account.
https://cds.climate.copernicus.eu/user/<uid>
Copernicus API requires two credentials to be searched in the path
$HOME/.cdsapirc, with the format:

url: https://cds.climate.copernicus.eu/api/v2
key: <uid>:<key>

These credentials only need to be configured once, `cdsapi` will
check them every request to the API. If the method `download` is
called, `.cdsapirc` will be searched. If the file is missing, a
prompt will ask for its first configuration. Then, the status will
be returned if successful:

'info': ['Welcome to the CDS']
'warning': []

Methods
-------

_interactive_con(answer)    : Asks for input UID and Key, will later be
                              stored at $HOME directory.

_check_credentials(uid, key): Credentials will be checked for right
                              UID length and Key format.

connect(opt[uid], opt[key]) : If none credentials are passed, it will request
                              a Status from `cdsapi` Client, if credentials are
                              not found, will enter _interactive_con.
"""
import uuid
import cdsapi
import logging

from typing import Optional
from satellite_weather_downloader.utils.globals import CDSAPIRC_PATH

credentials = "url: https://cds.climate.copernicus.eu/api/v2\n" "key: "


def _interactive_con(answer):
    """
    Asks for UID and Key via input.

    Attrs:
        answer (str): If != no, return uid and key for later verification.

    Returns:
        uid (input) : UID before verification.
        key (input) : API Key before verification.
    """
    no = ["N", "n", "No", "no", "NO"]
    if answer not in no:
        uid = str(input("Insert UID: "))
        key = str(input("Insert API Key: "))
        return uid, key
    else:
        logging.info("Usage: `cds_weather.connect(uid, key)`")


def _check_credentials(uid, key):
    """
    Simple evaluation for prevent credentials misspelling.

    Attrs:
        uid (str): UID found in Copernicus User page.
        key (str): API Key found in Copernicus User page.

    Returns:
        uid (str): UID ready to be stored at $HOME/.cdsapirc.
        key (str): API ready to be stored at $HOME/.cdsapirc.
    """
    valid_uid = eval("len(uid) == 6")
    valid_key = eval("uuid.UUID(key).version == 4")
    if not valid_uid:
        return logging.error("Invalid UID.")
    if not valid_key:
        return logging.error("Invalid API Key.")
    return uid, key


def connect(
    uid: Optional[str] = None,
    key: Optional[str] = None,
):
    """
    `connect()` will be responsible for inserting the credentials in
    the $HOME directory. If the file exists, it will simply call
    `cdsapi.Client().status()` and return a instance of the Client.
    If the credentials are passed with the `connect(api,key)` method or
    via `_interactive_con()`, the values are evaluated and stored at
    `$HOME/.cdsapirc` file, returning the Client instance as well.

    Attrs:
        uid (opt(str)) : UID found in Copernicus User page.
        key (opt(str)) : API Key found in Copernicus User page.

    Returns:
        cdsapi.Client(): Instance of the Copernicus API Client, used
                         for requesting data from the API.
    """
    if not uid and not key:
        try:
            status = cdsapi.Client().status()
            logging.info("Credentials file configured.")
            logging.info(status["info"])
            logging.warning(status["warning"])

        except Exception as e:
            logging.error(e)

            answer = input("Enter interactive mode? (y/n): ")
            uid_answer, key_answer = _interactive_con(answer)
            uid, key = _check_credentials(uid_answer, key_answer)

            with open(CDSAPIRC_PATH, "w") as f:
                f.write(credentials + f"{uid}:{key}")
                logging.info(f"Credentials stored at {CDSAPIRC_PATH}")
                logging.info(cdsapi.Client().status()["info"])

        finally:
            return cdsapi.Client()

    try:
        uid, key = _check_credentials(uid, key)
        with open(CDSAPIRC_PATH, "w") as f:
            f.write(credentials + f"{uid}:{key}")
            logging.info(f"Credentials stored at {CDSAPIRC_PATH}")

        return cdsapi.Client()

    except Exception as e:
        logging.error(e)

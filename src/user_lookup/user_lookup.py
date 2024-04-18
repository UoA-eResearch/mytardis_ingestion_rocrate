"""Access the Active directory and look up person details using LDAP"""

import logging
from sys import platform
from typing import Optional, Tuple

import ldap3
import passpy
from ldap3.utils.conv import escape_filter_chars
from ldap3.utils.dn import escape_rdn

from src.rocrate_builder.constants.organisatons import UOA
from src.rocrate_dataclasses.rocrate_dataclasses import Person

logger = logging.getLogger(__name__)

if platform in ["linux", "linux2"]:
    # linux
    GPG_BIN = "/usr/bin/gpg"
elif platform == "darwin":
    # OS X
    GPG_BIN = "/opt/homebrew/bin/gpg"
elif platform == "win32":
    # Windows
    GPG_BIN = "C:\\Program Files (x86)\\GnuPG\\bin\\gpg.exe"
else:
    logger.warning(
        "Unknown OS, please define where the gpg executable binary can be located"
    )


def lookup_user(upi: str) -> Optional[Tuple[str, str, str]]:
    """Looks up a user in the Active directory and returns their first name, last name and email

    Args:
        upi (str): The user's UPI

    Raises:
        ValueError: An error that is raised if the lookup fails

    Returns:
        Tuple[str,str,str]: A tuple containing the user's first name, last name and email address
    """
    if GPG_BIN is None:
        return None
    try:
        store = passpy.Store(gpg_bin=GPG_BIN)
        user = store.get_key("LDAP/admin_name")
        user = user.replace("\n", "")
        password = store.get_key("LDAP/admin_password")
        password = password.replace("\n", "")
        upi = escape_rdn(upi)
    except FileNotFoundError as e:
        logger.warning("Could not find value in LDAP lookup, %s", e)
        return None
    server = ldap3.Server(
        "ldaps://uoa.auckland.ac.nz",
        port=636,
        use_ssl=True,
    )
    search_filter = f"({escape_filter_chars(f'cn={upi}')})"
    with ldap3.Connection(server, user=user, password=password) as connection:
        try:
            data = _get_data_from_active_directory_(connection, search_filter)
            if not data:
                error_message = f"No one with cn: {upi} has been found in the LDAP"
                if logger:
                    logger.warning(error_message)
            return data
        except ValueError as err:
            error_message = (
                f"More than one person with cn: {upi} has  been found in the LDAP"
            )
            if logger:
                logger.error(error_message)
            raise ValueError(error_message) from err
    return None


def _get_data_from_active_directory_(
    connection: ldap3.Connection, search_filter: str
) -> Optional[Tuple[str, str, str]]:
    """With connection to Active Directory, run a query

    Args:
        connection (ldap3.Connection): An established connection to the active directory
        search_filter (str): The search query to run

    Raises:
        ValueError: Represents a lack of uniqueness in the query

    Returns:
        Optional[Dict[str, str]]: The dictionary of resutls from the search query run or None
    """
    connection.start_tls()
    connection.bind()
    connection.search(
        "ou=People,dc=UoA,dc=auckland,dc=ac,dc=nz",
        search_filter,
        attributes=["*"],
    )
    if len(connection.entries) > 1:
        raise ValueError()
    if len(connection.entries) == 0:
        connection.unbind()
        return None
    person = connection.entries[0]
    first_name_key = "givenName"
    last_name_key = "sn"
    email_key = "mail"
    first_name = person[first_name_key].value
    last_name = person[last_name_key].value
    try:
        email = person[email_key].value
    except KeyError:
        email = ""
    connection.unbind()
    return (first_name, last_name, email)


def create_person_object(upi: str) -> Person:
    """Look up a UPI and create a Person entry from the results

    Args:
        upi (str): The UPI for the person

    Returns:
        Person: A Person object created from the results of the UPI lookup

    Raises:
        ValueError: If the UPI can't be found
    """
    return Person(
        name="",
        email="",
        affiliation=UOA,
        identifiers=[upi],
    )

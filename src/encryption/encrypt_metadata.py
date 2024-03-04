# pylint: disable=too-few-public-methods, fixme
"""
Script for handling encryption objects such as keys encryption/decryption classes
Mostly stubs right now...

"""


class Encryptor:
    """Class for encryption of metadata as read into an RO-Crate"""

    def __init__(self, public_keys: list[str]):
        """setup encryptor object to encrypt strings based on a provided set of public keys

        Args:
            public_keys (list[str]): a list of public keys to encrypt strings to
        """
        # TODO import or implement public key class for storing keys
        self.keys = public_keys

    def encrypt_string(self, input_string: str) -> str:
        """Encrypt a given string to be decrypted by a range of public keys

        Args:
            input (str): the raw string to encrypt

        Returns:
            str: the encrypted string
        """
        input_string = "This string is so encrypted you can't even read it!"
        # TODO replace this stub with encryption method us self.keys
        return input_string

# pylint: disable=missing-module-docstring, too-few-public-methods
import importlib
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
PROFILE_BASE_LIB = "src.profiles."


class ProfileLoader:  # pylint: disable=too-few-public-methods
    """Load profiles based on profile names, taken from the MyTardis ingestion scripts"""

    def __init__(
        self,
        profile: str,
    ) -> None:
        """
        Initialize the ProfileLoader with the given profile.

        Args:
            profile (str): The name of the profile to load.

        Raises:
            ValueError: If the profile is not set.
        """
        if not profile:
            raise ValueError("Profile not set")
        self.profile = profile
        try:
            self.profile_module_str = PROFILE_BASE_LIB + profile
            self.profile_module = importlib.import_module(self.profile_module_str)
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(  # pylint: disable=broad-exception-caught
                "Error loading profile module, profile not found"
            ) from e
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(e)

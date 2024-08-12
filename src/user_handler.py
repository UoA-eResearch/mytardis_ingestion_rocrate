from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    MTMetadata,
    MyTardisContextObject,
    User
)

class UserHandler:

    def __init__(
        self,
        api_agent: MyTardisRestAgent,
        schema_namespaces: Dict[MtObject, str],
        pubkey_fingerprints: Optional[List[str]],
    ):
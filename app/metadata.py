class ServerMetadata:
    def __init__(self, replicaof: str, master_replid: str, master_repl_offset: int) -> None:
        self.role = "slave" if replicaof is not None else "master"
        self.master_replid = master_replid
        self.master_repl_offset = master_repl_offset

    def to_str(self) -> str:
        return f"role:{self.role}\nmaster_replid:{self.master_replid}\nmaster_repl_offset:{self.master_repl_offset}"

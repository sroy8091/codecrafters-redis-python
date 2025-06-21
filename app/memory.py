import json

MEMORY_FILE = "memory.rdb"

def set_memory_file(args):
    global MEMORY_FILE
    if args.dir is not None:
        print(f"Using directory: {args.dir}")
        if args.dbfilename is not None:
            print(f"Using RDB file: {args.dbfilename}")
            MEMORY_FILE = args.dir + "/" + args.dbfilename
            with open(MEMORY_FILE, "w") as f:
                json_str = json.dumps({"dir": (["dir", args.dir], -1), "dbfilename": (["dbfilename", args.dbfilename], -1)})
                print(f"Writing into storage {MEMORY_FILE} with data {json_str}")
                f.write(json_str)

    print("memory stored in ", MEMORY_FILE)

def store():
    pass

def fetch():
    pass
                

# IMPORTANT: expects working path to be root!

if __name__ == '__main__':
    from glob import glob

    from processing.database import Database

    # figure out what files we need to read
    processed = set()
    with open('data/processed.txt', 'ra') as f:
        for line in f.readlines():
            processed.add(line)

    todo = []
    for x in glob('data/RC*.zst'): # for now, we only accept comment data
        if x not in processed:
            todo.append(x)

    # load in databases
    db = Database(clear = True, writable = True)
    for t in todo:
        db.read(t, True, 100_000)

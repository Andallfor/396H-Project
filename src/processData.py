# IMPORTANT: expects working path to be root!

INPUT = "data"
PROCESSED = 'data/processed.txt'
DATABASE = 'data/database.db'
BACKUP = 'data/backup'

if __name__ == '__main__':
    from glob import glob
    import os
    from processing.database import Database

    os.makedirs(os.path.dirname(PROCESSED), exist_ok = True)
    if not os.path.exists(PROCESSED):
        open(PROCESSED, 'w').close()

    # figure out what files we need to read
    processed = set()
    processedHandle = open(PROCESSED, 'r')
    if processedHandle.readable():
        for line in processedHandle.readlines():
            processed.add(line.strip())
    processedHandle.close()

    todo = []
    for x in glob(os.path.join(INPUT, 'RC*.zst')): # for now, we only accept comment data
        if x not in processed:
            todo.append(x)
        else:
            print(f'Skipping {x} as it has been marked as already processed')

    # load in databases
    if len(todo) > 0:
        processedHandle = open(PROCESSED, 'a')
        db = Database(database = DATABASE, backupLoc = BACKUP, clear = True, writable = True, backup = False)
        for t in todo:
            db.read(t, True, 5_000_000)
            #processedHandle.write(t + '\n')
            pass
        
        processedHandle.close()

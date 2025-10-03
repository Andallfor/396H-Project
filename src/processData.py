# IMPORTANT: expects working path to be root!

if __name__ == '__main__':
    from glob import glob
    import os
    from processing.database import Database

    if not os.path.exists('data/processed.txt'):
        open('data/processed.txt', 'w').close()

    # figure out what files we need to read
    processed = set()
    processedHandle = open('data/processed.txt', 'r')
    if processedHandle.readable():
        for line in processedHandle.readlines():
            processed.add(line.strip())
    processedHandle.close()

    todo = []
    for x in glob('data/RC*.zst'): # for now, we only accept comment data
        if x not in processed:
            todo.append(x)

    # load in databases
    if len(todo) > 0:
        processedHandle = open('data/processed.txt', 'a')
        db = Database(clear = True, writable = True)
        for t in todo:
            db.read(t, True)
            processedHandle.write(t + '\n')
        
        processedHandle.close()

import sqlite3
import re
import shutil
import time
import pandas as pd
import os

from processing.reader import Reader
from processing.definitions import RemovalType, Collapsed, Distinguished, SubredditType

class Database:
    def __init__(self, database = "data/database.db", backupLoc = "data/backup", backup = True, clear = False, writable = False):
        # init database and backup as necessary
        os.makedirs(os.path.dirname(database), exist_ok = True)
        os.makedirs(os.path.dirname(backupLoc), exist_ok = True)

        isNew = False
        if not os.path.exists(database):
            isNew = True
            print(f'WARN: Database file not found at {database}, creating a new one at given location')
            open(database, 'w').close()

        if writable and not isNew and backup:
            # make backup of database incase something breaks
            if not os.path.exists(backupLoc):
                os.mkdir(backupLoc)
            shutil.copyfile(database, os.path.join(backupLoc, f'backup-{time.time()}.db'))
        
        self.databaseLoc = database
        self.backupLoc = backupLoc

        self.writable = writable
        self.file = database
        self.con = sqlite3.connect(database)
        self.cur = self.con.cursor()

        # TODO: for future, probably best to also split data into a users and subreddit table

        # NOTICE! BOOLEAN is not an actual datatype, we convert it to INTEGER IN (0,1)
        '''
        Notes on some data that is not included:
            From 3.7m comments, the below variables all had the exact same value - they seem to be redundant and so are not included
            num_awards (len(all_awardings))
            num_mod_reports (len(mod_reports))
            gilded
            total_awards_received
            author_is_blocked
            can_mod_post
            downs (and therefore ups, since ups == score)
        '''
        self.definitions = {
            'comments': { # https://github.com/ArthurHeitmann/arctic_shift/blob/master/schemas/RC/2025/RC_2025-07.ts
                'custom': [
                    # custom/need some processing
                    'num_sentences INTEGER',
                    'edited BOOLEAN', # given is false|utc timestamp, we coerce timestamp into true

                    # enums
                    'removal_type INTEGER', # RemovalType
                    'collapsed INTEGER', # Collapsed
                    'distinguished INTEGER', # Distinguished
                    'subreddit_type INTEGER', # SubredditType
                ],
                'literal': [ # expects column name to match the one given in zst schema
                    'author TEXT',
                    'body TEXT',
                    'created_utc INTEGER', # utc timestamp of when post was created
                    'archived BOOLEAN',
                    'controversiality INTEGER',
                    'id TEXT', # specific id for this comment
                    'link_id TEXT', # id of the submission the comment is under
                    'locked BOOLEAN',
                    'is_submitter BOOLEAN', # whether the commenter is the OP of the post? I think?
                    'parent_id TEXT', # id of the comment/submission the comment is replying to
                    'score INTEGER', # downvotes + upvotes
                    'subreddit_id TEXT',
                    'subreddit TEXT',
                    'stickied BOOLEAN',
                    'permalink TEXT',
                ]
            },
        }

        if clear:
            for name in self.definitions.keys():
                self.cur.execute(f'DROP TABLE IF EXISTS {name}')
            self.cur.execute(f'VACUUM')
            self.con.commit()

        boolReplace = re.compile(r'(\w+)\sBOOLEAN')
        for name, typ in self.definitions.items():
            schema = ','.join(typ['custom'] + typ['literal'])
            # boolean is not natively supported, so we define it as 0 or 1
            schema = re.sub(boolReplace, lambda m: f'{m.group(1)} INTEGER CHECK ({m.group(1)} IN (0,1))', schema)
            self.cur.execute(f'CREATE TABLE IF NOT EXISTS {name} ({schema}) STRICT;')
        
        self.con.commit()
    
    def query(self, cmd):
        return pd.read_sql(cmd, self.con)

    def _defHeaders(self, definition, initList = True) -> dict[str, any]:
        out = dict()
        for col, _ in [x.split(' ') for x in (definition['custom'] + definition['literal'])]:
            out[col] = [] if initList else 0
        
        return out
    
    def read(self, file, isComments, numLines = -1):
        if not self.writable:
            raise Exception("Database was initialized as non-writable, cannot perform file read operation!")

        if not isComments:
            raise NotImplementedError("Currently only support comments")
        else:
            self.logHandle = open(os.path.join(os.path.dirname(self.databaseLoc), 'log.txt'), 'w')
            self._rc(file, numLines)
            self.logHandle.close()
    
    def _rc(self, file, numLines):
        def ins(key, value):
            buf[key].append(value)

        BUFFER_MAX_LENGTH = 100_000
        if numLines != -1:
            BUFFER_MAX_LENGTH = min(BUFFER_MAX_LENGTH, numLines)
        
        print(f'=== Reading {'all' if numLines == -1 else numLines} lines from {file} ===')
        timeStart = time.time()
        dbSizeStart = os.stat(self.databaseLoc).st_size

        # need to bulk insert into table for performance
        buf = self._defHeaders(self.definitions['comments'])

        # define sentence endings as .?! followed by a space/new line, or any character followed by a new line
        sentenceDelimiter = re.compile(r'(\.|\?|!|\S$)(\s|$)', flags = re.MULTILINE)

        reader = Reader(file)
        for ind, obj in enumerate(reader.read(numLines)):
            obj['body'] = obj['body'].strip()

            # literals can just be directly copied from read file
            for col in [x.split(' ')[0] for x in self.definitions['comments']['literal']]:
                ins(col, obj[col])

            ins('num_sentences',   len(re.findall(sentenceDelimiter, obj['body'])))
            ins('edited',          not isinstance(obj['edited'], bool))

            ins('removal_type',    RemovalType.make(obj).value)
            ins('collapsed',       Collapsed.make(obj).value)
            ins('distinguished',   Distinguished.make(obj).value)
            ins('subreddit_type',  SubredditType.make(obj).value)

            # clear buffer and insert values into table
            if ind != 0 and ind % BUFFER_MAX_LENGTH == 0:
                self._insert(buf, 'comments')

                buf = self._defHeaders(self.definitions['comments'])
        
        if max([len(v) for v in buf.values()]) > 0:
            self._insert(buf, 'comments')
        
        print()
        print(f'Finished reading {file} (elapsed: {Reader._tFmt(time.time() - timeStart)})')
        print(f'Read size: {Reader._bFmt(reader.sz)}')
        print(f'Database size (difference): {Reader._bFmt(os.stat(self.databaseLoc).st_size - dbSizeStart)}')

    def _insert(self, buf: dict[str, list[bool] | list[int] | list[str]], table: str):
        col = list(buf.keys())
        values = []

        # we expect all keys to have the same num values
        m = max([len(v) for v in buf.values()])
        for c in col:
            if len(buf[c]) != m:
                self.logHandle.write(f'WARN: Column {c} has length {len(buf[c])} (expected {m})\n')
                self.logHandle.flush()
        
        for i in range(m):
            v = []
            for c in col:
                b = buf[c]

                # when creating SQL insert command, we need to sanitize strings
                if i >= len(b):
                    v.append('')
                elif isinstance(b[i], str):
                    v.append('"' + b[i].replace('"', '""').replace("'", "''") + '"')
                else:
                    v.append(str(b[i]))

            values.append(f'({','.join(v)})')
        
        if len(values) == 0:
            self.logHandle.write(f"WARN: Attempted to insert with no values! (m={m})\n")
            self.logHandle.flush()
            return

        cmd = f'INSERT INTO {table} ({','.join(col)}) VALUES {','.join(values)};'
        self.cur.execute(cmd)
        self.con.commit()

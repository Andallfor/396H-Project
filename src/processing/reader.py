# most of the actual parsing code is from https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py

import zstandard
import json
import os
import time
import datetime

class Reader():
    def __init__(self, file):
        self.file = file
        self.size = os.stat(file).st_size

        self.valid = 0
        self.invalid = 0
    
    def _read_and_decode(self, reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
            return self._read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

    def read(self, num = -1):
        startTime = time.time()

        with open(self.file, 'rb') as file_handle:
            def _print():
                sz = file_handle.tell()
                percent = sz / self.size
                barLen = 50
                bar = f'{'=' * int(percent * barLen)}{' ' * int(barLen - percent * barLen)}'
                ln = f'({self.valid}/{self.invalid}/{self.valid + self.invalid})'
                pt = f'{int(percent * 100)}% ({self._bFmt(sz)}/{self._bFmt(self.size)})'

                eta = self._tFmt((1 / percent - 1) * (time.time() - startTime)) if percent != 0 else 'Unknown'

                print(f'\r{self.file} {ln} -- [{bar}] {pt} -- ETA: {eta}', end='')

            _print()

            buffer = ''
            with zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle) as reader:
                while True:
                    chunk = self._read_and_decode(reader, 2**27, (2**29) * 2)
                    if not chunk:
                        break

                    lines = (buffer + chunk).split("\n")
                    for line in lines[:-1]:
                        try:
                            obj = json.loads(line)
                            self.valid += 1

                            yield obj
                        except:
                            self.invalid += 1

                        if num != -1 and (self.valid + self.invalid) >= num:
                            _print()
                            return

                    buffer = lines[-1]
                    _print()

            _print()

    def _bFmt(self, num, suffix="B"): # https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
        for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
            if abs(num) < 1024.0:
                return f"{num:3.1f} {unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f} Yi{suffix}"
    
    def _tFmt(self, seconds): # https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
        if seconds == 0:
            return 'inf'
        parts = []
        for unit, div in (('hour', 3600), ('min', 60), ('sec', 1)):
            amount, seconds = divmod(int(seconds), div)
            if amount > 0:
                parts.append('{} {}{}'.format(amount, unit, "" if amount == 1 else "s"))
        return ', '.join(parts)

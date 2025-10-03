# most of the actual parsing code is from https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py

import zstandard
import json
import os

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
        with open(self.file, 'rb') as file_handle:
            def _print():
                percent = file_handle.tell() / self.size
                barLen = 20
                bar = f'{'=' * int(percent * barLen)}{' ' * int(barLen - percent * barLen)}'
                print(f'{self.file} ({self.valid}/{self.invalid}/{self.valid + self.invalid}) -- [{bar}] {int(percent * 100)}%')

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

                        if (self.valid + self.invalid) % 100_000 == 0:    
                            _print()

                        if num != -1 and (self.valid + self.invalid) >= num:
                            _print()
                            return

                    buffer = lines[-1]

            _print()

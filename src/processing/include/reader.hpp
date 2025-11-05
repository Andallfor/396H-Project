#ifndef CMSC_READER_H
#define CMSC_READER_H

#include <string>
#include <stdio.h>
#include <iostream>
#include <generator>
#include <filesystem>
#include <exception>

#include <zstd.h>
#include <glaze/glaze.hpp>

#include "types.hpp"
#include "timing.hpp"

namespace fs = std::filesystem;
using time_point = std::chrono::system_clock::time_point;

struct Reader_Output {
    size_t fileSize = 0;
    std::string fileName = "";
    size_t numDesiredLines = 0;
    time_point startTime{};
    std::string fileSizeStr = "";
    
    size_t readSize = 0;
    size_t readLinesTotal = 0;
    size_t readLinesInvalid = 0;
    size_t readLinesFiltered = 0;
};

class Reader {
private:
    FILE* handle;
    char* in;
    char* out;

    size_t in_sz;
    size_t out_sz;

    ZSTD_DCtx* dctx;

    // use for printing progress bar
    size_t p_last_len = 0;

    Reader_Output stats{};

    const std::string p_sizes[4] = {"B", "KiB", "MiB", "GiB"};
    void bFmt(const size_t bytes, std::string& out) const {
        int s = 0;
        double count = bytes;
        while (count >= 1024 && s++ < 4) count /= 1024.0;
        out = std::format("{:.1f} {}", count, p_sizes[s]);
    }
public:
    Reader(const std::string& file, size_t numLines = 0) {
        handle = fopen(file.c_str(), "rb");
        if (!handle) {
            std::cerr << "Unknown file path " << file << std::endl;
            exit(1);
        }

        in_sz = ZSTD_DStreamInSize();
        out_sz = ZSTD_DStreamOutSize();

        in = new char[in_sz];
        out = new char[out_sz];

        dctx = ZSTD_createDCtx();

        stats.fileSize = fs::file_size(file);
        stats.startTime = Benchmark::timestamp();
        stats.fileName = fs::path(file).filename().string();
        stats.numDesiredLines = numLines;
        bFmt(stats.fileSize, stats.fileSizeStr);
    }

    ~Reader() { close(); }

    void close() {
        delete[] in;
        delete[] out;

        fclose(handle);
        ZSTD_freeDCtx(dctx);
    }

    template <TRedditData T>
    std::generator<const T&> decompress(const int update_rate, const size_t count, bool exitOnErr = true) {
        std::string buf = "";
        size_t read, str_i, str_base;
        glz::error_ctx err;
        T data{};

        ZSTD_inBuffer input = { in, read, 0 };
        ZSTD_outBuffer output = { out, out_sz, 0 };

        // https://github.com/facebook/zstd/blob/dev/examples/streaming_decompression.c
#ifdef BENCHMARK_ENABLED
        auto t_decompress = Benchmark::timestamp();
#endif
        while ((read = fread(in, 1, in_sz, handle))) {
            if (!read) break;
            stats.readSize += read;

            input.pos = 0;
            input.size = read;
            while (input.pos < input.size) {
                output.pos = 0;
                ZSTD_decompressStream(dctx, &output, &input);
#ifdef BENCHMARK_ENABLED
                Benchmark::sum("Decompress", t_decompress);
#endif
                str_i = 0;
                str_base = 0;
                for (; str_i < output.size; str_i++) {
                    if (out[str_i] == '\n') {
#ifdef BENCHMARK_ENABLED
                        auto t_json = Benchmark::timestamp();
#endif
                        data.reset();
                        if (str_base == 0 && buf.size() != 0) {
                            if (str_i != 0) buf += std::string(out, str_i);
                            err = glz::read<glz::opts{ .error_on_unknown_keys = false }>(data, buf);

                            if (exitOnErr && err) std::cout << std::endl << "Buffer error:" << std::endl << buf << std::endl;
                            buf.clear();
                        } else {
                            std::string_view str(out + str_base, str_i - str_base);
                            err = glz::read<glz::opts{ .error_on_unknown_keys = false }>(data, str);

                            if (exitOnErr && err) std::cout << std::endl << str << std::endl;
                        }

                        str_base = str_i + 1;
#ifdef BENCHMARK_ENABLED
                        Benchmark::sum("JSON", t_json);
#endif

                        if (err) {
                            if (exitOnErr) throw std::runtime_error(
                                std::format("(reader.hpp) Unable to read json! Glaze error code: {}", (uint32_t) err.ec));
                            stats.readLinesInvalid++;
                        } else {
                            if (data.valid()) co_yield data;
                            else stats.readLinesFiltered++;
                        }

                        if (stats.readLinesTotal++ % update_rate == 0) print();
                        if (count != 0 && stats.readLinesTotal >= count) goto end;
                    }
                }
#ifdef BENCHMARK_ENABLED
                t_decompress = Benchmark::timestamp();
#endif
                // chunks will have last entry cut off (without new line), so store and append to next
                // += because occasionally an entry will be so long that it spans multiple chunks
                buf += std::string(out + str_base, output.size - str_base);
            }
        }
end:
    }

    void print() {
        if (p_last_len != 0) std::cout << '\r';

        double percent = (stats.numDesiredLines == 0)
            ? (double) stats.readSize / (double) stats.fileSize
            : (double) stats.readLinesTotal / (double) stats.numDesiredLines;
        int index = 0;

        int barLen = 50;

        std::string r;
        bFmt(stats.readSize, r);

        std::string eta;
        if (percent == 1) eta = "Done";
        else if (percent == 0) eta = "Unknown";
        else Benchmark::tFmt((1.0 / percent - 1.0) * (double) Benchmark::elapsed(stats.startTime), eta);

        std::string s = std::format("{} -- ({}/{}/{} = {}/{}) -- [{}{}] {:.2f}% -- {}",
            stats.fileName,
            stats.readLinesTotal, stats.readLinesFiltered, stats.readLinesInvalid,
            r, stats.fileSizeStr,
            std::string((int) std::floor(percent * barLen), '='), std::string((int) std::ceil(barLen - percent * barLen), ' '),
            100.0 * percent,
            eta
        );
        std::cout << s;

        if (s.size() < p_last_len) std::cout << std::string(p_last_len - s.size(), ' ');
        p_last_len = s.size();

        std::cout << std::flush;
    }

    void print_end() {
        double t = (double) Benchmark::elapsed_ms(stats.startTime) / 1000.0;
        std::string size; bFmt(stats.readSize, size);
        std::string time; Benchmark::tFmt(t, time);
        double l = (double) stats.readLinesTotal / t;

        print();
        std::cout << std::format("\nSize read: {}\nTime elapsed: {}\nLines/s: {:.0f}\n", size, time, l);
        Benchmark::print();
    }

    const Reader_Output& status() const { return stats; }
};

#endif
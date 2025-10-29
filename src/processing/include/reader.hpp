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

class Reader {
private:
    FILE* handle;
    char* in;
    char* out;

    size_t in_sz;
    size_t out_sz;

    ZSTD_DCtx* dctx;

    size_t p_read = 0;
    double p_total = 0; 
    size_t p_lines = 0;
    size_t p_invalid_lines = 0;
    size_t p_last_len = 0;
    double p_total_lines = 0;
    time_point p_start;
    std::string p_total_str;
    std::string p_name;

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

        p_total = (double) fs::file_size(file);
        p_start = Benchmark::timestamp();
        p_name = fs::path(file).filename().string();
        p_total_lines = (double) numLines;
        bFmt(p_total, p_total_str);
    }

    ~Reader() { close(); }

    void close() {
        delete[] in;
        delete[] out;

        fclose(handle);
        ZSTD_freeDCtx(dctx);
    }

    template <typename T>
    std::generator<const T&> decompress(int update_rate, bool exitOnErr = true) {
        std::string buf; // chunks will have last entry cut off (without new line), so store and append to next

        // https://github.com/facebook/zstd/blob/dev/examples/streaming_decompression.c
        while (size_t read = fread(in, 1, in_sz, handle)) {
            if (!read) break;
            p_read += read;

            ZSTD_inBuffer input = { in, read, 0 };
            while (input.pos < input.size) {
#ifdef BENCHMARK_ENABLED
                auto t_decompress = Benchmark::timestamp();
#endif
                ZSTD_outBuffer output = { out, out_sz, 0 };

                const size_t ret = ZSTD_decompressStream(dctx, &output, &input);
                std::string str((const char*) out, output.size);
#ifdef BENCHMARK_ENABLED
                // note that this isnt the most accurate; we dont consider initial file reading or buffer allocation time
                Benchmark::sum("Decompress", t_decompress);
#endif
                int n;
                while ((n = str.find('\n')) != -1) {
                    std::string entry = str.substr(0, n);
                    str.erase(0, n + 1);

                    if (buf.size() != 0) {
                        entry = buf + entry;
                        buf.clear();
                    }

                    if (p_lines % update_rate == 0) print();
                    p_lines++;
#ifdef BENCHMARK_ENABLED
                    auto t_json = Benchmark::timestamp();
#endif
                    T data{}; // look into caching this and only writing into the same object
                    auto err = glz::read<glz::opts{
                        .error_on_unknown_keys = false,
                        .partial_read = true,
                    }>(data, entry);
#ifdef BENCHMARK_ENABLED
                    Benchmark::sum("JSON", t_json);
#endif
                    if (err) {
                        std::string s = "Unable to read line: " + std::to_string((uint32_t) err.ec);
                        if (exitOnErr) throw std::runtime_error(s);
                        std::cout << s << std::endl;
                        p_invalid_lines++;
                    } else co_yield data;
                }

                buf = str;
            }
        }
    }

    void print() {
        if (p_last_len != 0) std::cout << '\r';

        double percent = (p_total_lines == 0) ? (double) p_read / p_total : (double) p_lines / p_total_lines;
        int index = 0;

        int barLen = 50;
        std::stringstream ss;

        std::string r;
        bFmt(p_read, r);
        ss << p_name << " -- (" << p_lines << "/" << p_invalid_lines << ": " << r << "/" << p_total_str << ") -- [";
        ss << std::string((int) (percent * barLen), '=');
        ss << std::string((int) (barLen - percent * barLen), ' ') << "] ";
        ss << std::format("{:.2f}", 100.0 * percent) << "% -- ";

        std::string eta;
        if (percent == 1) eta = "Done";
        else if (percent == 0) eta = "Unknown";
        else Benchmark::tFmt((1.0 / percent - 1.0) * (double) Benchmark::elapsed(p_start), eta);

        ss << eta;

        std::string s = ss.str();
        std::cout << s;

        if (s.size() < p_last_len) std::cout << std::string(p_last_len - s.size(), ' ');
        p_last_len = s.size();

        std::cout << std::flush;
    }

    void print_end() {
        print();
        std::cout << std::endl;
        std::string s;

        bFmt(p_read, s);
        std::cout << "Size read: " << s << std::endl;

        s.clear();
        double t = (double) Benchmark::elapsed(p_start);
        Benchmark::tFmt(t, s);
        std::cout << "Time elapsed: " << s << std::endl;

        double l = (double) (p_lines - p_invalid_lines) / t;
        std::cout << "Lines/s: " << l << std::endl;

        std::cout << std::endl;
        Benchmark::print();
    }
};

#endif
#ifndef CMSC_TIMING_H
#define CMSC_TIMING_H

#include <unordered_map>
#include <string>
#include <chrono>
#include <iostream>

using time_point = std::chrono::system_clock::time_point;

class Benchmark {
private:
    static inline std::unordered_map<std::string, int64_t> time{};
    static const inline std::pair<std::string, int64_t> p_times[3] = {{"hour", 3600}, {"min", 60}, {"sec", 1}};
public:
    static void tFmt(int64_t sec, std::string& out) {
        if (sec < 1) out = "<1 sec";
        else {
            for (const auto& pair : p_times) {

                int64_t amt = sec / pair.second;
                sec %= pair.second;

                if (amt != 0) {
                    out += std::to_string(amt) + " " + pair.first;
                    if (amt != 1) out += "s";
                    if (pair.second != 1 && sec != 0) out += ", ";
                }
            }
        }
    }

    static time_point timestamp() { return floor<std::chrono::seconds>(std::chrono::system_clock::now()); }
    static int64_t elapsed(const time_point& t) { return floor<std::chrono::seconds>(std::chrono::system_clock::now() - t).count(); }

    static void sum(const std::string& key, int64_t t) {
        if (!time.count(key)) time[key] = t;
        else time[key] += t;
    }

    static void sum(const std::string& key, const time_point& t) { sum(key, elapsed(t)); }

    static void total(const std::string& key, std::string& out) {
        if (!time.count(key)) out = "NA";
        else tFmt(time[key], out);
    }

    static void print() {
        int64_t t = 0;
        for (const auto& pair : time) t += pair.second;

        for (const auto& pair : time) {
            std::string out;
            total(pair.first, out);
            std::cout << std::format("{}: {} ({:.1f}%)", pair.first, out, (double) (100 * pair.second) / (double) t) << std::endl;
        }
    }
};

#endif
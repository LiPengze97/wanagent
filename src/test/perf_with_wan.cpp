#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include <string.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>

#include <wan_agent.hpp>

using std::cout;
using std::endl;
using std::cerr;
using std::string;
using namespace wan_agent;

static inline uint64_t now_us() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return (tv.tv_sec * 1000000 + tv.tv_nsec / 1000);
}

static inline int Rand(int L, int R) {
    return rand() % (R - L + 1) + L;
}

static void print_help(const char* cmd) {
    std::cout << "Usage: " << cmd << " -c <json_config_file>"
              << " [-s(sender)]"
              << " [-i interval(micro)]"
              << " [-m message_size(in bytes)]"
              << " [-n number_of_messages]"
              << std::endl;
}

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_GRANULARITY_US (50)
#define MESSAGE_SIZE (1000)

const int MAXOPS = 1e5 + 100;

uint64_t w_send_time[MAXOPS] = {0};
uint64_t w_arrive_time[MAXOPS] = {0};

uint64_t r_send_time[MAXOPS] = {0};
uint64_t r_arrive_time[MAXOPS] = {0};

uint64_t tot_read_ops = 0;
uint64_t tot_write_ops = 0;

struct W {
    int seq;
    wan_agent::WriteRecvCallback C;
    W() {
        C = [&]() {
            w_arrive_time[seq] = now_us();
        };
    }
    void set_seq(int _seq) { seq = _seq; }
} wnodes[MAXOPS];

struct R {
    int seq;
    wan_agent::ReadRecvCallback C;
    R() {
        C = [&](const uint64_t version, const site_id_t site, Blob&& obj) {
            r_arrive_time[seq] = now_us();
        };
    }
    void set_seq(int _seq) { seq = _seq; }
} rnodes[MAXOPS];

inline void check_out(const int read_cnt, const int write_cnt, const string& trace) {
    uint64_t r_tot_wait_time = 0;
    uint64_t w_tot_wait_time = 0;
    for (int i = 1; i <= read_cnt; ++i) {
        assert(r_arrive_time[i] >= r_send_time[i]);
        r_tot_wait_time += (r_arrive_time[i] - r_send_time[i]);
    }
    for (int i = 1; i <= write_cnt; ++i) {
        assert(w_arrive_time[i] >= w_send_time[i]);
        w_tot_wait_time += (w_arrive_time[i] - w_send_time[i]);
    }
    long double r_mean_us = (long double)r_tot_wait_time/read_cnt;
    long double w_mean_us = (long double)w_tot_wait_time/write_cnt;
    long double r_mean_ms = (long double)r_mean_us/1000.0;
    long double w_mean_ms = (long double)w_mean_us/1000.0;

    long double w_std = 0;
    long double r_std = 0;
    for (int i = 1; i <= read_cnt; ++i) {
        long double dur = (r_arrive_time[i] - r_send_time[i])/1000.0;
        r_std += (dur - r_mean_ms) * (dur - r_mean_ms);
    }
    r_std /= (long double)(read_cnt - 1);
    r_std = sqrt(r_std);
    for (int i = 1; i <= write_cnt; ++i) {
        long double dur = (w_arrive_time[i] - w_send_time[i])/1000.0;
        w_std += (dur - w_mean_ms) * (dur - w_mean_ms);
    }
    w_std /= (long double)(write_cnt - 1);
    w_std = sqrt(w_std);

    long double tot_dur = std::max(r_arrive_time[read_cnt], w_arrive_time[write_cnt])
                        - std::min(w_send_time[1], r_send_time[1]);
    
    long double tot_bytes = MESSAGE_SIZE * 100000;
    long double tot_ops = 100000;

    long double thp_mibps = tot_bytes * 1000000/1048576/tot_dur;
    long double thp_ops = tot_ops * 1000000/tot_dur;
    
    std::cout << "Throughput (MiB/s): " << thp_mibps << std::endl;
    std::cout << "Throughput (Ops/s): " << thp_ops << std::endl;
    std::cout << "Average Read Latency = " << r_mean_us << "(us) " 
              << r_mean_ms << "(ms)" << endl;
    std::cout << "Average Write Latency = " << w_mean_us << "(us) " 
              << w_mean_ms << "(ms)" << endl;
    std::cout << "Std of read latency = " << r_std << endl;
    std::cout << "Std of write latency = " << w_std << endl;

    freopen((trace+".log").c_str(), "w", stdout);
    std::cout << "Throughput (MiB/s): " << thp_mibps << std::endl;
    std::cout << "Throughput (Ops/s): " << thp_ops << std::endl;
    std::cout << "Average Read Latency = " << r_mean_us << "(us) " 
              << r_mean_ms << "(ms)" << endl;
    std::cout << "Average Write Latency = " << w_mean_us << "(us) " 
              << w_mean_ms << "(ms)" << endl;
    std::cout << "Std of read latency = " << r_std << endl;
    std::cout << "Std of write latency = " << w_std << endl;
}

int main(int argc, char** argv) {
    srand(time(0));
    int opt;
    std::string json_config;
    std::string trace_name = "";

    int num_load = 0;
    int expected_mps = 200;
    while((opt = getopt(argc, argv, "c:t:n:p:")) != -1) {
        switch(opt) {
            case 'c':
                json_config = optarg;
                break;
            case 't':
                trace_name = optarg;
                break;
            case 'n':
                num_load = static_cast<int>(std::stoi(optarg));
                break;
            case 'p':
                expected_mps = static_cast<int>(std::stoi(optarg));
                break;
            default:
                std::cerr << "please enter config and trace" << std::endl;
                return -1;
        }
    }

    if(json_config.size() == 0) {
        std::cerr << "something wrong with the json file" << std::endl;
        return -1;
    }

    int number_of_messages = 100000;

    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    uint64_t* time_keeper = nullptr;
    std::atomic<bool> all_received(false);

    std::cerr << "trace name = " << trace_name << std::endl;

    time_keeper = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * 4 * number_of_messages));
    if(time_keeper == nullptr) {
        throw std::runtime_error("failed to allocate memory for time keeper");
    } else {
        // touch the memory
        bzero(static_cast<void*>(time_keeper), sizeof(uint64_t) * 4 * number_of_messages);
    }

    wan_agent::PredicateLambda pl = [&](const std::map<uint32_t, uint64_t>& table) {
        if(time_keeper != nullptr) {
            uint64_t min, max, median;
            std::vector<uint64_t> v;
            for(auto& site : conf[WAN_AGENT_CONF_SERVER_SITES]) {
                uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
                v.push_back(table.at(sid));
            }
            std::sort(v.begin(), v.end());
            min = v[0];
            median = v[v.size() / 2];
            max = v[v.size() - 1];
            uint64_t now = now_us();
            // global
            while(min > 0 && time_keeper[min * 4 - 1] == 0) {
                time_keeper[min * 4 - 1] = now;
                min--;
            }
            // majority
            while(median > 0 && time_keeper[median * 4 - 2] == 0) {
                time_keeper[median * 4 - 2] = now;
                median--;
            }
            // one
            while(max > 0 && time_keeper[max * 4 - 3] == 0) {
                time_keeper[max * 4 - 3] = now;
                max--;
            }
        }
        if(time_keeper[number_of_messages * 4 - 1] != 0) {
            all_received.store(true);
        }
    };

    wan_agent::WanAgentSender wan_agent_sender(conf, pl);


    for (int i = 0; i <= 100000; ++i) {
        wnodes[i].set_seq(i);
        rnodes[i].set_seq(i);
    }
    std::cerr << "Press ENTER" << std::endl;
    std::cin.get();

    std::ifstream L_fin(("../../../../"+trace_name+".load").c_str());
    std::ifstream T_fin(("../../../../"+trace_name+".trans").c_str());
    string tmp = "";
    string obj = "";
    for (int i = 1; i <= MESSAGE_SIZE; ++i) obj += 'a';
    int load_ctr = 0;
    // std::cerr << "Starting loading ..." << std::endl;
    // while (L_fin >> tmp) {
    //     ++load_ctr;
    //     if (load_ctr % 1000 == 0) cerr << load_ctr << endl;
        
    //     if (load_ctr == num_load) {
    //         wan_agent_sender.send_write_req(obj.c_str(), obj.size(), &wnodes[0].C);
    //         while (!w_arrive_time[0]) {}
    //     }
    //     else {
    //         wan_agent_sender.send_write_req(obj.c_str(), obj.size(), nullptr);
    //     }
    // }
    // std::cerr << "Load " << load_ctr << " operations" << std::endl;
    std::cerr << "Press enter to start transactions" << std::endl;
    std::cin.get();

    string ops = "";
    uint64_t version = uint64_t(-1);

    int read_ctr = 0, write_ctr = 0;
    int trans_ctr = 0;
    uint64_t start_time = now_us();
    uint64_t now_time;
    while (T_fin >> ops) {
        ++trans_ctr;
        if (trans_ctr % 1000 == 0) cerr << trans_ctr << endl;
        now_time = now_us();
        while ((now_time - start_time)/1000000.0*expected_mps < (trans_ctr - 1)) {
            std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_GRANULARITY_US));
            now_time = now_us();
        }
        if (ops[0] == 'R') {
            T_fin >> version;
            ++read_ctr;
            r_send_time[read_ctr] = now_us();
            wan_agent_sender.send_read_req(&rnodes[read_ctr].C);
        } else {
            T_fin >> tmp;
            ++write_ctr;
            w_send_time[write_ctr] = now_us();
            wan_agent_sender.send_write_req(obj.c_str(), obj.size(), &wnodes[write_ctr].C);
        }
    }
    std::cerr << write_ctr + read_ctr << std::endl;
    while ((write_ctr && !w_arrive_time[write_ctr]) || (read_ctr && !r_arrive_time[read_ctr])) {}

    check_out(read_ctr, write_ctr, trace_name);

    std::cerr << "Press ENTER to kill." << std::endl;
    std::cin.get();
    wan_agent_sender.shutdown_and_wait();
    free(time_keeper);
}
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

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_GRANULARITY_US (50)
const int MAXOPS = 1e5 + 100;

uint64_t w_send_time[MAXOPS] = {0};
uint64_t w_arrive_time[MAXOPS] = {0};

uint64_t r_send_time[MAXOPS] = {0};
uint64_t r_arrive_time[MAXOPS] = {0};

uint64_t tot_read_ops = 0;
uint64_t tot_write_ops = 0;

int MESSAGE_SIZE = 8000;
int n_message = 0;

inline void check_out(const int read_cnt, const int write_cnt, string pf, bool SWI) {
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

    uint64_t mx_time = 0;
    if (read_cnt) mx_time = r_arrive_time[read_cnt];
    if (write_cnt) mx_time = std::max(mx_time, w_arrive_time[write_cnt]);
    uint64_t mn_time = (uint64_t)-1;
    if (read_cnt) mn_time = r_send_time[1];
    if (write_cnt) mn_time = std::min(mx_time, w_send_time[1]);

    long double tot_dur = mx_time - mn_time;
    
    long double tot_bytes = MESSAGE_SIZE * n_message;
    long double tot_ops = 100000;

    long double thp_mibps = tot_bytes * 1000000/1048576/tot_dur;
    long double thp_ops = tot_ops * 1000000/tot_dur;

    std::cerr << "--------- " << pf << " ---------" << std::endl;    
    std::cerr << "Throughput (MiB/s): " << thp_mibps << std::endl;
    std::cerr << "Throughput (Ops/s): " << thp_ops << std::endl;
    std::cerr << "Average Read Latency = " << r_mean_us << "(us) " 
              << r_mean_ms << "(ms)" << endl;
    std::cerr << "Average Write Latency = " << w_mean_us << "(us) " 
              << w_mean_ms << "(ms)" << endl;
    std::cerr << "Std of read latency = " << r_std << endl;
    std::cerr << "Std of write latency = " << w_std << endl;
    if (SWI) {
        std::cout << thp_mibps;
    }
    else {
        std::cout << r_mean_us;
    }
}

int main(int argc, char** argv) {
    srand(time(0));
    int opt;
    std::string json_config;

    int expected_mps = 200;
    bool SWI  = 0;
    
    while((opt = getopt(argc, argv, "c:n:p:s:m:")) != -1) {
        switch(opt) {
            case 'c':
                json_config = optarg;
                break;
            case 'n':
                n_message = static_cast<int>(std::stoi(optarg));
                break;
            case 'p':
                expected_mps = static_cast<int>(std::stoi(optarg));
                break;
            case 'm':
                MESSAGE_SIZE = static_cast<int>(std::stoi(optarg));
                break;
            case 's':
                SWI = static_cast<bool>(std::stoi(optarg));
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

    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    uint64_t* time_keeper = nullptr;
    std::atomic<bool> all_received(false);

    time_keeper = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * 4 * n_message));
    if(time_keeper == nullptr) {
        throw std::runtime_error("failed to allocate memory for time keeper");
    } else {
        // touch the memory
        bzero(static_cast<void*>(time_keeper), sizeof(uint64_t) * 4 * n_message);
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
        if(time_keeper[n_message * 4 - 1] != 0) {
            all_received.store(true);
        }
    };
    wan_agent::WanAgentSender wan_agent_sender(conf, pl);

    string obj = "";
    for (int i = 1; i <= MESSAGE_SIZE; ++i) obj += 'a';

    std::string w_pr[4] = {
        "MIN($1,$2,$3,$4)",
        "MAX($1,$2,$3,$4)",
        "KTH_MIN($2,$1,$2,$3,$4)",
        "KTH_MIN($2,MAX($1,$2),$3,$4)",
    };

    std::string w_name[4] = {
        "w_all",
        "w_sig",
        "w_maj",
        "w_maj_reg",
    };

    std::string r_pr[4] = {
        "MIN($1,$2,$3,$4)",
        "MAX($1,$2,$3,$4)",
        "KTH_MIN($3,$1,$2,$3,$4)",
        "KTH_MIN($2,MIN($1,$2),$3,$4)",
    };

    std::string r_name[4] = {
        "r_sig",
        "r_all",
        "r_maj",
        "r_maj_reg",
    };
    for (SWI = 0; SWI <= 1; ++SWI) {
        std::cerr << "TESTING ON " << (SWI ? "WRITE" : "READ") << std::endl;
        if (!SWI) {
            wan_agent_sender.send_write_req(obj.c_str(), obj.size(), nullptr);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (SWI) freopen("write.log", "w", stdout);
        else freopen("read.log", "w", stdout);

        for (int T = 0; T < 4; ++T) {
            std::cerr << "TEST CASE = " << T << std::endl;
            int st = (SWI ? 2000 : 500);
            int ed = (SWI ? 10000 : 1100);
            int dt = (SWI ? 2000 : 200);
            for (int parm = st; parm <= ed; parm += dt) {
                (SWI ? MESSAGE_SIZE = parm : MESSAGE_SIZE = 6000);
                (SWI ? expected_mps = (int)1e7 : expected_mps = parm);
                std::atomic<int> write_recv_cnt = 0;
                std::atomic<int> read_recv_cnt = 0;
                wan_agent::WriteRecvCallback WRC = [&]() {
                    w_arrive_time[++write_recv_cnt] = now_us();
                };
                wan_agent::ReadRecvCallback RRC = [&](const uint64_t version, const site_id_t site, Blob&& obj) {
                    r_arrive_time[++read_recv_cnt] = now_us();
                };
                std::cerr << "TESTING on predicate :" << (SWI ? w_name[T] : r_name[T]) << std::endl;
                wan_agent_sender.submit_predicate("auto_test"+std::to_string(T), SWI ? w_pr[T] : r_pr[T], 1);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                int read_ctr = 0, write_ctr = 0;
                uint64_t start_time = now_us();
                uint64_t now_time;
                for (int i = 1; i <= n_message; ++i) {
                    if (i % 5000 == 0) cerr << i << endl;
                    now_time = now_us();
                    while ((now_time - start_time)/1000000.0*expected_mps < (i - 1)) {
                        std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_GRANULARITY_US));
                        now_time = now_us();
                    }
                    if (SWI) {
                        ++write_ctr;
                        w_send_time[write_ctr] = now_us();
                        wan_agent_sender.send_write_req(obj.c_str(), obj.size(), &WRC);
                    } else {
                        ++read_ctr;
                        r_send_time[read_ctr] = now_us();
                        wan_agent_sender.send_read_req(&RRC);
                    }
                }
                while ((write_ctr != write_recv_cnt) || (read_ctr != read_recv_cnt)) {}
                check_out(read_ctr, write_ctr, SWI ? w_name[T] : r_name[T], SWI);
                wan_agent_sender.wait();
            }
        }
        fclose(stdout);
    }
//    std::cerr << "hi" << std::endl;
//    std::this_thread::sleep_for(std::chrono::seconds(1));
//    wan_agent_sender.shutdown_and_wait();
    free(time_keeper);
}
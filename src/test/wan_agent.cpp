#include <wan_agent.hpp>
#include <nlohmann/json.hpp>
#include <list>
#include <iostream>
#include <map>
#include <mutex>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using namespace wan_agent;
using namespace persistent;

static inline uint64_t now_us() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return (tv.tv_sec * 1000000 + tv.tv_nsec / 1000);
}

static inline int Rand(int L, int R) {
    return rand() % (R - L + 1) + L;
}

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_US (10)
const int MAXOPS = 1e5 + 100;

uint64_t w_send_time[MAXOPS] = {0};
uint64_t w_arrive_time[MAXOPS] = {0};

uint64_t r_send_time[MAXOPS] = {0};
uint64_t r_arrive_time[MAXOPS] = {0};

uint64_t tot_read_ops = 0;
uint64_t tot_write_ops = 0;

int MESSAGE_SIZE = 8000;
int n_message = 0;

inline void check_out(const int read_cnt, const int write_cnt, string pf, int SWI) {
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
    if (write_cnt) mn_time = std::min(mn_time, w_send_time[1]);

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
        std::cout << r_mean_ms;
    }
}

static void print_help(const char *cmd)
{
    std::cout << "Usage: " << cmd << " -c <json_config_file>"
              << " [-s(sender)]"
              << " [-i interval(micro)]"
              << " [-m message_size(in bytes)]"
              << " [-n number_of_messages]"
              << std::endl;
}

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_GRANULARITY_US (50)
std::mutex all_lock;
std::map<uint64_t, version_t> seq_versions;
uint64_t max_version = 0;

int main(int argc, char **argv)
{
    int opt;
    bool is_sender = false;
    std::string json_config;
    std::size_t message_size = 0;
    std::size_t number_of_messages = 0;
    std::size_t expected_mps = 200;

    while ((opt = getopt(argc, argv, "c:s:i:m:n:p:")) != -1)
    {
        switch (opt)
        {
        case 'c':
            json_config = optarg;
            break;
        case 's':
            is_sender = true;
            break;
        case 'm':
            message_size = static_cast<std::size_t>(std::stol(optarg));
            break;
        case 'n':
            number_of_messages = static_cast<std::size_t>(std::stol(optarg));
            break;
        case 'p':
            expected_mps = static_cast<std::size_t>(std::stol(optarg));
            break;
        default:
            print_help(argv[0]);
            return -1;
        }
    }
    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    // for sender
    uint64_t *time_keeper = nullptr;
    std::atomic<bool> all_received(false);

    if (is_sender)
    {
        std::cout << "number_of_messages = " << number_of_messages << std::endl;
        std::cout << "message_size = " << message_size << std::endl;
        if (number_of_messages <= 0 || message_size <= 0)
        {
            std::cerr << "invalid argument." << std::endl;
            return -1;
        }

        // prepare time keepers
        time_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * 4 * number_of_messages));
        if (time_keeper == nullptr)
        {
            throw std::runtime_error("failed to allocate memory for time keeper");
        }
        else
        {
            // touch the memory
            bzero(static_cast<void *>(time_keeper), sizeof(uint64_t) * 4 * number_of_messages);
        }

        std::cout << "time_keeper:" << time_keeper << std::endl;
    }

    auto max_payload_size = conf[WAN_AGENT_MAX_PAYLOAD_SIZE];
    persistent::PersistentRegistry pr(nullptr, typeid(Blob), 0, 0);
    persistent::Persistent<Blob> pblob([max_payload_size]() { return std::make_unique<Blob>(nullptr, max_payload_size); },
                                       "Pblob",
                                       &pr,
                                       false);
    Blob latest_blob;
    std::atomic<int> ops_ctr = 0;
    std::atomic<int> len = 0;
    string obj = "";
    uint64_t max_rec_version = 0;
    for (int i = 1; i <= 5000; ++i) obj += 'a';
    
    wan_agent::RemoteMessageCallback rmc = [&](const RequestHeader &RH, const char *msg) {
        // cout << "message received from site:" << RH.site_id
        //      << ", message size:" << RH.payload_size << " bytes"
        //      << ", message version:" << RH.version
        //      << endl;
        if (RH.requestType == 1)
        {
            // version_t prev_version = pblob.getLatestVersion();
            // version_t cur_version = prev_version + 1;
            // cerr << "cur_version = " << cur_version << endl;
            // (*pblob) = std::move(Blob(msg, RH.payload_size));
            latest_blob = std::move(Blob(msg, RH.payload_size));
                
            // pblob.version(cur_version);
            // seq_versions[RH.version] = cur_version;
            // assert(max_version < RH.version);
            max_version = std::max(RH.version, max_version);
            // cout << "message received from site:" << RH.site_id
            // << ", message size:" << RH.payload_size << ", bytes"
            //  << ", message version:" << RH.version << ", max version: " << max_version
            //  << endl;
            // ++ops_ctr;
            // if (ops_ctr % 5000 == 0) std::cerr << ops_ctr << std::endl;
            // for (int i = 0; i < RH.payload_size; ++i) {
            //     obj[i] = 'a';
            // }
            // obj[RH.payload_size] = '\0';
            // len = RH.payload_size;
            // max_version = RH.version;
            // pblob.persist(cur_version);
            return std::make_pair(RH.version, std::move(Blob("done", 4)));
        }
        else
        {
            ++ops_ctr;
            if (ops_ctr % 5000 == 0) std::cerr << ops_ctr << std::endl;
            // if (RH.version == (uint64_t)-1) {
            //    auto cur_version = max_version;
            //    return std::make_pair(cur_version, std::move(*(pblob).get(cur_version)));
            // } else if (seq_versions.find(RH.version) == seq_versions.end()) {
            //    return std::make_pair((uint64_t)-1, std::move(Blob("OBJ_NOT_FOUND",13)));
            // }
            // uint64_t cur_version = seq_versions[RH.version];
            // return std::make_pair(RH.version, std::move(*(pblob.get(cur_version))));

            //** pseudo-version 12345, because pure WANAgent does not have data
            // long unsigned int tmp_version = 12345;
            // cout << "blob size: " << latest_blob.size << std::endl;
            return std::make_pair(max_version, latest_blob);
            // return std::make_pair(tmp_version, std::move(Blob(obj.c_str(), len)));
            // return std::make_pair(RH.version, std::move(Blob(obj.c_str(), len)));
        }
    };
    wan_agent::PredicateLambda pl = [&](const std::map<uint32_t, uint64_t> &table) {
        if (time_keeper != nullptr)
        {
            uint64_t min, max, median;
            std::vector<uint64_t> v;
            for (auto &site : conf[WAN_AGENT_CONF_SERVER_SITES])
            {
                uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
                v.push_back(table.at(sid));
            }
            std::sort(v.begin(), v.end());
            min = v[0];
            median = v[v.size() / 2];
            max = v[v.size() - 1];
            uint64_t now = now_us();
            // global
            while (min > 0 && time_keeper[min * 4 - 1] == 0)
            {
                time_keeper[min * 4 - 1] = now;
                min--;
            }
            // majority
            while (median > 0 && time_keeper[median * 4 - 2] == 0)
            {
                time_keeper[median * 4 - 2] = now;
                median--;
            }
            // one
            while (max > 0 && time_keeper[max * 4 - 3] == 0)
            {
                time_keeper[max * 4 - 3] = now;
                max--;
            }
        }
        if (time_keeper[number_of_messages * 4 - 1] != 0)
        {
            all_received.store(true);
        }
    };
    wan_agent::WanAgent wanagent(conf, pl, rmc);

    // simple test
    if(is_sender){
        std::cout << "expected_mps " << expected_mps << std::endl;
        std::cout << "enter to send " << std::endl;
        std::cin.get();
        string send_content = "";
        for (int i = 1; i <= message_size; ++i) send_content += 'a';
        std::atomic<int> write_recv_cnt = 0;
        std::atomic<int> read_recv_cnt = 0;
        wan_agent::WriteRecvCallback WRC = [&](const uint64_t stability_frontier) {
            w_arrive_time[stability_frontier-1] = now_us();
        };
        wan_agent::ReadRecvCallback RRC = [&](const uint64_t version, Blob&& obj) {
            r_arrive_time[++read_recv_cnt] = now_us();
            // std::cout << "receive read with version " << version <<" !!" << std::endl;
        };
        uint64_t start_time = now_us();
        uint64_t now_time;
        for (int i = 1; i <= number_of_messages; ++i){
            now_time = now_us();
            while ((now_time - start_time)/1000000.0*expected_mps < (i - 1)) {
                std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_US));
                now_time = now_us();
            }
            wanagent.wansender->send_write_req(send_content.c_str(), send_content.size(), &WRC);
            latest_blob = Blob(send_content.c_str(), send_content.size());
        }
        std::cout << "done!" << std::endl;
        // std::this_thread::sleep_for(std::chrono::seconds(5));
        // for (int i = 1; i <= 30; ++i)
        //     wanagent.wansender->send_read_req(&RRC);
    }

    // complete test
    /*
    if (is_sender)
    {
            std::atomic<int> write_recv_cnt = 0;
            std::atomic<int> read_recv_cnt = 0;
            wan_agent::WriteRecvCallback WRC = [&]() {
                w_arrive_time[++write_recv_cnt] = now_us();
            };
            wan_agent::ReadRecvCallback RRC = [&](const uint64_t version, Blob&& obj) {
                r_arrive_time[++read_recv_cnt] = now_us();
                std::cout << "receive read with version " << version <<" !!";
            };
            std::cout << "Press ENTER to send start the experiment." << std::endl;
            std::cin.get();

            for (SWI = 0; SWI <= 1; ++SWI) {
            std::cerr << "TESTING ON " << (SWI ? "WRITE" : "READ") << std::endl;
            if (!SWI) {
                //warm up
                for (int i = 1; i <= 1000; ++i)
                    wanagent.wansender->send_write_req(obj.c_str(), obj.size(), nullptr);
                std::this_thread::sleep_for(std::chrono::seconds(5));
                for (int i = 1; i <= 1000; ++i)
                    wanagent.wansender->send_read_req(nullptr);
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }

            if (SWI) freopen("write.log", "w", stdout);
            else freopen("read.log", "w", stdout);

            for (int T = 0; T < 4; ++T) {
                std::cerr << "TEST CASE = " << T << std::endl;
                int st = (SWI ? 2000 : 800);
                int ed = (SWI ? 10000 : 1400);
                int dt = (SWI ? 2000 : 200);
                std::cout << T << ' ';
                for (int parm = st; parm <= ed; parm += dt) {
                    (SWI ? MESSAGE_SIZE = parm : MESSAGE_SIZE = 5000);
                    (SWI ? expected_mps = (int)1e6 : expected_mps = parm);
                    obj = "";
                    for (int i = 1; i <= MESSAGE_SIZE; ++i) obj += 'a';
                    std::atomic<int> write_recv_cnt = 0;
                    std::atomic<int> read_recv_cnt = 0;
                    wan_agent::WriteRecvCallback WRC = [&]() {
                        w_arrive_time[++write_recv_cnt] = now_us();
                    };
                    wan_agent::ReadRecvCallback RRC = [&](const uint64_t version, Blob&& obj) {
                        r_arrive_time[++read_recv_cnt] = now_us();
                    };
                    std::cerr << "TESTING on predicate :" << (SWI ? w_name[T] : r_name[T]) << std::endl;
                    wanagent.wansender->submit_predicate("auto_test"+std::to_string(T), SWI ? w_pr[T] : r_pr[T], 1);
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
                            wanagent.wansender->send_write_req(obj.c_str(), obj.size(), &WRC);
                        } else {
                            ++read_ctr;
                            r_send_time[read_ctr] = now_us();
                            wanagent.wansender->send_read_req(&RRC);
                        }
                    }
                    while ((write_ctr != write_recv_cnt) || (read_ctr != read_recv_cnt)) {}
                    check_out(read_ctr, write_ctr, SWI ? w_name[T] : r_name[T], SWI);
                    std::cout << ' ';
                    wanagent.wansender->wait();
                }
                std::cout << endl;
            }
            fclose(stdout);
        }
        std::cout << "Done send messages." << std::endl;
        std::cout << "Press ENTER to kill." << std::endl;
        std::cin.get();
        
        wanagent.shutdown_and_wait();
        free(time_keeper);
    }
    */
    std::cout << "Press ENTER to kill." << std::endl;
    std::cin.get();
    wanagent.shutdown_and_wait();
    
    return 0;
}
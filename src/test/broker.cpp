#include <wan_agent.hpp>
#include <nlohmann/json.hpp>
#include <list>
#include <iostream>
#include <map>
#include <mutex>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <wan_agent_utils.hpp>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

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


// broker epoll staff

class Broker{
private:
    uint64_t max_version = 0;
    std::string json_config;
    nlohmann::json conf;
    Blob latest_blob;
    std::vector<int> subscribers;
public:
    wan_agent::WanAgent wanagent;   
    int server_socket;
    std::list<std::thread> worker_threads;
    const int epoll_max_events = 64;
    Broker(std::string json_config):json_config(json_config){
        std::ifstream json_file(json_config);
        json_file >> conf;

        sockaddr_in serv_addr;
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if(fd < 0)
        throw std::runtime_error("Broker failed to create socket.");
        int reuse_addr = 1;
        if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse_addr,
                        sizeof(reuse_addr)) < 0) {
            fprintf(stderr, "ERROR on setsockopt: %s\n", strerror(errno));
        }

        int flag = 1;
        int ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));

        if(ret == -1) {
        fprintf(stderr, "ERROR on setsockopt: %s\n", strerror(errno));
        exit(-1);
        }

        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(38000);
        if(bind(fd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr, "ERROR on binding to socket: %s\n", strerror(errno));
            throw std::runtime_error("Broker failed to bind socket.");
        }
        listen(fd, 5);
        server_socket = fd;
        std::cout << "Broker listening on " << 38000 << std::endl;
        init_wanagent();
    }

    void init_wanagent(){
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
            if (RH.requestType == 1)
            {
                std::cout << "receive a message!!\n";
                latest_blob = std::move(Blob(msg, RH.payload_size));
                max_version = std::max(RH.version, max_version);
                for(int i = 0; i < subscribers.size(); i++){
                    bool success = sock_write(subscribers[i], Response{RH.payload_size, RH.version, RH.seq, RH.site_id});
                    if(!success)
                        throw std::runtime_error("Failed to send ACK message");
                }

                return std::make_pair(RH.version, std::move(Blob("done", 4)));
            }
            else
            {
                ++ops_ctr;
                if (ops_ctr % 5000 == 0) std::cerr << ops_ctr << std::endl;
                return std::make_pair(max_version, latest_blob);
            }
        };
        wan_agent::PredicateLambda pl = [&](const std::map<uint32_t, uint64_t> &table) {};
        wanagent = wan_agent::WanAgent(conf, pl, rmc);
    }

    void establish_connection(){
        // auto num_fd = (num_senders << 1);
        auto num_fd = 1;
        std::cout << "try estabilish connection\n";
        while(worker_threads.size() < num_fd) {
            struct sockaddr_storage client_addr_info;
            socklen_t len = sizeof client_addr_info;
            int connected_sock_fd = ::accept(server_socket, (struct sockaddr*)&client_addr_info, &len);
            std::cout << connected_sock_fd <<"\n";
            subscribers.push_back(connected_sock_fd);
            worker_threads.emplace_back(std::thread(&Broker::epoll_worker, this, connected_sock_fd));
        }
    }

    void epoll_worker(int connected_sock_fd) {
        wan_agent::WriteRecvCallback WRC = [&](const uint64_t stability_frontier) {
            // w_arrive_time[stability_frontier-1] = now_us();
        };
        RequestHeader header;
        int max_payload_size = 102400;
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(max_payload_size);
        bool success;
        std::cout << "epoll_worker start\n";
        int epoll_fd_recv_msg = epoll_create1(0);
        if(epoll_fd_recv_msg == -1)
            throw std::runtime_error("failed to create epoll fd");
        add_epoll(epoll_fd_recv_msg, EPOLLIN, connected_sock_fd);
        struct epoll_event events[epoll_max_events];
        while(true) {
            int n = epoll_wait(epoll_fd_recv_msg, events, epoll_max_events, -1);
            for(int i = 0; i < n; i++) {
                if(events[i].events & EPOLLIN) {
                    success = sock_read(connected_sock_fd, header);
                    if(!success) {
                        std::cout << "Failed to read request header, "
                                << "receive " << n << " messages from sender.\n";
                        throw std::runtime_error("Failed to read request header");
                    }
                    // std::cout << "header.payload_size " << header.payload_size << "\n";
                    success = sock_read(connected_sock_fd, buffer.get(), header.payload_size);
                    wanagent.wansender->send_write_req(buffer.get(), header.payload_size, &WRC);
                    latest_blob = Blob(buffer.get(), header.payload_size);
                    if(!success)
                        throw std::runtime_error("Failed to receive object");
                    
                    // success = sock_write(connected_sock_fd, Response{version_obj.second.size, version_obj.first, header.seq, local_site_id});
                    // if(!success)
                    //     throw std::runtime_error("Failed to send ACK message");
                }
            }
        }
    }
};


int main(int argc, char **argv)
{

    int opt;
    std::string json_config;

    while ((opt = getopt(argc, argv, "c:")) != -1)
    {
        switch (opt)
        {
        case 'c':
            json_config = optarg;
            break;
        default:
            print_help(argv[0]);
            return -1;
        }
    }
    Broker broker(json_config);
    std::thread rms_establish_thread(&Broker::establish_connection, &broker);
    rms_establish_thread.detach();
    std::cout << "Press ENTER to kill." << std::endl;
    std::cin.get();
    broker.wanagent.shutdown_and_wait();
    return 0;
}
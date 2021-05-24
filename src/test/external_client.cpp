#include <nlohmann/json.hpp>
#include <list>
#include <iostream>
#include <map>
#include <mutex>
#include <wan_agent_utils.hpp>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <wan_agent.hpp>
#include <string>

using wan_agent::Response;
using wan_agent::RequestHeader;

static inline uint64_t now_us() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return (tv.tv_sec * 1000000 + tv.tv_nsec / 1000);
}

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_US (10)
#define MAXOPS 30000

uint64_t w_send_time[MAXOPS] = {0};
uint64_t w_arrive_time[MAXOPS] = {0};

bool all_shutdown = false;
const int epoll_max_events = 64;
int epoll_fd_send_msg;
int epoll_fd_recv_ack;

void recv_ack_loop() {
    auto tid = pthread_self();
    std::cout << "recv_ack_loop start, tid = " << tid << std::endl;
    struct epoll_event events[EPOLL_MAXEVENTS];
    while(!all_shutdown) {
        int n = epoll_wait(epoll_fd_recv_ack, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLIN) {
                // received ACK
                Response res;
                std::cout << "receive a publish message!" << res.version << std::endl;
                auto success = sock_read(events[i].data.fd, res);
                if (!success) {
                    throw std::runtime_error("failed receiving ACK message");
                }
            }
        }
    }
}


void send_msg_loop() {
    auto tid = pthread_self();
    struct epoll_event events[EPOLL_MAXEVENTS];
    while(!all_shutdown) {
        // has item on the queue to send
        int n = epoll_wait(epoll_fd_send_msg, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLOUT) {
                // socket send buffer is available to send message
                // sock_write(events[i].data.fd, RequestHeader{requestType, version, version, local_site_id, payload_size});
                // sock_write(events[i].data.fd, node.message_body, payload_size);
            }
        }
    }
}


static void print_help(const char *cmd)
{
    std::cout << "Usage: " << cmd 
              << " [-p send_rate]"
              << " [-m message_size(in bytes)]"
              << " [-n number_of_messages]"
              << std::endl;
}

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
    if (number_of_messages == 0 || message_size == 0){
        std::cout << "wrong parameters!\n";
        print_help(argv[0]);
        return 0;
    }
    std::cout << "expected mps: " << expected_mps << ", message size: " << message_size << ", number of messages: " << number_of_messages << "\n";
    std::pair<std::string, uint16_t> ip_port = std::make_pair("127.0.0.1", 38000);
    epoll_fd_send_msg = epoll_create1(0);
    if(epoll_fd_send_msg == -1)
        throw std::runtime_error("failed to create epoll fd");
    epoll_fd_recv_ack = epoll_create1(0);
    if(epoll_fd_recv_ack == -1)
        throw std::runtime_error("failed to create epoll fd");
    sockaddr_in serv_addr;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0)
        throw std::runtime_error("MessageSender failed to create socket.");
    int flag = 1;
    int ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if(ret == -1) {
        fprintf(stderr, "ERROR on setsockopt: %s\n", strerror(errno));
        exit(-1);
    }
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(ip_port.second);
    inet_pton(AF_INET, ip_port.first.c_str(), &serv_addr.sin_addr);
    if(connect(fd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        throw std::runtime_error("MessageSender failed to connect socket");
    }
    std::cout << "new fd " << fd << std::endl;
    add_epoll(epoll_fd_send_msg, EPOLLOUT, fd);
    add_epoll(epoll_fd_recv_ack, EPOLLIN, fd);
    auto recv_loop = std::thread(recv_ack_loop);
    recv_loop.detach();
    std::string send_content = "";
    for (int i = 1; i <= message_size; ++i) send_content += 'a';
    struct epoll_event events[EPOLL_MAXEVENTS];
    std::cout << "press enter to start" << std::endl;
    std::cin.get();
    uint64_t start_time = now_us();
    uint64_t now_time;
    for (int i = 1; i <= number_of_messages; ++i){
        int n = epoll_wait(epoll_fd_send_msg, events, EPOLL_MAXEVENTS, -1);
        for(int j = 0; j < n; j++) {
            now_time = now_us();
            while ((now_time - start_time)/1000000.0*expected_mps < (i - 1)) {
                std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_US));
                now_time = now_us();
            }
            w_send_time[i] = now_us();
            sock_write(events[j].data.fd, RequestHeader{1, 2, 3, 4, message_size});
            sock_write(events[j].data.fd, send_content.c_str(), send_content.size());
        }
    }
    std::string file_name = "./" + std::to_string(expected_mps) + "_publish_time.csv";
    std::ofstream file(file_name);
    if(file) {
        file << "enter_time\n";
        for(int i = 1; i <= number_of_messages; i++) {
            file << w_send_time[i] << "\n";
        }
    }
    file.close();
    while(true);
    return 0;
}
#pragma once
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/HLC.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/openssl/signature.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <list>
#include <future>
#include <map>
#include <memory>
#include <queue>
#include <mutex>
#include <nlohmann/json.hpp>
#include <thread>
#include <utility>
#include <wan_agent_type_definitions.hpp>
#include <wan_agent_object.hpp>
#include <wan_predicate_reverser.hpp>
// #include <wan_agent/wan_agent_utils.hpp>
#include "pre_driver.hpp"
#include "predicate_generator.hpp"
namespace wan_agent {

// Abandon the division of WanAgent base class and TCP implementation class, and use only TCP's WanAgent class.

// configuration entries
#define WAN_AGENT_CONF_VERSION "version"
#define WAN_AGENT_CONF_TRANSPORT "transport"
#define WAN_AGENT_CONF_PRIVATE_IP "private_ip"
#define WAN_AGENT_CONF_PRIVATE_PORT "private_port"
#define WAN_AGENT_CONF_LOCAL_SITE_ID "local_site_id"
#define WAN_AGENT_CONF_SITES "sites"
#define WAN_AGENT_CONF_SERVER_SITES "server_sites"
#define WAN_AGENT_CONF_SENDER_SITES "sender_sites"
#define WAN_AGENT_CONF_NUM_SENDER_SITES "num_of_sender_sites"
#define WAN_AGENT_CONF_SITES_ID "id"
#define WAN_AGENT_CONF_SITES_IP "ip"
#define WAN_AGENT_CONF_SITES_PORT "port"
#define WAN_AGENT_MAX_PAYLOAD_SIZE "max_payload_size"
#define WAN_AGENT_WINDOW_SIZE "window_size"
#define WAN_AGENT_PREDICATE "predicate"
#define EPOLL_MAXEVENTS 64
#define WAN_AGENT_CHECK_SITE_ENTRY(x)                                           \
    if(site.find(x) == site.end()) {                                            \
        throw std::runtime_error(std::string(x) + " missing in a site entry."); \
    }

/**
     * predicate lambda on the "WAN SST", which is organized as a map.
     * The map key is a node id, while the value is a counter for the number
     * of messages that being acknowleged by the corresponding site.
     * Plesae note that the parameter is a copy of the working 'message_counters'
     * The implementation should provide a function to return a message_counters.
     */
using PredicateLambda = std::function<void(const std::map<site_id_t, uint64_t>&)>;

using ReportACKFunc = std::function<void()>;
using NotifierFunc = std::function<void()>;

using read_future_t = std::future<std::pair<persistent::version_t, Blob>>;
using read_promise_t = std::promise<std::pair<persistent::version_t, Blob>>;


// TODO: how to have multiple wan agents on one site?
// I decided to hand this to applications. For example, an application could
// create a Derecho subgroup with multiple WAN agent nodes, each of which joins
// a parallel WAN group doing exactly the same thing. In each of the WAN group,
// the messages is ordered. But no guarantee across WAN groups. The application
// should taking care of this when they try to leverage the bandwidth benefits of
// multiple WAN groups.

struct RequestHeader {
    uint32_t requestType;
    uint64_t version;
    uint64_t seq;
    uint32_t site_id;
    size_t payload_size;
};

struct Response {
    size_t payload_size;
    uint64_t version;
    uint64_t seq;
    uint32_t site_id;
};

/**
     * remote message callback function type
     * @param const RequestHeader&: the copy of request header
     * @param const char*: message in byte array
     */
using RemoteMessageCallback = std::function<std::pair<uint64_t, Blob>(const RequestHeader&, 
                                                 const char*)>;
using ReadRecvCallback = std::function<void(const uint64_t, const site_id_t, Blob&&)>;
using WriteRecvCallback = std::function<void()>;

/**
     * The Wan Agent abstract class
     */
// TODO: break down into sender and receiver
class WanAgent {
    // private:
protected:
    std::atomic<bool> is_shutdown;
    /** local site id */
    site_id_t local_site_id;
    std::string local_ip;
    unsigned short local_port;

    std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> server_sites_ip_addrs_and_ports;
    std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> sender_sites_ip_addrs_and_ports;

    int num_senders;

    /**
         * configuration
         */
    const nlohmann::json config;

    /**
         * load configuration from this->config
         */
    void load_config() noexcept(false);

    /**
         * get local ip and port string
         */
    std::string get_local_ip_and_port() noexcept(false);

public:
    /**
         * constructor
         * @param wan_group_config - the wan_group_config in json.
         * @param pl    - predicate lambda
         * @param rmc   - remote message callback
         */
    WanAgent(const nlohmann::json& wan_group_config, std::string log_level = "trace");

    /**
         * destructor
         */
    virtual ~WanAgent() {}

    /**
         * get local id
         */
    const site_id_t get_local_site_id() const {
        return this->local_site_id;
    }

    /**
         * shutdown the wan agent service and block until finished.
         */
    virtual void shutdown_and_wait() noexcept(false) = 0;

    bool get_is_shutdown() const {
        return is_shutdown.load();
    }
};


// class WanAgentServer;
// the Server worker
class RemoteMessageService final {
private:
    const site_id_t local_site_id;

    size_t num_senders;
    const size_t max_payload_size;
    const RemoteMessageCallback rmc;

    std::list<std::thread> worker_threads;

    int server_socket;
    /**
         * configuration
         */
    const nlohmann::json config;

    const WanAgent* hugger;

    // use epoll to get message from senders.

public:
    RemoteMessageService(const site_id_t local_site_id,
                         int num_senders,
                         unsigned short local_port,
                         const size_t max_payload_size,
                         const RemoteMessageCallback& rmc,
                         WanAgent* hugger);

    void establish_connections();

    void worker(int sock);
    void epoll_worker(int sock);

    bool is_server_ready();
};

class WanAgentServer : public WanAgent {
private:
    /** 
         * remote_message_callback is called when a new message is received.
         */
    const RemoteMessageCallback& remote_message_callback;

    RemoteMessageService remote_message_service;
    // the conditional variable for initialization
    std::mutex ready_mutex;            // TODO: 思考下ready的作用究竟是什么
    std::condition_variable ready_cv;  // TODO: 思考下ready的作用究竟是什么

public:
    WanAgentServer(const nlohmann::json& wan_group_config,
                   const RemoteMessageCallback& rmc, std::string log_level = "trace");
    ~WanAgentServer() {}

    // bool is_ready()
    // {
    //     if (!remote_message_service.is_server_ready())
    //     {
    //         return false;
    //     }

    //     return true;
    // }

    /**
         * shutdown the wan agent service and block until finished.
         */
    virtual void shutdown_and_wait() noexcept(false) override;
};

struct LinkedBufferNode {
    size_t message_size;
    char* message_body;
    uint32_t message_type;
    uint64_t message_version;
    ReadRecvCallback* RRC;
    WriteRecvCallback* WRC;
    // LinkedBufferNode* next;

    LinkedBufferNode() {}
};

// the Client worker
class MessageSender final {
private:
    std::list<LinkedBufferNode> buffer_list;
    std::list<LinkedBufferNode> read_buffer_list;
    const site_id_t local_site_id;
    // std::map<site_id_t, int> sockets;
    int epoll_fd_send_msg;
    int epoll_fd_recv_ack;
    int epoll_fd_read_msg;
    int epoll_fd_recv_read_ack;

    int non_gccjit_calculation(int* seq_vec);
    std::mutex stability_frontier_arrive_mutex;
    std::condition_variable stability_frontier_arrive_cv;
    bool sf_flag = false;
    // if program can work the below 3 lines can be deleted
    // int target_sf = -1;

    const size_t n_slots;
    // size_t head = 0;
    // size_t tail = 0;
    // size_t size = 0;
    
    // std::vector<std::unique_ptr<char[]>> buf;
    // mutex and condition variables for producer-consumer problem
    std::mutex mutex;
    std::condition_variable not_empty;
    std::mutex size_mutex;
    // std::condition_variable not_full;
    std::mutex read_mutex;
    std::condition_variable read_not_empty;
    std::mutex read_size_mutex;

    uint64_t last_all_sent_seqno;
    std::map<site_id_t, uint64_t> last_sent_seqno;
    uint64_t R_last_all_sent_seqno;
    std::map<site_id_t, uint64_t> R_last_sent_seqno;
    std::map<int, site_id_t> sockfd_to_server_site_id_map;
    std::map<int, site_id_t> R_sockfd_to_server_site_id_map;

    std::map<site_id_t, std::atomic<uint64_t>>& message_counters;
    std::map<site_id_t, std::atomic<uint64_t>>& read_message_counters;
    const ReportACKFunc report_new_ack;

    std::atomic<bool> thread_shutdown;
    const int N_MSG = 520000;

    int nServer;
    uint64_t max_version = 0;

    std::mutex rcs;
    std::mutex wcs;
    std::map<uint64_t, ReadRecvCallback*> read_callback_store;
    std::map<uint64_t, WriteRecvCallback*> write_callback_store;

public:
    std::vector<pre_operation> operations;
    // uint64_t *buffer_size = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * N_MSG));
    uint64_t *leave_queue_time_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * 7 * N_MSG));
    /**all sf for each msg**/
    uint64_t *sf_arrive_time_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * 6 * N_MSG));
    // uint64_t *ack_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * 10 ));
    uint64_t *enter_queue_time_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * N_MSG));
    // wait for certaion stability frontier
    int stability_frontier = 0;
    uint64_t read_stability_frontier = (uint64_t)0;
    // uint64_t *who_is_max = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * N_MSG));
    // wait for what sf?
    int wait_target_sf = -1;
    int test_arr[11] = {0, 3, 7, 1, 5, 4, 2, 8, 6, 0, 9};
    // uint64_t* all_sf_situation = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * 40000));
    // index for sf_time_keeper;
    int all_sf_tics = 0;
    int msg_idx = 0;
    // uint64_t *sf_time_keeper = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * 80000));
    // index for sf_time_keeper;
    // int sf_timer_tics = 0;
    std::map<uint64_t, uint64_t> sf_arrive_time_map;
    std::map<uint64_t, uint64_t> remote_all_send_map;
    predicate_fn_type predicate;
    predicate_fn_type inverse_predicate;
    predicate_fn_type complicate_predicate;
    std::map<std::string, predicate_fn_type> predicate_map;
    std::map<std::string, uint64_t> predicate_arrive_map;
    uint64_t sf_arrive_time = 0;

    double sf_calculation_cost = 0;
    double transfer_data_cost = 0;
    double get_size_cost = 0;

    std::mutex stability_frontier_set_mutex;
    std::condition_variable stability_frontier_set_cv;

    // std::map<uint64_t, read_promise_t> read_promise_store;
    // std::mutex read_promise_lock;
    std::map<uint64_t, std::tuple<uint64_t, site_id_t, Blob> > read_object_store;
    std::map<uint64_t, uint64_t> read_recv_cnt;
    std::map<uint64_t, uint16_t> disregards;
    std::map<uint64_t, uint16_t> w_disregards;

    std::map<uint64_t, uint64_t> write_recv_cnt;

    MessageSender(const site_id_t& local_site_id,
                  const std::map<site_id_t, std::pair<ip_addr_t, uint16_t>>& server_sites_ip_addrs_and_ports,
                  const size_t& n_slots, const size_t& max_payload_size,
                  std::map<site_id_t, std::atomic<uint64_t>>& message_counters,
                  std::map<site_id_t, std::atomic<uint64_t>>& read_message_counters,
                  const ReportACKFunc& report_new_ack);
    inline void update_max_version(const uint64_t& version) {
        max_version = std::max(version, max_version);
    }
    void recv_ack_loop();
    void recv_read_ack_loop();
    uint64_t enqueue(const uint32_t requestType, const char* payload, const size_t payload_size, const uint64_t version, WriteRecvCallback* WRC);
    void read_enqueue(const uint64_t& version, ReadRecvCallback* RRC);
    void send_msg_loop();
    void read_msg_loop();
    void predicate_calculation();
    void read_predicate_calculation();
    void wait_stability_frontier_loop(int sf);
    void sf_time_checker_loop();
    void wait_read_predicate(const uint64_t seq, const uint64_t version, const site_id_t site, Blob&& obj);
    void trigger_read_callback(const uint64_t seq, const uint64_t version, const site_id_t site, Blob&& obj);
    void trigger_write_callback(const uint64_t seq);
    // void set_stability_frontier(int sf);
    void shutdown() {
        thread_shutdown.store(true);
        std::cout << "set thread_shutdown to " << thread_shutdown.load() << " in MessageSender shutdown\n";
    }
};

class WanAgentSender : public WanAgent {

private:
    /** the conditional variable and thread for notification */
    std::mutex new_ack_mutex;
    std::condition_variable new_ack_cv;
    bool has_new_ack;
    // std::thread predicate_thread;
    /** the conditional variable for stability waiting*/

    /**
         * predicted_lambda is called when an acknowledgement is received.
         */
    const PredicateLambda& predicate_lambda;

    std::unique_ptr<MessageSender> message_sender;
    std::thread recv_ack_thread;
    std::thread send_msg_thread;
    std::thread wait_sf_thread;
    std::thread recv_read_ack_thread;
    std::thread read_msg_thread;
    uint64_t all_start_time;
    std::map<site_id_t, std::atomic<uint64_t>> message_counters;
    std::map<site_id_t, std::atomic<uint64_t>> read_message_counters;
    std::string predicate_experssion;
    std::string inverse_predicate_expression;
    Predicate_Generator* predicate_generator;
    Predicate_Generator* inverse_predicate_generator;
    predicate_fn_type predicate;
    predicate_fn_type inverse_predicate;
    std::map<std::string, predicate_fn_type> predicate_map;

public:
    WanAgentSender(const nlohmann::json& wan_group_config,
                   const PredicateLambda& pl, std::string log_level = "trace");
    ~WanAgentSender() {}

    // bool is_ready()
    // {
    //     if (!message_sender->is_client_ready())
    //     {
    //         return false;
    //     }

    //     return true;
    // }

    virtual void shutdown_and_wait() noexcept(false) override;

    /**
         * report new ack. Implementation should call this to wake up the predicate thread.
         */
    void report_new_ack();
    void out_out_file();
    /**
         * send the message
         */
    virtual void send(const char* message, const size_t message_size) {
        this->message_sender->enqueue(1, message, message_size, uint64_t(-1), nullptr);
    }
    virtual uint64_t send_write_req(const char* payload, const size_t payload_size, WriteRecvCallback* WRC, const uint64_t version=(uint64_t)-1) {
        return this->message_sender->enqueue(1, payload, payload_size, version, WRC);
    }
    virtual void send_read_req(ReadRecvCallback* RRC, const uint64_t version=(uint64_t)-1) {
        this->message_sender->read_enqueue(version, RRC);
    }

    void submit_predicate(std::string key, std::string predicate_str, bool inplace);

    void generate_predicate();

    void change_predicate(std::string key);

    int get_stability_frontier();

    uint64_t get_stability_frontier_arrive_time();
    void set_stability_frontier(int sf);
    void test_predicate();
    void wait() {
        bool mark = 1;
        while (mark) {
            mark = 0;
            for (auto& p : message_counters) {
                if (p.second != get_stability_frontier()) {
                    mark = 1;
                    break;
                }
            }
            if (!mark) {
                for (auto& p : read_message_counters) {
                    if (p.second != message_sender->read_stability_frontier) {
                        mark = 1;
                        break;
                    }
                }
            }
        }
    }
    /**
         * return a moveable counter table
         */
    std::map<uint32_t, uint64_t> get_message_counters() noexcept(true) {
        std::map<uint32_t, uint64_t> counters;
        for(auto& item : message_counters) {
            counters[item.first] = item.second.load();
        }
        return std::move(counters);
    }
};

}  // namespace wan_agent

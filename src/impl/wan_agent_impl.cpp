#include <arpa/inet.h>
#include <dlfcn.h>
#include <exception>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
#include <logger.hpp>
#include <wan_agent.hpp>
#include <wan_agent_utils.hpp>

inline uint64_t get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

namespace wan_agent {

void WanAgentAbstract::load_config() noexcept(false) {
    log_enter_func();
    // Check if all mandatory keys are included.
    static const std::vector<std::string> must_have{
            WAN_AGENT_CONF_VERSION,
            // WAN_AGENT_CONF_TRANSPORT, // not so mandatory now.
            WAN_AGENT_CONF_LOCAL_SITE_ID,
            WAN_AGENT_CONF_SERVER_SITES,
            // WAN_AGENT_CONF_SENDER_SITES,
            WAN_AGENT_CONF_NUM_SENDER_SITES,
            // we need to get local ip & port info directly
            WAN_AGENT_CONF_PRIVATE_IP,
            WAN_AGENT_CONF_PRIVATE_PORT};
    for(auto& must_have_key : must_have) {
        if(config.find(must_have_key) == config.end()) {
            throw std::runtime_error(must_have_key + " is not found");
        }
    }
    local_site_id = config[WAN_AGENT_CONF_LOCAL_SITE_ID];
    local_ip = config[WAN_AGENT_CONF_PRIVATE_IP];
    local_port = config[WAN_AGENT_CONF_PRIVATE_PORT];
    num_senders = config[WAN_AGENT_CONF_NUM_SENDER_SITES];
    // Check if sites are valid.
    // if (config[WAN_AGENT_CONF_SENDER_SITES].size() == 0 || config[WAN_AGENT_CONF_SERVER_SITES] == 0)
    if(config[WAN_AGENT_CONF_SERVER_SITES] == 0) {
        throw std::runtime_error("Sites do not have any configuration");
    }
    for(auto& site : config[WAN_AGENT_CONF_SERVER_SITES]) {
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_ID);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_IP);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_PORT);
        server_sites_ip_addrs_and_ports.emplace(site[WAN_AGENT_CONF_SITES_ID],
                                                std::make_pair(site[WAN_AGENT_CONF_SITES_IP],
                                                               site[WAN_AGENT_CONF_SITES_PORT]));
    }

    if(config.find(WAN_AGENT_CONF_SENDER_SITES) != config.end()) {
        for(auto& site : config[WAN_AGENT_CONF_SENDER_SITES]) {
            WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_ID);
            WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_IP);
            WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_PORT);
            sender_sites_ip_addrs_and_ports.emplace(site[WAN_AGENT_CONF_SITES_ID],
                                                    std::make_pair(site[WAN_AGENT_CONF_SITES_IP],
                                                                   site[WAN_AGENT_CONF_SITES_PORT]));
        }
    }

    log_exit_func();
}  // namespace wan_agent

std::string WanAgentAbstract::get_local_ip_and_port() noexcept(false) {
    std::string local_ip;
    unsigned short local_port = 0;
    if(config.find(WAN_AGENT_CONF_PRIVATE_IP) != config.end() && config.find(WAN_AGENT_CONF_PRIVATE_PORT) != config.end()) {
        local_ip = config[WAN_AGENT_CONF_PRIVATE_IP];
        local_port = config[WAN_AGENT_CONF_PRIVATE_PORT];
    } else {
        throw std::runtime_error("Cannot find ip and port configuration for local site.");
    }
    return local_ip + ":" + std::to_string(local_port);
}

WanAgentAbstract::WanAgentAbstract(const nlohmann::json& wan_group_config, std::string log_level)
        : is_shutdown(false),
          config(wan_group_config) {
    // this->message_counters = std::make_unique<std::map<uint32_t,std::atomic<uint64_t>>>();
    load_config();
    Logger::set_log_level(log_level);
}

RemoteMessageService::RemoteMessageService(const site_id_t local_site_id,
                                           int num_senders,
                                           unsigned short local_port,
                                           const size_t max_payload_size,
                                           const RemoteMessageCallback& rmc,
                                           int msg_num,
                                           const nlohmann::json& wan_group_config,
                                           WanAgentAbstract* hugger)
        : local_site_id(local_site_id),
          num_senders(num_senders),
          max_payload_size(max_payload_size),
          rmc(rmc),
          total_msg(msg_num),
          config(wan_group_config),
          hugger(hugger) {
    std::cout << "1: " << local_site_id << std::endl;
    std::cout << "2: " << total_msg << std::endl;
    sockaddr_in serv_addr;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0)
        throw std::runtime_error("RemoteMessageService failed to create socket.");

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
    serv_addr.sin_port = htons(local_port);
    if(bind(fd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr, "ERROR on binding to socket: %s\n", strerror(errno));
        throw std::runtime_error("RemoteMessageService failed to bind socket.");
    }
    listen(fd, 5);
    server_socket = fd;
    std::cout << "RemoteMessageService listening on " << local_port << std::endl;
    init_message_status_counter();
    // dbg_default_info("RemoteMessageService listening on {} ...", local_port);
};

void RemoteMessageService::establish_connections() {
    // TODO: maybe support dynamic join later, i.e. having a infinite loop always listening for join requests?
    auto num_fd = (num_senders << 1);
    while(worker_threads.size() < num_fd) {
        struct sockaddr_storage client_addr_info;
        socklen_t len = sizeof client_addr_info;
        int connected_sock_fd = ::accept(server_socket, (struct sockaddr*)&client_addr_info, &len);
        worker_threads.emplace_back(std::thread(&RemoteMessageService::epoll_worker, this, connected_sock_fd));
    }
}

std::string RemoteMessageService::prepare_reply(){
    json j;
    for(auto iter = message_status.begin(); iter != message_status.end(); iter++){
        j[iter->first] = iter->second.load();
    }    
    return j.dump();
}

void RemoteMessageService::update_message_status(std::string key){
    // message_status[key]++;

    // tmp update all
    for(auto& predicate_tmp : config[WAN_AGENT_PREDICATES]) {
        message_status[predicate_tmp["key"]]++;
    }
}

void RemoteMessageService::init_message_status_counter(){
    for(auto& predicate_tmp : config[WAN_AGENT_PREDICATES]) {
        message_status[predicate_tmp["key"]] = 0;
    }
}

void RemoteMessageService::epoll_worker(int connected_sock_fd) {
    RequestHeader header;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(max_payload_size);
    bool success;
    // std::cout << "epoll_worker start\n";

    int epoll_fd_recv_msg = epoll_create1(0);
    if(epoll_fd_recv_msg == -1)
        throw std::runtime_error("failed to create epoll fd");
    add_epoll(epoll_fd_recv_msg, EPOLLIN, connected_sock_fd);

    // std::cout << "The connected_sock_fd is " << connected_sock_fd << std::endl;

    struct epoll_event events[EPOLL_MAXEVENTS];
    while(!hugger->get_is_shutdown()) {
        int n = epoll_wait(epoll_fd_recv_msg, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLIN) {
                success = sock_read(connected_sock_fd, header);
                if(!success) {
                    std::cout << "Failed to read request header, "
                              << "receive " << n << " messages from sender.\n";
                    throw std::runtime_error("Failed to read request header");
                }
                if(receive_cnt==1000 && all_start_time == 0){
                    all_start_time = get_time_us();
                }
                if (header.payload_size) {
                    success = sock_read(connected_sock_fd, buffer.get(), header.payload_size);
                    if(!success)
                        throw std::runtime_error("Failed to receive object");
                    receive_cnt++;
                    if(msg_size == -1){
                        msg_size = header.payload_size;
                    }
                    last_message_time = get_time_us();
                }
                std::pair<uint64_t, Blob> version_obj = std::move(rmc(header, buffer.get()));
                update_message_status("test");
                std::string json_reply = prepare_reply();
                success = sock_write(connected_sock_fd, Response{version_obj.second.size, json_reply.size(),version_obj.first, header.seq, local_site_id});

                // the json reply
                success = sock_write(connected_sock_fd, json_reply.c_str(), json_reply.size());
                // std::cout << "ACK sent of request = " + std::to_string(header.seq) + " which is a " + (header.requestType ? "read":"write") << " request\n";
                // std::cout << "ACK sent of request = " + std::to_string(header.seq) + "\n";
                // if(total_msg == receive_cnt){
                //     receive_cnt -= 1000;
                //     double total_time = (last_message_time-all_start_time)/1000000.0;
                //     std::cout << receive_cnt << " msg" << "\n";
                //     std::cout << receive_cnt/total_time <<" msg/s"<< msg_size*8*receive_cnt/total_time/1024/1024 << " Mbit/s"<<"\n";
                // }
                if (header.version != -1 && version_obj.first != header.version)
                    throw std::runtime_error("Receiver: something wrong with version");
                if(!success)
                    throw std::runtime_error("Failed to send ACK message");
                if (header.requestType == 0) { // read request
                    success = sock_write(connected_sock_fd, version_obj.first);
                    if (!success)
                        throw std::runtime_error("Failed to send version");
                    success = sock_write(connected_sock_fd, version_obj.second.bytes, version_obj.second.size);
                    if (!success)
                        throw std::runtime_error("Failed to send all the bytes");
                }
            }
        }
    }
}



WanAgentServer::WanAgentServer(const nlohmann::json& wan_group_config,
                               const RemoteMessageCallback& rmc, std::string log_level)
        : WanAgentAbstract(wan_group_config, log_level),
          remote_message_callback(rmc),
          remote_message_service(
                  local_site_id,
                  num_senders,
                  local_port,
                  wan_group_config[WAN_AGENT_MAX_PAYLOAD_SIZE],
                  rmc,
                  wan_group_config["message_num"],
                  wan_group_config,
                  this) {
    std::thread rms_establish_thread(&RemoteMessageService::establish_connections, &remote_message_service);
    rms_establish_thread.detach();
}

void WanAgentServer::shutdown_and_wait() {
    log_enter_func();
    is_shutdown.store(true);
    log_exit_func();
}

MessageSender::MessageSender(const site_id_t& local_site_id,
                             const std::map<site_id_t, std::pair<ip_addr_t, uint16_t>>& server_sites_ip_addrs_and_ports,
                             const size_t& n_slots, const size_t& max_payload_size,
                             std::map<site_id_t, std::atomic<uint64_t>>& message_counters,
                             std::map<std::string, std::map<site_id_t, std::atomic<uint64_t>> > &message_counters_for_types,
                            //  std::map<site_id_t, std::atomic<uint64_t>>& read_message_counters,
                             const ReportACKFunc& report_new_ack)
        : local_site_id(local_site_id),
          n_slots(n_slots),  // TODO: useless after using linked list
          last_all_sent_seqno(static_cast<uint64_t>(-1)),
          R_last_all_sent_seqno(static_cast<uint64_t>(-1)),
          message_counters(message_counters),
          message_counters_for_types(message_counters_for_types),
        //   read_message_counters(read_message_counters),
          report_new_ack(report_new_ack),
          thread_shutdown(false) {
    log_enter_func();
    // for(unsigned int i = 0; i < n_slots; i++) {
    //     buf.push_back(std::make_unique<char[]>(sizeof(size_t) + max_payload_size));
    // }

    epoll_fd_send_msg = epoll_create1(0);
    if(epoll_fd_send_msg == -1)
        throw std::runtime_error("failed to create epoll fd");

    epoll_fd_recv_ack = epoll_create1(0);
    if(epoll_fd_recv_ack == -1)
        throw std::runtime_error("failed to create epoll fd");

    epoll_fd_read_msg = epoll_create1(0);
    if (epoll_fd_read_msg == -1)
        throw std::runtime_error("failed to creat epoll fd");

    epoll_fd_recv_read_ack = epoll_create1(0);
    if (epoll_fd_recv_read_ack == -1)
        throw std::runtime_error("failed to create epoll fd");

    for(const auto& [site_id, ip_port] : server_sites_ip_addrs_and_ports) {
        // if(site_id != local_site_id) {
            site_id_to_rank[site_id] = site_num_count++;
            sockaddr_in serv_addr;
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            int R_fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if(fd < 0 || R_fd < 0)
                throw std::runtime_error("MessageSender failed to create socket.");
            int flag = 1;
            int ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
            if(ret == -1) {
                fprintf(stderr, "ERROR on setsockopt: %s\n", strerror(errno));
                exit(-1);
            }
            ret = setsockopt(R_fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
            if (ret == -1) {
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
            if (connect(R_fd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
                throw std::runtime_error("MessageSender failed to connect socket");
            }
            std::cout << "new fd pair " << site_id << ' ' << fd << ' ' << R_fd << std::endl;
            add_epoll(epoll_fd_send_msg, EPOLLOUT, fd);
            add_epoll(epoll_fd_recv_ack, EPOLLIN, fd);
            add_epoll(epoll_fd_read_msg, EPOLLOUT, R_fd);
            add_epoll(epoll_fd_recv_read_ack, EPOLLIN, R_fd);
            sockfd_to_server_site_id_map[fd] = site_id;
            R_sockfd_to_server_site_id_map[R_fd] = site_id;
            last_sent_seqno.emplace(site_id, static_cast<uint64_t>(-1));
            R_last_sent_seqno.emplace(site_id, static_cast<uint64_t>(-1));
        // }
    }

    nServer = message_counters.size();
    set_read_quorum(message_counters.size()/2+1);
    std::cout << "nServer = " << nServer << ",read_quorum " << read_quorum << std::endl;
    log_exit_func();
}

// void MessageSender::wait_read_predicate(const uint64_t seq,
//                                         const uint64_t version,
//                                         const site_id_t site,
//                                         Blob&& obj) {
//     read_recv_cnt[seq]++;
//     if (disregards[seq]) {
//         if (read_recv_cnt[seq] == nServer) {
//             read_recv_cnt.erase(read_recv_cnt.find(seq));
//             disregards.erase(disregards.find(seq));
//         }
//         return;
//     }
//     std::tuple<uint64_t, site_id_t, Blob>& cur_obj = read_object_store[seq];
//     if (version > std::get<0>(cur_obj)) {
//         cur_obj = std::move(std::tuple<uint64_t, site_id_t, Blob>(version, site, obj));
//     }
//     if (read_stability_frontier > seq) {
//         read_callback_store[seq](std::get<0>(cur_obj), std::get<1>(cur_obj), std::move(std::get<2>(cur_obj)));
//         read_callback_store.erase(read_callback_store.find(seq));
//         disregards[seq] = 1;
//     }
// }

// void MessageSender::trigger_read_callback(const uint64_t seq, 
//                                           const uint64_t version, 
//                                           const site_id_t site, 
//                                           Blob&& obj) {
//     read_recv_cnt[seq]++;
//     read_callback_store[seq](version, site, std::move(obj));
//     if (read_recv_cnt[seq] == nServer) {
//         read_recv_cnt.erase(read_recv_cnt.find(seq));
//         read_callback_store.erase(read_callback_store.find(seq));
//     }
// }

void MessageSender::recv_ack_loop() {
    log_enter_func();
    auto tid = pthread_self();
    // std::cout << "recv_ack_loop start, tid = " << tid << std::endl;
    struct epoll_event events[EPOLL_MAXEVENTS];
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(65536);
    while(!thread_shutdown.load()) {
        int n = epoll_wait(epoll_fd_recv_ack, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLIN) {
                // received ACK
                Response res;
                auto success = sock_read(events[i].data.fd, res);
                if (!success) {
                    throw std::runtime_error("failed receiving ACK message");
                }
                auto json_size = res.json_reply_size;
                success = sock_read(events[i].data.fd, buffer.get(), res.json_reply_size);
                if (!success) {
                    throw std::runtime_error("failed receiving json reply");
                }
                // std::cout << "site_id: " << res.site_id<<", json_size: " << json_size <<  " ," << std::string(buffer.get()) << std::endl;
                json json_reply = json::parse(std::string(buffer.get(), json_size));
                // update_predicate_counter(json_reply, res.site_id);
                update_predicate_counter_postfix(json_reply, res.site_id);
                // uint64_t sfst = get_time_us();
                predicate_calculation_postfix();
                // sf_calculation_cost += (get_time_us() - sfst*1.0) / 1000.0;
                // if (res.site_id == 1000)
                //     std::cout << "received ACK from " + std::to_string(res.site_id) + " for msg " + std::to_string(res.version) + '\n';// +
                // "payload " + std::to_string(res.payload_size) + "seq " + std::to_string(res.seq) + '\n';
                // ack_keeper[4*(message_counters[res.site_id])+(res.site_id - 1001)] = get_time_us();
                // message_counters[res.site_id]++;
                // uint64_t pre_cal_st_time = get_time_us();
                // int pre_stability_frontier = stability_frontier;
                // predicate_calculation_multi();
                // predicate_calculation();
                // trigger_write_callback(pre_stability_frontier);
                // if(stability_frontier == 10000){
                //     std::cout << "all done! " << (get_time_us() - enter_queue_time_keeper[0]) << std::endl;
                // }
                // std::cout << "current write stability frontier = " + std::to_string(stability_frontier) + '\n';
                // transfer_data_cost += (get_time_us() - pre_cal_st_time) / 1000000.0;
                
                // if(res.seq == wait_target_sf) {
                //     ack_keeper[res.site_id - 1000] = get_time_us();
                // }
            }
        }
    }
    log_exit_func();
}

void MessageSender::update_predicate_counter(json json_reply, site_id_t site_id){
    for (json::iterator it = json_reply.begin(); it != json_reply.end(); ++it) {
        message_counters_for_types[it.key()][site_id] = it.value();
    }
}

void MessageSender::update_predicate_counter_postfix(json json_reply, site_id_t site_id){
    for (json::iterator it = json_reply.begin(); it != json_reply.end(); ++it) {
        arr_message_counter[ack_type_id[it.key()] * 16 + site_id_to_rank[site_id] ] = it.value();
    }
    // print_arr_msg_counter();
}

void MessageSender::trigger_write_callback(const int pre_stability_frontier){
    if(stability_frontier != pre_stability_frontier){
        // jump from fast predicate to slow may skip the stability frontier between
        if(stability_frontier > pre_stability_frontier){
            for(int j = pre_stability_frontier+1; j <= stability_frontier; j++){
                if(write_callback_store.count(j-1) == 0) {
                    continue;
                }
                (*write_callback_store[j-1])(j);
                write_callback_store.erase(write_callback_store.find(j-1));
            }
        }else{// Symmetrically, slow to fast may cause flashback
            std::cerr<<"flashback!!!"<<std::endl;
        }
    }
}

void MessageSender::set_read_quorum(int read_quorum){
    this->read_quorum = read_quorum;
}

void MessageSender::recv_read_ack_loop() {
    log_enter_func();
    auto tid = pthread_self();
    // std::cout << "recv_read_ack_loop start, tid = " << tid << std::endl;
    struct epoll_event events[EPOLL_MAXEVENTS];
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(65536);
    while(!thread_shutdown.load()) {
        int n = epoll_wait(epoll_fd_recv_read_ack, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLIN) {
                // received ACK
                // ack_cnt++;
                Response res;
                auto success = sock_read(events[i].data.fd, res);
                if (!success) {
                    throw std::runtime_error("failed receiving ACK message");
                }
                // std::cout << "received read ACK size " + std::to_string(res.payload_size) << " from " + std::to_string(res.site_id) << " seq is " + std::to_string(res.seq) << '\n';
                auto json_size = res.json_reply_size;
                success = sock_read(events[i].data.fd, buffer.get(), res.json_reply_size);
                if (!success) {
                    throw std::runtime_error("failed receiving json reply");
                }
                json json_reply = json::parse(std::string(buffer.get()));
                update_predicate_counter(json_reply, res.site_id);

                auto obj_size = res.payload_size;
                if (!obj_size) {
                    throw std::runtime_error("Read Request: Received an empty object");
                }
                uint64_t cur_version;
                success = sock_read(events[i].data.fd, cur_version);
                if (!success)
                    throw std::runtime_error("Read Request: Failed receiving object version");
                Blob cur_obj = std::move(Blob(nullptr, obj_size));
                // uint64_t qq_time = get_time_us();
                success = sock_read(events[i].data.fd, cur_obj.bytes, obj_size);
                // read_time_cost += (get_time_us() - qq_time*1.0) / 1000.0;
                if (!success)
                    throw std::runtime_error("failed receiving object for read request");
                // if there are already read_quorum read acks, discard all future read acks.
                if(read_seq_counter[res.seq] < read_quorum){
                    if(read_highest_version_keeper.count(res.seq) == 0){
                        read_highest_version_keeper[res.seq] = std::make_pair(cur_obj, res.version);
                    }else{
                        if(read_highest_version_keeper[res.seq].second < res.version){
                            read_highest_version_keeper[res.seq] = std::make_pair(cur_obj, res.version);
                        }
                    }
                    if(++read_seq_counter[res.seq] == read_quorum){
                        // std::cout << "quorum fullfilled from " + std::to_string(res.site_id) << " seq is " + std::to_string(res.seq) << '\n';
                        (*read_callback_store[res.seq])(res.version, std::move(read_highest_version_keeper[res.seq].first));
                        read_highest_version_keeper.erase(read_highest_version_keeper.find(res.seq));
                        read_callback_store.erase(read_callback_store.find(res.seq));
                    }
                }
                
                
                // read_predicate_calculation();
                // std::cout << "current read stability frontier = " + std::to_string(read_stability_frontier) + '\n';
                // if (res.version == -1) {
                //     wait_read_predicate(res.seq, cur_version, res.site_id, std::move(cur_obj));
                // } else {
                //     trigger_read_callback(res.seq, res.version, res.site_id, std::move(cur_obj));
                // }
            }
        }
    }

    log_exit_func();
}

void MessageSender::predicate_calculation_multi() {
    log_enter_func(); 
    for(auto iter = message_counters_for_types.begin(); iter != message_counters_for_types.end(); iter++){
        std::vector<int> value_ve;
        std::vector<std::pair<site_id_t, uint64_t>> pair_ve;
        value_ve.reserve(iter->second.size());
        pair_ve.reserve(iter->second.size());
        value_ve.push_back(0);
        for(std::map<site_id_t, std::atomic<uint64_t>>::iterator it = iter->second.begin(); it != iter->second.end(); it++) {
            value_ve.push_back(it->second.load());
            pair_ve.push_back(std::make_pair(it->first, it->second.load()));
        }
        int* arr = &value_ve[0];
        int val = predicate_map[iter->first](5, arr);
        stability_frontier_for_types[iter->first] = pair_ve[val - 1].second;
        // std::cout << iter->first << " sf is : " << stability_frontier_for_types[iter->first] << std::endl;
    }
}


void MessageSender::predicate_calculation_postfix() {
    int val = new_type_predicate(arr_message_counter);
    new_type_stability_frontier = arr_message_counter[val];
    for(auto& key_predicate : new_predicate_map){
        int sf = arr_message_counter[key_predicate.second(arr_message_counter)];
        new_predicate_arrive_map[key_predicate.first] = sf;
        // here we can trigger something

    }
    stability_frontier_arrive_cv.notify_one();
    monitor_stability_frontier_cv.notify_all();
    printf("%d\n", new_type_stability_frontier);
    // std::cout  << "new type sf is : " << new_type_stability_frontier << std::endl;
}

void MessageSender::predicate_calculation() {
    log_enter_func();
    std::vector<int> value_ve;
    std::vector<std::pair<site_id_t, uint64_t>> pair_ve;
    value_ve.reserve(message_counters.size());
    pair_ve.reserve(message_counters.size());
    value_ve.push_back(0);
    for(std::map<site_id_t, std::atomic<uint64_t>>::iterator it = message_counters.begin(); it != message_counters.end(); it++) {
        value_ve.push_back(it->second.load());
        pair_ve.push_back(std::make_pair(it->first, it->second.load()));
    }
    int* arr = &value_ve[0];
    int val = predicate(5, arr);
    stability_frontier = pair_ve[val - 1].second;

    /**general recording all sf at every 5000 message**/
    if((stability_frontier + 1) % 5000 == 0) {
        std::cout << stability_frontier << std::endl;
    //     sf_arrive_time_map[stability_frontier] = get_time_us();
        // for(int i = 1; i < (int)value_ve.size(); i++) {
        //     std::cout << arr[i] << " ";
        // }
        // std::cout << std::endl;
    //     all_sf_situation[all_sf_tics * 7] = get_time_us();
    //     int tmp_idx = 1;
    //     for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //         int tmp_val = it->second(5, arr);
    //         all_sf_situation[all_sf_tics * 7 + tmp_idx] = pair_ve[tmp_val - 1].second;
    //         tmp_idx++;
    //     }
    //     all_sf_tics++;
    }

    /**wait for certain file size, and record the first time it arrives**/
    // for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //     int tmp_val = it->second(5, arr);
    //     int tmp_sf = pair_ve[tmp_val - 1].second;
    //     if(tmp_sf == wait_target_sf) {
    //         if(predicate_arrive_map.count(it->first) == 0) {
    //             predicate_arrive_map[it->first] = get_time_us();
    //         }
    //     }
    // }

    /**record every message arrive to see each file's performance**/
    // int predicate_idx = 6;
    // for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //     int tmp_val = it->second(5, arr);
    //     int tmp_sf = pair_ve[tmp_val - 1].second;
    //     if(tmp_sf > 0 && sf_arrive_time_keeper[tmp_sf * 6 - predicate_idx] == 0) {
    //         sf_arrive_time_keeper[tmp_sf * 6 - predicate_idx] = get_time_us();
    //     }
    //     // if(tmp_sf > 0 && it->first == "MAX_NODE" && who_is_max[tmp_sf] == 0) {
    //     //     who_is_max[tmp_sf] = pair_ve[tmp_val - 1].first;
    //     // }

    //     predicate_idx--;
    // }
    // stability_frontier_arrive_cv.notify_one();

    /**comparation with gccjit and none gccjit**/
    // uint64_t sf_cal_st_time = get_time_us();
    // complicate_predicate(5, test_arr);
    // predicate_map["Complicated"](5,test_arr);
    // non_gccjit_calculation(test_arr);
    // sf_calculation_cost += (get_time_us() - sf_cal_st_time) / 1000000.0;
    // log_exit_func();
}


// void MessageSender::read_predicate_calculation() {
//     std::vector<int> value_ve;
//     std::vector<std::pair<site_id_t, uint64_t>> pair_ve;
//     value_ve.reserve(read_message_counters.size());
//     pair_ve.reserve(read_message_counters.size());
//     value_ve.push_back(0);
//     for(std::map<site_id_t, std::atomic<uint64_t>>::iterator it = read_message_counters.begin(); it != read_message_counters.end(); it++) {
//         value_ve.push_back(it->second.load());
//         pair_ve.push_back(std::make_pair(it->first, it->second.load()));
//     }
//     int* arr = &value_ve[0];

//     int val = inverse_predicate(5, arr);
//     read_stability_frontier = pair_ve[val - 1].second;
// }

int MessageSender::non_gccjit_calculation(int* seq_vec) {
    int predicate_size = (int)operations.size();
    int operation_num = (int)operations.size();
    // blocks initialization based on # of operator
    for(int i = 0; i < predicate_size; i++) {
        int maxx = -1, minn = 0x7f7f7f7f, kth = 0;
        int maxx_idx = -1, minn_idx = -1, kth_idx = -1;
        int operation_index = operation_num - 1 - i;
        int op = operations[operation_index].op_code;

        switch(op) {
            case 1:  // MAX
                maxx = -1;
                for(int j = 0; j < (int)operations[operation_index].site_range.size(); j++) {
                    if(maxx < seq_vec[operations[operation_index].site_range[j]]) {
                        maxx = seq_vec[operations[operation_index].site_range[j]];
                        maxx_idx = operations[operation_index].site_range[j];
                    }
                }
                if(operations[operation_index].pass_result_to != -1) {
                    // int pass_to = operation_num - 1 - operations[operation_index].pass_result_to;
                    // cout << operations[operation_index].pass_result_to << endl;
                    // operations[pass_to].site_range.push_back(maxx_idx);
                    operations[operations[operation_index].pass_result_to].site_range.push_back(maxx_idx);
                } else {
                    return maxx_idx;
                }
                break;
            case 2:  //MIN
                minn = 0x7f7f7f7f;
                for(int j = 0; j < (int)operations[operation_index].site_range.size(); j++) {
                    if(minn > seq_vec[operations[operation_index].site_range[j]]) {
                        minn = seq_vec[operations[operation_index].site_range[j]];
                        minn_idx = operations[operation_index].site_range[j];
                    }
                }
                if(operations[operation_index].pass_result_to != -1) {
                    // int pass_to = operation_num - 1 - operations[operation_index].pass_result_to;
                    // operations[pass_to].site_range.push_back(minn_idx);
                    operations[operations[operation_index].pass_result_to].site_range.push_back(minn_idx);
                } else {
                    return minn_idx;
                }
                break;
            case 3:  //KTH
                kth = operations[operation_index].kth_k;
                for(int j = 0; j < (int)operations[operation_index].site_range.size() - 1; j++) {
                    for(int k = 0; k < (int)operations[operation_index].site_range.size() - j - 1; k++) {
                        if(seq_vec[operations[operation_index].site_range[k]] > seq_vec[operations[operation_index].site_range[k + 1]]) {
                            int tmp = seq_vec[operations[operation_index].site_range[k]];
                            seq_vec[operations[operation_index].site_range[k]] = seq_vec[operations[operation_index].site_range[k + 1]];
                            seq_vec[operations[operation_index].site_range[k + 1]] = tmp;
                        }
                    }
                }
                if(operations[operation_index].pass_result_to != -1) {
                    int pass_to = operation_num - 1 - operations[operation_index].pass_result_to;
                    operations[pass_to].site_range.push_back(operations[operation_index].site_range[kth - 1]);
                } else {
                    return operations[operation_index].site_range[kth - 1];
                }
                break;
            default:
                break;
        }
    }
}

void MessageSender::wait_stability_frontier_loop(int sf, std::string predicate_key) {
    while (true)
    {
        std::unique_lock<std::mutex> lock(stability_frontier_arrive_mutex);
        stability_frontier_arrive_cv.wait(lock, [this, &sf, &predicate_key]() { return new_predicate_arrive_map[predicate_key] >= sf; });
        break;
    }
    printf("%d arrived\n", sf);
    pthread_exit(NULL);
    //below two are together
    // sf_arrive_time = get_time_us();
    // stability_frontier_set_cv.notify_one();
}

void MessageSender::monitor_stability_frontier_loop(std::string predicate_key, MonitorCallback mc) {
    if (new_predicate_map.count(predicate_key) == 0)
    {
        std::cerr << "no such key!\n";
        return;
    }
    std::mutex mutex_for_this_loop;
    // monitor_stability_frontier_mutexes.push_back(mutex_for_this_loop);
    int old_value = new_predicate_arrive_map[predicate_key];
    while (true)
    {
        std::unique_lock<std::mutex> lock(monitor_stability_frontier_mutex);
        monitor_stability_frontier_cv.wait(lock, [this, &predicate_key, &old_value]() { 
            return new_predicate_arrive_map[predicate_key] != old_value; 
        });
        old_value = new_predicate_arrive_map[predicate_key];
        char* sss;
        mc(new_predicate_arrive_map[predicate_key], sss);
    }
    
}




uint64_t MessageSender::enqueue(const uint32_t requestType, const char* payload, const size_t payload_size, const uint64_t version, WriteRecvCallback* WRC) {
        // std::unique_lock<std::mutex> lock(mutex);
    size_mutex.lock();
    LinkedBufferNode* tmp = new LinkedBufferNode();
    tmp->message_size = payload_size;
    tmp->message_body = (char*)malloc(payload_size);
    memcpy(tmp->message_body, payload, payload_size);
    tmp->message_type = requestType;
    tmp->RRC = nullptr;
    tmp->WRC = WRC;

    uint64_t ret = 0;
    if (version == (uint64_t)-1) {
        tmp->message_version = (++max_version);
        ret = max_version;
    } else {
        tmp->message_version = version;
        ret = version;
        max_version = std::max(version, max_version);
    }
    
    buffer_list.push_back(std::move(*tmp));
    delete tmp;
    enter_queue_time_keeper[msg_idx++] = get_time_us();
    size_mutex.unlock();
    not_empty.notify_one();
    return ret;
}

void MessageSender::read_enqueue(const uint64_t& version, ReadRecvCallback* RRC) {
    read_size_mutex.lock();
    LinkedBufferNode* tmp = new LinkedBufferNode();
    tmp->message_body = nullptr;
    tmp->message_size = 0;
    tmp->message_type = 0;
    tmp->message_version = version;
    tmp->RRC = RRC;
    tmp->WRC = nullptr;

    read_buffer_list.push_back(std::move(*tmp));
    // enter_queue_time_keeper[msg_idx++] = get_time_us();
    read_size_mutex.unlock();
    read_not_empty.notify_one();
    delete tmp;
}

void MessageSender::send_msg_loop() {
    log_enter_func();
    auto tid = pthread_self();
    // std::cout << "send_msg_loop start, tid = " << tid << std::endl;
    struct epoll_event events[EPOLL_MAXEVENTS];
    while(!thread_shutdown.load()) {
        // std::cout << "in send_msg_loop, thread_shutdown.load() is " << thread_shutdown.load() << std::endl;
        std::unique_lock<std::mutex> lock(mutex);
        not_empty.wait(lock, [this]() { return buffer_list.size() > 0; });
        // has item on the queue to send
        int n = epoll_wait(epoll_fd_send_msg, events, EPOLL_MAXEVENTS, -1);
        // log_trace("epoll returned {} sockets ready for write", n);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLOUT) {
                // socket send buffer is available to send message
                site_id_t site_id = sockfd_to_server_site_id_map[events[i].data.fd];
                // log_trace("send buffer is available for site {}.", site_id);
                auto offset = last_sent_seqno[site_id] - last_all_sent_seqno;
                if(offset == buffer_list.size()) {
                    // all messages on the buffer have been sent for this site_id
                    continue;
                }
                // auto pos = (offset + head) % n_slots;
                auto node = buffer_list.front(); //!!!!! dangerous, a copy of char*
                size_t payload_size = node.message_size;
                auto requestType = node.message_type;
                auto version = node.message_version;
                // decode paylaod_size in the beginning
                // memcpy(&payload_size, buf[pos].get(), sizeof(size_t));
                auto curr_seqno = last_sent_seqno[site_id] + 1;
                write_callback_store[curr_seqno] = node.WRC;
                // log_info("sending msg {} to site {}.", curr_seqno, site_id);
                // send over socket
                // time_keeper[curr_seqno*4+site_id-1] = now_us();
                sock_write(events[i].data.fd, RequestHeader{requestType, version, version, local_site_id, payload_size});
                if (payload_size)
                    sock_write(events[i].data.fd, node.message_body, payload_size);
                leave_queue_time_keeper[curr_seqno * 4 + site_id - 1001] = get_time_us();
                // buffer_size[curr_seqno] = size;
                last_sent_seqno[site_id] = curr_seqno;
            }
        }

        // static_cast<uint64_t>(-1) will simpliy the logic in the above loop
        // but we need to be careful when computing min_element, since it's actually 0xFFFFFFF
        // but we still want -1 to be the min element.
        auto it = std::min_element(last_sent_seqno.begin(), last_sent_seqno.end(),
                                   [](const auto& p1, const auto& p2) { 
                                           if (p1.second == static_cast<uint64_t>(-1)) {return true;} 
                                           else {return p1.second < p2.second;} });

        // log_debug("smallest seqno in last_sent_seqno is {}", it->second);
        // dequeue from ring buffer
        // || min_element == 0 will skip the comparison with static_cast<uint64_t>(-1)
        if(it->second > last_all_sent_seqno || (last_all_sent_seqno == static_cast<uint64_t>(-1) && it->second == 0)) {
            // log_info("{} has been sent to all remote sites, ", it->second);
            // assert(it->second - last_all_sent_seqno == 1);
            // std::unique_lock<std::mutex> list_lock(list_mutex);
            size_mutex.lock();
            buffer_list.pop_front();
            // list_lock.lock();
            size_mutex.unlock();
            // list_lock.unlock();
            last_all_sent_seqno++;
        }
        lock.unlock();
    }

    log_exit_func();
}

void MessageSender::read_msg_loop() {
    log_enter_func();
    auto tid = pthread_self();
    // std::cout << "read_msg_loop start, tid = " << tid << std::endl;
    struct epoll_event events[EPOLL_MAXEVENTS];
    while(!thread_shutdown.load()) {
        std::unique_lock<std::mutex> lock(read_mutex);
        read_not_empty.wait(lock, [this]() { return read_buffer_list.size() > 0; });
        int n = epoll_wait(epoll_fd_read_msg, events, EPOLL_MAXEVENTS, -1);
        for(int i = 0; i < n; i++) {
            if(events[i].events & EPOLLOUT) {
                site_id_t site_id = R_sockfd_to_server_site_id_map[events[i].data.fd];
                auto offset = R_last_sent_seqno[site_id] - R_last_all_sent_seqno;
                if(offset == read_buffer_list.size()) {
                    continue;
                }
                auto node = read_buffer_list.front();
                size_t payload_size = node.message_size;
                auto requestType = node.message_type;
                auto version = node.message_version;
                auto curr_seqno = R_last_sent_seqno[site_id] + 1;
                read_callback_store[curr_seqno] = node.RRC;
                sock_write(events[i].data.fd, RequestHeader{requestType, version, curr_seqno, local_site_id, payload_size});
                if (payload_size) {
                    throw std::runtime_error("Something went wrong with read requests");
                }
                R_last_sent_seqno[site_id] = curr_seqno;
            }
        }

        auto it = std::min_element(R_last_sent_seqno.begin(), R_last_sent_seqno.end(),
                                   [](const auto& p1, const auto& p2) { 
                                           if (p1.second == static_cast<uint64_t>(-1)) {return true;} 
                                           else {return p1.second < p2.second;} });

        if(it->second > R_last_all_sent_seqno || (R_last_all_sent_seqno == static_cast<uint64_t>(-1) && it->second == 0)) {
            assert(it->second - R_last_all_sent_seqno == 1);
            read_size_mutex.lock();
            read_buffer_list.pop_front();
            read_size_mutex.unlock();
            R_last_all_sent_seqno++;
        }
        lock.unlock();
    }

    log_exit_func();
}

WanAgentSender::WanAgentSender(const nlohmann::json& wan_group_config,
                               const PredicateLambda& pl,
                               std::string log_level)
        : WanAgentAbstract(wan_group_config, log_level),
          has_new_ack(false),
          predicate_lambda(pl) {
    // std::string pss = "MIN($1,MAX($2,$3))";
    predicate_experssion = wan_group_config[WAN_AGENT_PREDICATE];
    // inverse_predicate_expression = reverser::get_inverse_predicate(predicate_experssion);
    std::istringstream iss(predicate_experssion);
    // std::istringstream i_iss(inverse_predicate_expression);
    predicate_generator = new Predicate_Generator(iss);
    // inverse_predicate_generator = new Predicate_Generator(i_iss);
    predicate = predicate_generator->get_predicate_function();
    // inverse_predicate = inverse_predicate_generator->get_predicate_function();
    // std::cout << predicate_experssion << std::endl;
    // std::cout << inverse_predicate_expression << std::endl;

    std::string new_type_expression = wan_group_config["new_predicate"];
    std::istringstream new_type_iss(new_type_expression);
    new_type_predicate_generator = new Predicate_Generator(new_type_iss, wan_group_config);
    new_type_predicate = predicate_generator->get_new_predicate_function();

    // start predicate thread.
    // predicate_thread = std::thread(&WanAgentSender::predicate_loop, this);
    for(const auto& pair : server_sites_ip_addrs_and_ports) {
        // if(local_site_id != pair.first) {
            message_counters[pair.first] = 0;
            // read_message_counters[pair.first] = 0;
        // }
    }

    message_sender = std::make_unique<MessageSender>(
            local_site_id,
            server_sites_ip_addrs_and_ports,
            wan_group_config[WAN_AGENT_WINDOW_SIZE],  // TODO: useless after using linked list
            wan_group_config[WAN_AGENT_MAX_PAYLOAD_SIZE],
            message_counters,
            message_counters_for_types,
            // read_message_counters,
            [this]() {});
    // [this]() { this->report_new_ack(); });
    // generate_predicate();
    // generate_predicate(wan_group_config);
    init_postfix(wan_group_config);
    recv_ack_thread = std::thread(&MessageSender::recv_ack_loop, message_sender.get());
    send_msg_thread = std::thread(&MessageSender::send_msg_loop, message_sender.get());
    recv_read_ack_thread = std::thread(&MessageSender::recv_read_ack_loop, message_sender.get());
    read_msg_thread = std::thread(&MessageSender::read_msg_loop, message_sender.get());
    // message_sender->set_read_quorum(message_counters.size()/2+1);
    message_sender->predicate = predicate;
    message_sender->new_type_predicate = new_type_predicate;
    message_sender->new_predicate_map["default"] = new_type_predicate;
    new_predicate_map["default"] = new_type_predicate;
    message_sender->new_predicate_arrive_map["default"] = 0;
    // message_sender->inverse_predicate = inverse_predicate;
}

// void WanAgentSender::report_new_ack()
// {
//     log_enter_func();
//     std::unique_lock lck(new_ack_mutex);
//     has_new_ack = true;
//     lck.unlock();
//     new_ack_cv.notify_all();
//     log_exit_func();
// }

void WanAgentSender::submit_predicate(std::string key, std::string predicate_str, bool inplace) {
    std::istringstream iss(predicate_str);
    predicate_generator = new Predicate_Generator(iss, config);
    predicate_fn_type prl = predicate_generator->get_predicate_function();
    if(inplace) {
        predicate = prl;
        message_sender->predicate = predicate;
        std::cerr << "current predicate :" << key << std::endl;
    }
    predicate_map[key] = prl;
    message_sender->predicate_map[key] = prl;
    // std::cerr << "----------------------------------" << std::endl;
    // for(auto it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //     std::cerr << "we have predicate :" << it->first << std::endl;
    // }
    
    if(key == "Complicated") {
        std::vector<pre_operation> pre_vec(std::begin(predicate_generator->driver.operations), std::end(predicate_generator->driver.operations));
        message_sender->operations = pre_vec;
        message_sender->complicate_predicate = prl;
        std::cout << "sender's operation: " << message_sender->operations.size() << std::endl;
    }
    // test_predicate();
}

void WanAgentSender::submit_new_predicate(std::string key, std::string predicate_str, bool inplace) {
    std::istringstream new_type_iss(predicate_str);
    new_type_predicate_generator = new Predicate_Generator(new_type_iss, config);
    new_type_predicate = predicate_generator->get_new_predicate_function();

    if(inplace) {
        new_type_predicate = new_type_predicate;
        message_sender->new_type_predicate = new_type_predicate;
        std::cerr << "current new predicate :" << key << std::endl;
    }

    new_predicate_map[key] = new_type_predicate;
    message_sender->new_predicate_map[key] = new_type_predicate;
    message_sender->new_predicate_arrive_map[key] = 0;
}

void WanAgentSender::print_predicate_map(){
    for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
        std::cout << it->first << std::endl;
    }
}

void WanAgentSender::set_read_quorum(int read_quorum){
    message_sender->set_read_quorum(read_quorum);
}

void WanAgentSender::init_postfix(const nlohmann::json& config){
    int idx = 1;
    for(auto& pf : config["postfix"]){
        message_sender->ack_type_id[pf] = idx++;
    }
}

void WanAgentSender::generate_predicate(const nlohmann::json& config) {
    // generate predicates for different kinds of ACK type
    for(auto& predicate_tmp : config[WAN_AGENT_PREDICATES]) {
        submit_predicate(predicate_tmp["key"], predicate_tmp["value"],0);
        init_predicate_counter(predicate_tmp["key"], config);
        // std::cout << "key " << predicate_tmp["key"] << ",value: " << predicate_tmp["value"] << std::endl;
    }
    // print_predicate_map();
}

void WanAgentSender::init_predicate_counter(std::string key, const nlohmann::json& config) {
    message_counters_for_types[key] = std::map<site_id_t, std::atomic<uint64_t>>();
    for(const auto& pair : server_sites_ip_addrs_and_ports) {
        // if(local_site_id != pair.first) {
            message_counters_for_types[key][pair.first] = 0;
        // }
    }
    // std::cout << key << " " << message_counters_for_types[key].size() << std::endl;
}



void WanAgentSender::generate_test_predicate() {
    // origin
    // std::string predicates[6] = {
    //         "MAX($1,$2,$3,$4,$5,$6,$7)",
    //         "MAX(MAX($2,$3,$4),MAX($5,$6),$7)",
    //         "KTH_MIN($2,MAX($2,$3,$4),MAX($5,$6),$7)",
    //         "MIN(MAX($2,$3,$4),MAX($5,$6),$7)",
    //         "MIN($1,$2,$3,$4,$5,$6,$7)",
    //         "KTH_MIN($4,$1,$2,$3,$4,$5,$6,$7)"};
    // "KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10),KTH_MIN($3,$1, $4,$2,$3,$5,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10,$6, $7,$8,$9,$10))"};
    
    // new
    std::string predicates[6] = {
            "MAX($1,$2,$3,$4,$5,$6,$7)",
            "MAX(MAX($2,$3,$4,$5),$6,$7)",
            "KTH_MIN($2,MAX($2,$3,$4,$5),$6,$7)",
            "MIN(MAX($2,$3,$4,$5),$6,$7)",
            "MIN($1,$2,$3,$4,$5,$6,$7)",
            "KTH_MIN($4,$1,$2,$3,$4,$5,$6,$7)"};
    std::string keys[6] = {
            "MAX_NODE",
            "MAX_REGION",
            "MAJ_REGION",
            "MIN_REGION",
            "MIN_NODE",
            "MAJ_NODE"};
    // "Complicated"};
    for(int i = 0; i < 6; i++) {
        submit_predicate(keys[i], predicates[i], false);
    }
}

void WanAgentSender::set_stability_frontier(int sf) {
    message_sender->wait_target_sf = sf;
    std::cout << "msg senderwaiting for " << message_sender->wait_target_sf << std::endl;
    wait_sf_thread = std::thread(&MessageSender::wait_stability_frontier_loop, message_sender.get(), sf, "default");
}

void WanAgentSender::wait_for(int sequnce_number, std::string predicate_key){
    if (new_predicate_map.count(predicate_key) == 0)
    {
        std::cerr << "no such key!\n";
        return;
    }
    wait_sf_threads.push_back(std::thread(&MessageSender::wait_stability_frontier_loop, message_sender.get(), sequnce_number, predicate_key));
    // wait_sf_thread = std::thread(&MessageSender::wait_stability_frontier_loop, message_sender.get(), sequnce_number, predicate_key);
}

void WanAgentSender::monitor_stability_frontier(MonitorCallback mc, std::string predicate_key) {
    monitor_sf_threads.push_back(std::thread(&MessageSender::monitor_stability_frontier_loop, message_sender.get(), predicate_key, mc));
    monitor_sf_threads[monitor_sf_threads.size()-1].detach();
}

uint64_t WanAgentSender::get_stability_frontier_arrive_time() {
    std::unique_lock<std::mutex> lock(message_sender->stability_frontier_set_mutex);
    message_sender->stability_frontier_set_cv.wait(lock, [this]() { return message_sender->sf_arrive_time != 0; });
    return message_sender->sf_arrive_time;
}

int WanAgentSender::get_stability_frontier() {
    return message_sender->stability_frontier;
}

void WanAgentSender::change_predicate(std::string key) {
    log_debug("changing predicate to {}", key);
    if(predicate_map.find(key) != predicate_map.end()) {  // 0-success
        predicate = predicate_map[key];
        message_sender->predicate = predicate;
        std::cerr << "change success" << std::endl;
        log_debug("change success");
    } else {  //1-error
        log_debug("change failed");
        throw std::runtime_error(key + "predicate is not found");
    }
}

void WanAgentSender::test_predicate() {
    int arr[6] = {0, 3, 7, 1, 5, 9};
    for(auto it = predicate_map.begin(); it != predicate_map.end(); it++) {
        int val = it->second(5, arr);
        std::cout << "test_predicate " << it->first << " returned: " << val << std::endl;
    }
    int cur = predicate(5, arr);
    log_debug("current test_predicate returned: {}", cur);
}
void WanAgentSender::out_out_file() {
    // std::ofstream file("./enter_leave.csv");
    // if(file) {
    //     file << "enter_time,s1,s2,s3,s4,s5,s6,s7\n";
    //     for(int i = 0; i < message_sender->msg_idx; i++) {
    //         file << message_sender->enter_queue_time_keeper[i] << "," << message_sender->leave_queue_time_keeper[i * 7] << "," << message_sender->leave_queue_time_keeper[i * 7 + 1] << "," << message_sender->leave_queue_time_keeper[i * 7 + 2] << "," << message_sender->leave_queue_time_keeper[i * 7 + 3] << "," << message_sender->leave_queue_time_keeper[i * 7 + 4] << "," << message_sender->leave_queue_time_keeper[i * 7 + 5] << "," << message_sender->leave_queue_time_keeper[i * 7 + 6] << "\n";
    //     }
    // }
    // file.close();

    // std::ofstream file("./enter_leave.csv");
    // if(file) {
    //     file << "enter_time,utah2,wisc,clme,mass\n";
    //     for(int i = 0; i < message_sender->msg_idx; i++) {
    //         file << message_sender->enter_queue_time_keeper[i] << "," << message_sender->leave_queue_time_keeper[i * 4] << "," << message_sender->leave_queue_time_keeper[i * 4 + 1] << "," << message_sender->leave_queue_time_keeper[i * 4 + 2] << "," << message_sender->leave_queue_time_keeper[i * 4 + 3] << "\n";
    //     }
    // }
    // file.close();

    std::ofstream file("./enter_receive.csv");
    if(file) {
        file << "enter_time,utah2,wisc,clme,mass\n";
        for(int i = 0; i < message_sender->msg_idx; i++) {
            file << message_sender->enter_queue_time_keeper[i] << "," << message_sender->ack_keeper[i * 4] << "," << message_sender->ack_keeper[i * 4 + 1] << "," << message_sender->ack_keeper[i * 4 + 2] << "," << message_sender->ack_keeper[i * 4 + 3] << "\n";
        }
    }
    file.close();


    // std::ofstream file1("./all_sf.csv");
    // if(file1) {
    //     file1 << "timestamp,";
    //     for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //         file1 << it->first << ",";
    //     }
    //     file1 << "\n";
    //     for(int i = 0; i < message_sender->all_sf_tics; i++) {
    //         file1 << message_sender->all_sf_situation[i * 7] << "," << message_sender->all_sf_situation[i * 7 + 1] << "," << message_sender->all_sf_situation[i * 7 + 2] << "," << message_sender->all_sf_situation[i * 7 + 3] << "," << message_sender->all_sf_situation[i * 7 + 4] << "," << message_sender->all_sf_situation[i * 7 + 5] << "," << message_sender->all_sf_situation[i * 7 + 6] << "\n";
    //     }
    // }
    // file1.close();

    /**wait for certain file size, and record the first time it arrives**/
    // std::string file2name = "./" + std::to_string(message_sender->wait_target_sf + 1) + "_files_arrive_time.csv";
    // std::ofstream file2(file2name);
    // if(file2) {
    //     file2 << "predicate,timestamp\n";
    //     file2 << "start," << message_sender->enter_queue_time_keeper[0] << "\n";
    //     for(std::map<std::string, uint64_t>::iterator it = message_sender->predicate_arrive_map.begin(); it != message_sender->predicate_arrive_map.end(); it++) {
    //         file2 << it->first << "," << (it->second - message_sender->enter_queue_time_keeper[0]) / 1000000.0 << "\n";
    //     }
    // }
    // file2.close();

    /**record every message arrive to see each file's performance**/
    // std::ofstream file3("./all_sf_for_each_msg.csv");
    // if(file3) {
    //     file3 << "enter_time,";
    //     for(std::map<std::string, predicate_fn_type>::iterator it = predicate_map.begin(); it != predicate_map.end(); it++) {
    //         file3 << it->first << ",";
    //     }
    //     file3 << "who_is_max";
    //     file3 << "\n";
    //     for(int i = 0; i < message_sender->msg_idx; i++) {
    //         file3 << message_sender->enter_queue_time_keeper[i] << "," << message_sender->sf_arrive_time_keeper[i * 6] << "," << message_sender->sf_arrive_time_keeper[i * 6 + 1] << "," << message_sender->sf_arrive_time_keeper[i * 6 + 2] << "," << message_sender->sf_arrive_time_keeper[i * 6 + 3] << "," << message_sender->sf_arrive_time_keeper[i * 6 + 4] << "," << message_sender->sf_arrive_time_keeper[i * 6 + 5] << "," << message_sender->who_is_max[i] << "\n";
    //     }
    // }
    // file3.close();

    /**record ack to calculate the utility of the bandwidth**/
    // std::ofstream file4("./ack_keeper_bandwidth.csv");
    // if(file4) {
    //     file4 << "time,s1,s2,s3,s4,s5,s6,s7\n";
    //     file4 << message_sender->enter_queue_time_keeper[0] << ",";
    //     for(int i = 0; i < 7; i++) {
    //         file4 << message_sender->ack_keeper[i] << ",";
    //     }
    //     file4 << "\n";
    // }
    // file4.close();
}

void WanAgentSender::shutdown_and_wait() {
    // std::cout << "all done! " << (get_time_us() - enter_queue_time_keeper[0]) << std::endl;
    // std::cout << "all done used " << (message_sender->sf_arrive_time - message_sender->enter_queue_time_keeper[0]) / 1000000.0 << std::endl;
    // std::cout << "new sf cal cost " << message_sender->sf_calculation_cost / 1000.0 << std::endl;
    // std::cout << "sf cal cost " << message_sender->sf_calculation_cost / 100000.0 << std::endl;
    // std::cout << "total sf cal cost " << message_sender->transfer_data_cost / 100000.0 << std::endl;
    // std::cout << "per latency " << ((message_sender->sf_arrive_time - message_sender->enter_queue_time_keeper[0]) / 1000000.0) / 100000 << std::endl;
    log_enter_func();
    is_shutdown.store(true);
    // report_new_ack(); // to wake up all predicate_loop threads with a pusedo "new ack"
    // predicate_thread.join();
    // out_out_file();
    message_sender->shutdown();
    // send_msg_thread.join();
    // recv_ack_thread.join();
    std::cout << "send_msg_thread.joinable(): " << send_msg_thread.joinable() << ", recv_ack_thread.joinable(): " << recv_ack_thread.joinable() << std::endl;
    send_msg_thread.detach();
    recv_ack_thread.detach();
    wait_sf_thread.detach();
    log_exit_func();
}

}  // namespace wan_agent
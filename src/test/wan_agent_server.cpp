#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/HLC.hpp>
#include <derecho/persistent/Persistent.hpp>
#include <derecho/openssl/signature.hpp>
#include <derecho/persistent/detail/util.hpp>
#include <algorithm>
#include <atomic>
#include <arpa/inet.h>
#include <condition_variable>
#include <fstream>
#include <list>
#include <iostream>
#include <map>
#include <mutex>
#include <utility>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <wan_agent.hpp>
using std::cout;
using std::endl;
using std::cerr;
using namespace wan_agent;
using namespace persistent;

/**** Key-Version Pairs ****/
std::mutex all_lock;
std::map<uint64_t, version_t> seq_versions;
uint64_t max_version;

int main(int argc, char** argv) {
    // TODO: should use code in wan_agent/wan_agent.hpp, not duplicated code.

    if(argc < 2) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "wan_agent configuration file" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    std::string json_config = argv[1];
    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    auto max_payload_size = conf[WAN_AGENT_MAX_PAYLOAD_SIZE];
    persistent::PersistentRegistry pr(nullptr, typeid(Blob), 0, 0);
    persistent::Persistent<Blob> pblob([max_payload_size](){ return std::make_unique<Blob>(nullptr, max_payload_size); }, 
                                       "Pblob", 
                                       &pr, 
                                       false);

    int ops_ctr = 0;

    std::string obj = "";
    for (int i = 0; i < 1000; ++i) obj += 'a';

    wan_agent::RemoteMessageCallback rmc = [&](const RequestHeader& RH, const char* msg) {
        // cout << "message received from site:" << RH.site_id
        //           << ", message size:" << RH.payload_size << " bytes"
        //           << ", message version:" << RH.version
        //           << endl;
        if (RH.requestType == 1) {
            all_lock.lock();
            //version_t prev_version = pblob.getLatestVersion();
            //version_t cur_version = prev_version + 1;
            // cerr << "cur_version = " << cur_version << endl;
            //(*pblob) = std::move(Blob(msg, RH.payload_size));
            //pblob.version(cur_version);
            //seq_versions[RH.version] = cur_version;
            //assert(max_version < RH.version);
            ++ops_ctr;
            if (ops_ctr % 1000 == 0) std::cerr << ops_ctr << std::endl;
            //max_version = RH.version;
            //pblob.persist(cur_version);
            all_lock.unlock();
            return std::make_pair(RH.version, std::move(Blob("done", 4)));
        } else {
            all_lock.lock();
            ++ops_ctr;
            if (ops_ctr % 1000 == 0) std::cerr << ops_ctr << std::endl;
            all_lock.unlock();
            //if (RH.version == (uint64_t)-1) {
            //    auto cur_version = max_version;
            //    all_lock.unlock();
            //    return std::make_pair(cur_version, std::move(*(pblob).get(cur_version)));
            //} else if (seq_versions.find(RH.version) == seq_versions.end()) {
            //    all_lock.unlock();
            //    return std::make_pair((uint64_t)-1, std::move(Blob("OBJ_NOT_FOUND",13)));
            ///}
            //uint64_t cur_version = seq_versions[RH.version];
            //all_lock.unlock();
            //return std::make_pair(RH.version, std::move(*(pblob.get(cur_version))));
	        return std::make_pair((uint64_t)0, std::move(Blob(obj.c_str(), 1000)));
        }
    };

    wan_agent::WanAgentServer w_server(conf, rmc);
    return 0;
}

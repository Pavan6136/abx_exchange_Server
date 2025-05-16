#include <iostream>
#include <vector>
#include <map>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include "json.hpp"
using json = nlohmann::json;

struct Packet {
    std::string symbol;
    char buysellindicator;
    int32_t quantity;
    int32_t price;
    int32_t packetSequence;
};


int connect_to_server(const std::string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Error creating socket" << std::endl;
        return -1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address/Address not supported" << std::endl;
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Connection failed" << std::endl;
        close(sock);
        return -1;
    }

    return sock;
}

bool send_request(int sock, uint8_t call_type, uint8_t resend_seq = 0) {
    uint8_t request[2] = {call_type, resend_seq};
    if (send(sock, request, sizeof(request), 0) < 0) {
        std::cerr << "Send failed" << std::endl;
        return false;
    }
    return true;
}

std::vector<uint8_t> receive_data(int sock) {
    std::vector<uint8_t> buffer(1024);
    std::vector<uint8_t> result;
    
    while (true) {
        ssize_t bytes_received = recv(sock, buffer.data(), buffer.size(), 0);
        if (bytes_received <= 0) {
            break;
        }
        result.insert(result.end(), buffer.begin(), buffer.begin() + bytes_received);
    }
    
    return result;
}

std::vector<Packet> parse_packets(const std::vector<uint8_t>& data) {
    std::vector<Packet> packets;
    const size_t packet_size = 17;
    
    for (size_t i = 0; i + packet_size <= data.size(); i += packet_size) {
        Packet packet;
        
        packet.symbol = std::string(data.begin() + i, data.begin() + i + 4);
        
        packet.buysellindicator = static_cast<char>(data[i + 4]);
        
        packet.quantity = ntohl(*reinterpret_cast<const int32_t*>(&data[i + 5]));
        
        packet.price = ntohl(*reinterpret_cast<const int32_t*>(&data[i + 9]));
        
        packet.packetSequence = ntohl(*reinterpret_cast<const int32_t*>(&data[i + 13]));
        
        packets.push_back(packet);
    }
    
    return packets;
}

std::vector<int32_t> find_missing_sequences(const std::vector<Packet>& packets) {
    if (packets.empty()) return {};
    
    std::vector<int32_t> missing;
    int32_t max_seq = packets.back().packetSequence;
    
    std::map<int32_t, bool> sequence_map;
    for (const auto& packet : packets) {
        sequence_map[packet.packetSequence] = true;
    }
    
    for (int32_t i = 1; i <= max_seq; ++i) {
        if (!sequence_map[i]) {
            missing.push_back(i);
        }
    }
    
    return missing;
}

std::vector<Packet> request_missing_packets(const std::string& host, int port, const std::vector<int32_t>& missing_sequences) {
    std::vector<Packet> missing_packets;
    
    for (int32_t seq : missing_sequences) {
        int sock = connect_to_server(host, port);
        if (sock < 0) continue;
        
        if (send_request(sock, 2, static_cast<uint8_t>(seq))) {
            auto data = receive_data(sock);
            auto packets = parse_packets(data);
            if (!packets.empty()) {
                missing_packets.push_back(packets[0]);
            }
        }
        
        close(sock);
    }
    
    return missing_packets;
}

void write_to_json(const std::vector<Packet>& packets, const std::string& filename) {
    json j;
    
    for (const auto& packet : packets) {
        json packet_json;
        packet_json["symbol"] = packet.symbol;
        packet_json["buysellindicator"] = std::string(1, packet.buysellindicator);
        packet_json["quantity"] = packet.quantity;
        packet_json["price"] = packet.price;
        packet_json["packetSequence"] = packet.packetSequence;
        
        j.push_back(packet_json);
    }
    
    std::ofstream outfile(filename);
    outfile << std::setw(4) << j << std::endl;
}

int main() {
    const std::string host = "localhost";
    const int port = 3000;
    const std::string output_file = "output.json";
    
    int sock = connect_to_server(host, port);
    if (sock < 0) {
        return 1;
    }
    
    if (!send_request(sock, 1)) {
        close(sock);
        return 1;
    }
    
    auto initial_data = receive_data(sock);
    close(sock);
    
    auto packets = parse_packets(initial_data);
    
    auto missing_sequences = find_missing_sequences(packets);
    if (!missing_sequences.empty()) {
        auto missing_packets = request_missing_packets(host, port, missing_sequences);
        packets.insert(packets.end(), missing_packets.begin(), missing_packets.end());
    }
    
    std::sort(packets.begin(), packets.end(), [](const Packet& a, const Packet& b) {
        return a.packetSequence < b.packetSequence;
    });
    
    write_to_json(packets, output_file);
    
    std::cout << "Data successfully written to " << output_file << std::endl;
    
    return 0;
}

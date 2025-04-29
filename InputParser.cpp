#include "InputParser.h"
#include <iostream>
#include <fstream>
#include <regex>
#include <sstream>
#include <thread>
#include <future>
#include <vector>
#include <mutex>

std::vector<Triple> InputParser::parseNTriples(const std::string& filename) {
    std::vector<Triple> triples;
    std::ifstream file(filename);
    // std::cout << "Parsing file: " << filename << std::endl;
    std::string line;
    std::regex tripleRegex(R"(<([^>]+)> <([^>]+)> (\"[^\"]*\"|<[^>]+>|_:.*) \.)");
    // 主语： <uri>
    // 谓语： <uri>
    // 宾语： <uri> 或 "literal" 或 _:blankNode

    while (std::getline(file, line)) {
        // std::cout << line << std::endl;
        std::smatch match;
        if (std::regex_match(line, match, tripleRegex)) {
            std::string subject = match[1].str();
            std::string predicate = match[2].str();
            std::string object = match[3].str();
            triples.emplace_back(subject, predicate, object);
        }
    }

    return triples;
}

std::vector<Triple> InputParser::parseTurtle(const std::string& filename) {
    std::vector<Triple> triples;
    std::ifstream file(filename);
    std::string line;

    // 正则表达式匹配三元组和前缀声明
    std::regex tripleRegex(R"((<[^>]+>|_:.*|[^:]+:[^ ]+)\s+(<[^>]+>|[^:]+:[^ ]+)\s+(\"[^\"]*\"|<[^>]+>|_:.*|[^:]+:[^ ]+)\s*\.)");
    std::regex prefixRegex(R"(@prefix\s+([^:]+):\s+<([^>]+)>\s*\.)");

    // 全局前缀映射表
    std::map<std::string, std::string> prefixMap;

    // 第一步：预处理前缀声明
    while (std::getline(file, line)) {
        line = std::regex_replace(line, std::regex(R"(^\s+|\s+$)"), "");
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::smatch prefixMatch;
        if (std::regex_match(line, prefixMatch, prefixRegex)) {
            std::string prefixName = prefixMatch[1].str();
            std::string prefixUri = prefixMatch[2].str();
            prefixMap[prefixName] = prefixUri;
        }

        // 当匹配到三元组时说明已经读到数据部分，停止读取前缀声明
        std::smatch tripleMatch;
        if (std::regex_match(line, tripleMatch, tripleRegex)) {
            break;
        }
    }

    // 重置文件流
    file.clear();
    file.seekg(0, std::ios::beg);

    // 第二步：多线程处理三元组
    std::vector<std::string> lines;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }

    const size_t numThreads = std::thread::hardware_concurrency();
    size_t chunkSize = lines.size() / numThreads;
    std::vector<std::vector<Triple>> threadResults(numThreads);
    std::mutex resultMutex;

    auto parseChunk = [&](size_t start, size_t end, size_t threadIndex) {
        std::vector<Triple> localTriples;
        for (size_t i = start; i < end; ++i) {
            std::string line = lines[i];
            line = std::regex_replace(line, std::regex(R"(^\s+|\s+$)"), "");

            if (line.empty() || line[0] == '#' || std::regex_match(line, prefixRegex)) {
                continue;
            }

            std::smatch tripleMatch;
            if (std::regex_match(line, tripleMatch, tripleRegex)) {
                auto expandPrefix = [&prefixMap](const std::string& term) -> std::string {
                    size_t colonPos = term.find(':');
                    if (colonPos != std::string::npos) {
                        std::string prefix = term.substr(0, colonPos);
                        if (prefixMap.find(prefix) != prefixMap.end()) {
                            return prefixMap[prefix] + term.substr(colonPos + 1);
                        }
                    }
                    return term;
                };

                std::string subject = expandPrefix(tripleMatch[1].str());
                std::string predicate = expandPrefix(tripleMatch[2].str());
                std::string object = expandPrefix(tripleMatch[3].str());
                localTriples.emplace_back(subject, predicate, object);
            }
        }

        std::lock_guard<std::mutex> lock(resultMutex);
        threadResults[threadIndex] = std::move(localTriples);
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? lines.size() : start + chunkSize;
        threads.emplace_back(parseChunk, start, end, i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (const auto& threadResult : threadResults) {
        triples.insert(triples.end(), threadResult.begin(), threadResult.end());
    }

    return triples;
}

std::vector<Triple> InputParser::parseCSV(const std::string& filename) {
    std::vector<Triple> triples;
    std::ifstream file(filename);
    std::string line;

    while (std::getline(file, line)) {
        std::istringstream ss(line);
        std::string subject, predicate, object;

        if (std::getline(ss, subject, ',') &&
            std::getline(ss, predicate, ',') &&
            std::getline(ss, object, ',')) {
            triples.emplace_back(subject, predicate, object);
        }
    }

    return triples;
}

std::vector<Rule> InputParser::parseDatalogFromFile(const std::string &filename) {
    std::vector<Rule> rules;
    std::ifstream file(filename);
    std::string line;

    // 正则表达式匹配 PREFIX 声明
    std::regex prefixRegex(R"(PREFIX\s+([^:]+):\s+<([^>]+)>)");
    // 正则表达式匹配规则（支持有无前缀的情况）
    std::regex ruleRegex(R"(([\w:]+\([^)]+\)) :- (.+)\.)");
    std::regex tripleRegex(R"(([\w:]+)\(([^,]+), ([^)]+)\))");

    // 前缀映射表
    std::map<std::string, std::string> prefixMap;

    while (std::getline(file, line)) {
        // 去除行首尾空白字符
        line = std::regex_replace(line, std::regex(R"(^\s+|\s+$)"), "");

        // 跳过空行和注释
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // 匹配 PREFIX 声明
        std::smatch prefixMatch;
        if (std::regex_match(line, prefixMatch, prefixRegex)) {
            std::string prefixName = prefixMatch[1].str();
            std::string prefixUri = prefixMatch[2].str();
            prefixMap[prefixName] = prefixUri;
            continue;
        }

        // 匹配规则
        std::smatch ruleMatch;
        if (std::regex_match(line, ruleMatch, ruleRegex)) {
            std::string headStr = ruleMatch[1].str();
            std::string bodyStr = ruleMatch[2].str();

            // 展开前缀的辅助函数
            auto expandPrefix = [&prefixMap](const std::string& term) -> std::string {
                size_t colonPos = term.find(':');
                if (colonPos != std::string::npos) {
                    std::string prefix = term.substr(0, colonPos);
                    if (prefixMap.find(prefix) != prefixMap.end()) {
                        return prefixMap[prefix] + term.substr(colonPos + 1);
                    }
                }
                return term; // 如果没有前缀，直接返回原始值
            };

            // 解析规则头部
            std::smatch headMatch;
            auto matchResult = std::regex_match(headStr, headMatch, tripleRegex);
            Triple head(
                headMatch[2].str(),
                expandPrefix(headMatch[1].str()),
                headMatch[3].str()
            );

            // 解析规则体
            std::vector<Triple> body;
            std::sregex_iterator bodyBegin(bodyStr.begin(), bodyStr.end(), tripleRegex);
            std::sregex_iterator bodyEnd;
            for (std::sregex_iterator i = bodyBegin; i != bodyEnd; ++i) {
                const std::smatch& bodyMatch = *i;
                body.emplace_back(
                    bodyMatch[2].str(),
                    expandPrefix(bodyMatch[1].str()),
                    bodyMatch[3].str()
                );
            }

            rules.emplace_back("", body, head);
        }
    }

    return rules;
}
#include <algorithm>
#include <map>
#include <set>
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <future>
#include "DatalogEngine.h"

void DatalogEngine::initiateRulesMap() {
    // 建立规则关于规则体中各模式三元组的谓语的索引，方便迭代中用三元组触发规则的应用
    for (const auto& rule : rules) {
        for (const auto& triple : rule.body) {
            if (isVariable(triple.predicate)) {
                // 变量不作为索引
                continue;
            }
            std::string predicate = triple.predicate;

            if (rulesMap.find(predicate) == rulesMap.end()) {
                // 如果当前谓语不在map中，则添加
                // rulesMap[predicate] = std::vector<std::pair<size_t, size_t>>();  // 规则下标，规则体中谓语下标
                rulesMap[predicate] = std::vector<size_t>();  // 规则下标
            }

            // 由于按照下标遍历，故每个vector中最后一个元素最大，只需检测是否小于当前规则下标即可防止重复
            if (!rulesMap[predicate].empty() && rulesMap[predicate].back() >= &rule - &rules[0]) {
                continue;  // 已存在当前规则下标
            }
            // if (!rulesMap[predicate].empty() && rulesMap[predicate].back().first >= &rule - &rules[0]) {
            //     continue;  // 已存在当前规则下标
            // }

            // // 向map中谓语对应的规则下标列表中添加当前规则的下标以及该谓语在规则体中的下标
            // rulesMap[predicate].emplace_back(&rule - &rules[0], &triple - &rule.body[0]);
            // 向map中谓语对应的规则下标列表中添加当前规则的下标
            rulesMap[predicate].emplace_back(&rule - &rules[0]);

        }
    }
}


// void DatalogEngine::reason() {
//     bool newFactAdded = false;
//     int epoch = 0;
//     do {
//         std::cout << epoch++ << std::endl;
//         // 重置标志
//         newFactAdded = false;
//
//         // 根据自底向上，遍历规则，逐条应用
//         for (const auto& rule : rules) {
//             std::vector<Triple> newFacts;
//             // leapfrogTriejoin(store.getTriePSORoot(), rule, newFacts);
//             leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts);
//
//             if (!newFacts.empty()) {
//                 for (const auto& triple : newFacts) {
//                     if (std::find(store.getAllTriples().begin(), store.getAllTriples().end(), triple) == store.getAllTriples().end()) {
//                         // 若存储结构中不存在，则将当前事实加入，并标记有新事实加入
//                         store.addTriple(triple);
//                         newFactAdded = true;
//
//                         // std::cout << "New fact added: " << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
//                     }
//                 }
//             }
//
//         }
//     } while (newFactAdded);  // 若有新事实加入，则继续推理，否则结束
// }

void DatalogEngine::reason() {
    bool newFactAdded = false;
    int epoch = 0;
    do {
        std::cout << "Epoch: " << epoch++ << std::endl;
        newFactAdded = false;

        // 创建线程池
        std::vector<std::future<std::vector<Triple>>> futures;
        std::mutex storeMutex;

        int ruleId = 0;
        for (const auto& rule : rules) {
            std::cout << "Applying rule: " << ruleId++ << std::endl;
            // 使用 std::async 异步执行规则
            futures.push_back(std::async(std::launch::async, [&]() {
                std::vector<Triple> newFacts;
                leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts);
                return newFacts;
            }));
        }

        // 收集线程结果并合并
        for (auto& future : futures) {
            std::vector<Triple> newFacts = future.get();
            std::lock_guard<std::mutex> lock(storeMutex);
            for (const auto& triple : newFacts) {
                if (std::find(store.getAllTriples().begin(), store.getAllTriples().end(), triple) == store.getAllTriples().end()) {
                    store.addTriple(triple);
                    newFactAdded = true;
                }
            }
        }

    } while (newFactAdded);
}

bool DatalogEngine::isVariable(const std::string& term) {
    // 判断是否为变量，变量以?开头，如"?x"
    return !term.empty() && term[0] == '?';
}

void DatalogEngine::leapfrogTriejoin(TrieNode* psoRoot, TrieNode* posRoot, const Rule& rule, std::vector<Triple>& newFacts) {

    std::set<std::string> variables;
    std::map<std::string, std::vector<std::pair<int, int>>> varPositions; // 变量 -> [(triple_idx, position)]

    for (int i = 0; i < rule.body.size(); i++) {
        const Triple& triple = rule.body[i];
        if (isVariable(triple.subject)) {
            variables.insert(triple.subject);
            varPositions[triple.subject].emplace_back(i, 0); // 0 表示主语位置
        }
        if (isVariable(triple.predicate)) {
            variables.insert(triple.predicate);
            varPositions[triple.predicate].emplace_back(i, 1); // 1 表示谓语位置
            // 实际基本不考虑谓语为变量的情况，但以防万一还是加上
        }
        if (isVariable(triple.object)) {
            variables.insert(triple.object);
            varPositions[triple.object].emplace_back(i, 2); // 2 表示宾语位置
        }
    }

    std::map<std::string, std::string> bindings;
    // 对每个变量进行leapfrog join
    join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, 0, newFacts);
}

void DatalogEngine::join_by_variable(
    TrieNode* psoRoot, TrieNode* posRoot,
    const Rule& rule,  // 当前规则
    const std::set<std::string>& variables,  // 当前规则的变量全集
    const std::map<std::string, std::vector<std::pair<int, int>>>& varPositions,  // 变量 -> [(变量所在三元组模式在规则体中的下标, 主0/谓1/宾2)]
    std::map<std::string, std::string>& bindings,  // 变量 -> 变量当前的绑定值（常量，未绑定则为空）
    int varIdx,
    std::vector<Triple>& newFacts
) {
    // 当所有变量都已绑定时，生成新的事实
    if (varIdx >= variables.size()) {
        std::string newSubject = substituteVariable(rule.head.subject, bindings);
        std::string newPredicate = substituteVariable(rule.head.predicate, bindings);
        std::string newObject = substituteVariable(rule.head.object, bindings);

        newFacts.emplace_back(newSubject, newPredicate, newObject);
        return;
    }

    // 获取当前要处理的变量
    auto varIt = variables.begin();
    std::advance(varIt, varIdx);
    std::string currentVar = *varIt;

    // 对当前变量创建迭代器
    std::vector<TrieIterator*> iterators;
    for (const auto& pos : varPositions.at(currentVar)) {
        int tripleIdx = pos.first;
        int position = pos.second;
        const Triple& triple = rule.body[tripleIdx];

        TrieIterator* it = nullptr;

        // 根据变量位置选择适当的Trie
        if (position == 0) { // 主语位置
            // 如果宾语已绑定，就从posTrie中对应宾语的子节点中查找
            if (!isVariable(triple.object) || bindings.find(triple.object) != bindings.end()) {
                it = new TrieIterator(posRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    // 对已确定的宾语进行seek
                    TrieIterator objIt = it->open();
                    std::string objValue = substituteVariable(triple.object, bindings);
                    objIt.seek(objValue);
                    if (!objIt.atEnd() && objIt.key() == objValue) {
                        iterators.push_back(new TrieIterator(objIt.open()));
                    }
                }
            }
            // 否则，从psoTrie中查找
            else {
                it = new TrieIterator(psoRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    TrieIterator subIt = it->open();
                    iterators.push_back(new TrieIterator(subIt));
                }
            }
        }
        // 这里暂时不考虑谓语为变量，即position == 1的情况
        else if (position == 2) { // 宾语位置
            // 如果主语已绑定，就从psoTrie中对应主语的子节点中查找
            if (!isVariable(triple.subject) || bindings.find(triple.subject) != bindings.end()) {
                it = new TrieIterator(psoRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    // 对已确定的主语进行seek
                    TrieIterator subjIt = it->open();
                    std::string subjValue = substituteVariable(triple.subject, bindings);
                    subjIt.seek(subjValue);
                    if (!subjIt.atEnd() && subjIt.key() == subjValue) {
                        iterators.push_back(new TrieIterator(subjIt.open()));
                    }
                }
            }
            // 否则，从posTrie中查找
            else {
                it = new TrieIterator(posRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    TrieIterator objIt = it->open();
                    iterators.push_back(new TrieIterator(objIt));
                }
            }
        }

        if (it && iterators.empty()) {
            delete it;
        }
    }

    // 对当前变量执行leapfrog join
    if (!iterators.empty()) {
        LeapfrogJoin lf(iterators);
        while (!lf.atEnd()) {
            std::string key = lf.key();
            bindings[currentVar] = key;

            // 递归处理下一个变量
            join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, varIdx + 1, newFacts);

            lf.next();
        }

        // 清理迭代器
        for (auto it : iterators) {
            delete it;
        }
    }

    // 删除当前变量的绑定
    bindings.erase(currentVar);
}

// 辅助函数：若绑定中存在变量则替换其绑定的值，否则返回原字符串（此时为常量）
std::string DatalogEngine::substituteVariable(const std::string& term, const std::map<std::string, std::string>& bindings) {
    if (isVariable(term) && bindings.find(term) != bindings.end()) {
        return bindings.at(term);
    }
    return term;
}

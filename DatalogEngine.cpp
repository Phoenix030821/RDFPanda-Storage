#include <algorithm>
#include <map>
#include <iostream>
#include "DatalogEngine.h"

void DatalogEngine::reason() {
    bool newFactAdded = false;
    do {
        // 重置标志
        newFactAdded = false;

        // 根据自底向上，遍历规则，逐条应用
        for (const auto& rule : rules) {

            ////////////// 旧版本代码 //////////////
            /*
            // 应用当前规则
            std::vector<Triple> inferredTriples = applyRule(rule);
            // 将新事实加入存储
            for (const auto& triple : inferredTriples) {
                if (std::find(store.getAllTriples().begin(), store.getAllTriples().end(), triple) == store.getAllTriples().end()) {
                    // 若存储结构中不存在，则将当前事实加入，并标记有新事实加入
                    store.addTriple(triple);
                    newFactAdded = true;

                    // std::cout << "New fact added: " << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
                }
            }
            */
            //////////////////////////////////////

            ////////////// leapfrogTriejoin版本 //////////////
            std::vector<Triple> newFacts;
            leapfrogTriejoin(store.getTrieRoot(), rule, newFacts);

            if (!newFacts.empty()) {
                for (const auto& triple : newFacts) {
                    if (std::find(store.getAllTriples().begin(), store.getAllTriples().end(), triple) == store.getAllTriples().end()) {
                        // 若存储结构中不存在，则将当前事实加入，并标记有新事实加入
                        store.addTriple(triple);
                        newFactAdded = true;

                        // std::cout << "New fact added: " << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
                    }
                }
            }

        }
    } while (newFactAdded);  // 若有新事实加入，则继续推理，否则结束
}

std::vector<Triple> DatalogEngine::applyRule(const Rule& rule) {
//    std::vector<Triple> result;
//    //todo: 实现规则匹配与应用逻辑
//
//    std::vector<Triple> body = rule.body;
//    Triple head = rule.head;
//    std::vector<Triple> allTriples = store.getAllTriples(); // 暂时取所有事实进行匹配，之后可优化
//
//    for (const auto& triple : allTriples) {
//        std::map<std::string, std::string> variableBindings;
//        bool allPatternsMatch = true;
//
//        for (const auto& bodyTriple : rule.body) {
//            // 匹配规则body中的每个三元组
//            bool patternMatch = false;
//            for (const auto& candidateTriple : allTriples) {
//                if (matchTriple(candidateTriple, bodyTriple, variableBindings)) {
//                    patternMatch = true;
//                    break;
//                }
//            }
//            if (!patternMatch) {
//                allPatternsMatch = false;
//                break;
//            }
//        }
//
//        if (allPatternsMatch) {
//            Triple instantiatedHead = instantiateTriple(rule.head, variableBindings);
//            result.push_back(instantiatedHead);
//        }
//    }
//
//    return result;

    // 初始绑定集合，包含一个空绑定
    std::vector<std::map<std::string, std::string>> bindings;
    bindings.emplace_back();

    // 针对规则体中每个三元组模式
    for (const auto& pattern : rule.body) {
        std::vector<std::map<std::string, std::string>> newBindings;

        // 根据模式中的常量选择候选三元组
        std::vector<Triple> candidateTriples;
        // 如果subject是常量，则用subject索引
        if (!isVariable(pattern.subject)) {
            candidateTriples = store.queryBySubject(pattern.subject);
        }
        // 否则，如果predicate是常量，则用predicate索引
        else if (!isVariable(pattern.predicate)) {
            candidateTriples = store.queryByPredicate(pattern.predicate);
        }
        // 否则，如果object是常量，则用object索引
        else if (!isVariable(pattern.object)) {
            candidateTriples = store.queryByObject(pattern.object);
        }
        // 如果都为变量，则遍历所有事实
        else {
            candidateTriples = store.getAllTriples();
        }

        // 对当前已有的每组绑定，尝试匹配候选三元组
        for (const auto& binding : bindings) {
            for (const auto& fact : candidateTriples) {
                std::map<std::string, std::string> localBindings = binding;
                if (matchTriple(fact, pattern, localBindings)) {
                    newBindings.push_back(localBindings);
                }
            }
        }

        bindings = std::move(newBindings);
        if (bindings.empty()) {
            // 当前规则体模式无法匹配到任何事实，直接返回空
            return {};
        }
    }

    // 利用完整的绑定实例化规则头，生成新推导出的事实
    std::vector<Triple> newFacts;
    for (const auto& binding : bindings) {
        Triple inferred = instantiateTriple(rule.head, binding);
        newFacts.push_back(inferred);
    }
    return newFacts;
}

bool DatalogEngine::matchTriple(const Triple& triple, const Triple& pattern, std::map<std::string, std::string>& variableBindings) {
    // 匹配三元组与模式，参数中triple为事实三元组，pattern为规则三元组模式，variableBindings为变量绑定
    for (int i = 0; i < 3; ++i) {
        // const std::string& tripleElement = (i == 0) ? triple.subject : (i == 1) ? triple.predicate : triple.object;
        // const std::string& patternElement = (i == 0) ? pattern.subject : (i == 1) ? pattern.predicate : pattern.object;
        const std::string& tripleElement = getElem(triple, i);
        const std::string& patternElement = getElem(pattern, i);

        if (isVariable(patternElement)) {
            std::string var = patternElement;
            if (variableBindings.find(var) == variableBindings.end()) {
                variableBindings[var] = tripleElement;
            } else if (variableBindings[var] != tripleElement) {
                return false;
            }
        } else if (patternElement != tripleElement) {
            return false;
        }
    }
    // 匹配成功则返回true
    // std::cout << "Matched: " << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    return true;
}

Triple DatalogEngine::instantiateTriple(const Triple& pattern, const std::map<std::string, std::string>& variableBindings) {
    // 实例化三元组，参数中pattern为规则中的三元组模式，variableBindings为变量绑定
    Triple result = pattern;
    for (int i = 0; i < 3; ++i) {
        const std::string& patternElement = getElem(pattern, i);
        if (isVariable(patternElement)) {
            if (i == 0) {
                result.subject = variableBindings.at(patternElement);
            } else if (i == 1) {
                result.predicate = variableBindings.at(patternElement);
            } else {
                result.object = variableBindings.at(patternElement);
            }
        }
    }
    return result;
}

bool DatalogEngine::isVariable(const std::string& term) {
    // 判断是否为变量，变量以?开头，如"?x"
    return !term.empty() && term[0] == '?';
}

std::string DatalogEngine::getElem(const Triple& triple, int i) {
    // 根据i返回三元组的主语(0)、谓语(1)或宾语(2)
    return (i == 0) ? triple.subject : (i == 1) ? triple.predicate : triple.object;
}

// 递归实现 Leapfrog Triejoin：
void DatalogEngine::leapfrogTriejoin(TrieNode* trieRoot, const Rule& rule, std::vector<Triple>& newFacts) {
    std::vector<std::vector<TrieIterator*>> iterators;

    // 为规则 body 中的每个三元组创建 Trie 迭代器
    for (const Triple& condition : rule.body) {
        TrieIterator* it = new TrieIterator(trieRoot);
        it->seek(condition.predicate);  // 只查找匹配的谓语
        if (!it->atEnd()) {
            iterators.push_back({it});
        } else {
            delete it;
        }
    }

    if (iterators.size() != rule.body.size()) {
        // 规则的 body 没有完整匹配，无法推导新事实
        return;
    }

    std::vector<std::string> binding;
    join_recursive(iterators, rule, 0, binding, newFacts);

    // 释放迭代器
    for (auto& list : iterators) {
        for (TrieIterator* it : list) {
            delete it;
        }
    }
}

void DatalogEngine::join_recursive(std::vector<std::vector<TrieIterator *>> &iterators, const Rule &rule, int level,
                                   std::vector<std::string> &binding, std::vector<Triple> &newFacts) {
    if (level >= rule.body.size()) {
        // level已经到达body的最后一个三元组，当所有 body 条件都匹配后，生成新的 Triple
        std::string newSubject = binding[0];  // 变量替换规则
        std::string newPredicate = rule.head.predicate;
        std::string newObject = binding[1];  // 变量替换规则

        newFacts.emplace_back(newSubject, newPredicate, newObject);
        return;
    }

    // 当前层的 Leapfrog Join
    LeapfrogJoin lf(iterators[level]);
    while (!lf.atEnd()) {
        std::string key = lf.key();
        binding.push_back(key);

        // 进入下一层
        join_recursive(iterators, rule, level + 1, binding, newFacts);

        // binding.pop_back();
        lf.next();
    }
}

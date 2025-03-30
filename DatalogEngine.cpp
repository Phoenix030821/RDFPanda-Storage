#include <algorithm>
#include <map>
#include <set>
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
            // leapfrogTriejoin(store.getTriePSORoot(), rule, newFacts);
            leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts);

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

void DatalogEngine::leapfrogTriejoin(TrieNode* psoRoot, TrieNode* posRoot, const Rule& rule, std::vector<Triple>& newFacts) {
    // 1. 收集规则体中的所有唯一变量
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

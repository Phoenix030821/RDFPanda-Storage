#ifndef RDFPANDA_STORAGE_DATALOGENGINE_H
#define RDFPANDA_STORAGE_DATALOGENGINE_H

#include <vector>
#include <algorithm>
#include <map>
#include <set>

#include "TripleStore.h"


class DatalogEngine {
private:
    TripleStore& store;
    std::vector<Rule> rules;
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> rulesMap; // 谓语 -> [规则下标, 规则体中谓语下标]

public:
    DatalogEngine(TripleStore& store, const std::vector<Rule>& rules) : store(store), rules(rules) {
        initiateRulesMap();
    }
    void reason();

private:
    // std::vector<Triple> applyRule(const Rule& rule);
    // bool matchTriple(const Triple& triple, const Triple& pattern, std::map<std::string, std::string>& variableBindings);
    // Triple instantiateTriple(const Triple& triple, const std::map<std::string, std::string>& variableBindings);
    bool isVariable(const std::string& str);
    // std::string getElem(const Triple& triple, int i);

    void initiateRulesMap();

    void leapfrogTriejoin(TrieNode *psoRoot, TrieNode *posRoot, const Rule &rule, std::vector<Triple> &newFacts);

    void join_by_variable(TrieNode *psoRoot, TrieNode *posRoot, const Rule &rule,
                          const std::set<std::string> &variables,
                          const std::map<std::string, std::vector<std::pair<int, int>>> &varPositions,
                          std::map<std::string, std::string> &bindings, int varIdx, std::vector<Triple> &newFacts);

    std::string substituteVariable(const std::string &term, const std::map<std::string, std::string> &bindings);


    /*
    void leapfrogTriejoin(TrieNode* trieRoot, const Rule& rule, std::vector<Triple>& newFacts);
    void join_recursive(std::vector<TrieIterator*>& iterators,
                        const Rule& rule, int varIndex,
                        std::map<std::string, std::string>& binding, std::vector<Triple>& newFacts);
*/


};


#endif //RDFPANDA_STORAGE_DATALOGENGINE_H

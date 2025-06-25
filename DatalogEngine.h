#ifndef RDFPANDA_STORAGE_DATALOGENGINE_H
#define RDFPANDA_STORAGE_DATALOGENGINE_H

#include <vector>
#include <algorithm>
#include <map>
#include <set>

#include "TripleStore.h"


class DatalogEngine {
private:
    TripleStore& originalStore;
    TripleStore& store;
    std::vector<Rule> rules;
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> rulesMap; // 谓语 -> [规则下标, 规则体中谓语下标]
    // std::map<std::string, std::vector< size_t>> rulesMap; // 谓语 -> [规则下标]

public:
    DatalogEngine(TripleStore& store, const std::vector<Rule>& rules) : store(store), originalStore(store), rules(rules) {
        initiateRulesMap();
    }
    void reason();

    void reasonNaive();

    void leapfrogDRed(std::vector<Triple>& deletedFacts, std::vector<Triple>& insertedFacts);

private:
    // std::vector<Triple> applyRule(const Rule& rule);
    // bool matchTriple(const Triple& triple, const Triple& pattern, std::map<std::string, std::string>& variableBindings);
    // Triple instantiateTriple(const Triple& triple, const std::map<std::string, std::string>& variableBindings);
    static bool isVariable(const std::string& str);
    // std::string getElem(const Triple& triple, int i);

    void initiateRulesMap();

    void leapfrogTriejoin(TrieNode *psoRoot, TrieNode *posRoot, const Rule &rule,
                            std::vector<Triple> &newFacts,
                            std::map<std::string, std::string> &bindings);

    void leapfrogTriejoinBackwards(TrieNode *psoRoot, TrieNode *posRoot, const Rule &rule,
                                    std::vector<Triple> &newFacts,
                                    std::map<std::string, std::string> &bindings, Triple &currentTriple);

    void join_by_variable(TrieNode *psoRoot, TrieNode *posRoot, const Rule &rule,
                          const std::set<std::string> &variables,
                          const std::map<std::string, std::vector<std::pair<int, int>>> &varPositions,
                          std::map<std::string, std::string> &bindings, int varIdx, std::vector<Triple> &newFacts);

    static std::string substituteVariable(const std::string &term, const std::map<std::string, std::string> &bindings);

    bool checkConflictingTriples(const std::map<std::string, std::string>& bindings,
                                    const std::map<std::string, std::vector<std::pair<int, int>>>& varPositions,
                                    const Rule& rule) const;

    void overdeleteDRed(std::vector<Triple> &overdeletedFacts, std::vector<Triple> deletedFacts);

    void insertDRed(std::vector<Triple> newFacts);

    /*
    void leapfrogTriejoin(TrieNode* trieRoot, const Rule& rule, std::vector<Triple>& newFacts);
    void join_recursive(std::vector<TrieIterator*>& iterators,
                        const Rule& rule, int varIndex,
                        std::map<std::string, std::string>& binding, std::vector<Triple>& newFacts);
*/


};


#endif //RDFPANDA_STORAGE_DATALOGENGINE_H

#ifndef RDFPANDA_STORAGE_DATALOGENGINE_H
#define RDFPANDA_STORAGE_DATALOGENGINE_H

#include <vector>
#include <algorithm>
#include <map>
#include <set>

#include "TripleStore.h"


class DatalogEngine {
private:
    TripleStore originalStore;
    TripleStore& store;
    std::map<Triple, int> recursiveNum;
    std::map<Triple, int> nonrecursiveNum;
    std::vector<Rule> rules;
    std::vector<Rule> recursiveRules;
    std::vector<Rule> nonrecursiveRules;
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> rulesMap; // 谓语 -> [规则下标, 规则体中谓语下标]
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> nonrecursiveRulesMap; // 谓语 -> [规则下标, 规则体中谓语下标]
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> recursiveRulesMap; // 谓语 -> [规则下标, 规则体中谓语下标]
    
    // std::map<std::string, std::vector< size_t>> rulesMap; // 谓语 -> [规则下标]

public:
    DatalogEngine(TripleStore& store, const std::vector<Rule>& rules) : store(store), originalStore(store), rules(rules) {
        // 将规则分为递归和非递归
        for (const auto& rule : rules) {
            bool isRecursive = false;
            const std::string& headPredicate = rule.head.predicate;
            for (const auto& triple : rule.body) {
                if (triple.predicate == headPredicate) {
                    isRecursive = true;
                    break;
                }
            }
            if(isRecursive) {
                recursiveRules.push_back(rule);
            } else {
                nonrecursiveRules.push_back(rule);
            }
        }

        initiateRulesMap();
        initiateCounting();
    }
    void reason();

    void reasonNaive();

    void leapfrogDRed(std::vector<Triple>& deletedFacts, std::vector<Triple>& insertedFacts);

    void leapfrogDRedCounting(std::vector<Triple>& deletedFacts, std::vector<Triple>& insertedFacts);

private:
    // std::vector<Triple> applyRule(const Rule& rule);
    // bool matchTriple(const Triple& triple, const Triple& pattern, std::map<std::string, std::string>& variableBindings);
    // Triple instantiateTriple(const Triple& triple, const std::map<std::string, std::string>& variableBindings);
    static bool isVariable(const std::string& str);
    // std::string getElem(const Triple& triple, int i);

    void initiateRulesMap();

    void initiateCounting();

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

    void insertDRed(std::vector<Triple> newFacts, std::vector<Triple> redrivedFacts);

    void overdeleteDRedCounting(std::vector<Triple> &overdeletedFacts, std::vector<Triple> deletedFacts);

    void insertDRedCounting(std::vector<Triple> newFacts, std::vector<Triple> redrivedFacts);

    /*
    void leapfrogTriejoin(TrieNode* trieRoot, const Rule& rule, std::vector<Triple>& newFacts);
    void join_recursive(std::vector<TrieIterator*>& iterators,
                        const Rule& rule, int varIndex,
                        std::map<std::string, std::string>& binding, std::vector<Triple>& newFacts);
*/


};


#endif //RDFPANDA_STORAGE_DATALOGENGINE_H

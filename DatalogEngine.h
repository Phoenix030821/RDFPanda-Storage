#ifndef RDFPANDA_STORAGE_DATALOGENGINE_H
#define RDFPANDA_STORAGE_DATALOGENGINE_H

#include <vector>
#include <algorithm>
#include <map>

#include "TripleStore.h"


class DatalogEngine {
private:
    TripleStore& store;
    std::vector<Rule> rules;

public:
    DatalogEngine(TripleStore& store, const std::vector<Rule>& rules) : store(store), rules(rules) {}
    void infer();

private:
    std::vector<Triple> applyRule(const Rule& rule);
    bool matchTriple(const Triple& triple, const Triple& pattern, std::map<std::string, std::string>& variableBindings);
    Triple instantiateTriple(const Triple& triple, const std::map<std::string, std::string>& variableBindings);
    bool isVariable(const std::string& str);
    std::string getElem(const Triple& triple, int i);
};


#endif //RDFPANDA_STORAGE_DATALOGENGINE_H

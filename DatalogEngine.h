#ifndef RDFPANDA_STORAGE_DATALOGENGINE_H
#define RDFPANDA_STORAGE_DATALOGENGINE_H

#include <vector>
#include <algorithm>

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
};


#endif //RDFPANDA_STORAGE_DATALOGENGINE_H

#ifndef RDFPANDA_STORAGE_INPUTPARSER_H
#define RDFPANDA_STORAGE_INPUTPARSER_H

#include <string>
#include <vector>
#include <tuple>

#include "TripleStore.h"

// using Triple = std::tuple<std::string, std::string, std::string>;

class InputParser {
public:
    std::vector<Triple> parseNTriples(const std::string& filename);
    std::vector<Triple> parseTurtle(const std::string& filename);
    std::vector<Triple> parseCSV(const std::string& filename);
};

#endif //RDFPANDA_STORAGE_INPUTPARSER_H
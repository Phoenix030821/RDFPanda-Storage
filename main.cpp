#include <iostream>
#include <fstream>
#include <string>

#include "InputParser.h"
#include "TripleStore.h"

//// 测试用，打印文件内容
void printFileContent(const std::string& filename) {
    std::ifstream file(filename);
    std::string line;
    while (std::getline(file, line)) {
        std::cout << line << std::endl;
    }
}

void parseNTFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseNTriples(filename);
    for (const auto& triple : triples) {
        std::cout << std::get<0>(triple) << " " << std::get<1>(triple) << " " << std::get<2>(triple) << std::endl;
    }
}

void parseTurtleFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseTurtle(filename);
    for (const auto& triple : triples) {
        std::cout << std::get<0>(triple) << " " << std::get<1>(triple) << " " << std::get<2>(triple) << std::endl;
    }
}

void parseCSVFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseCSV(filename);
    for (const auto& triple : triples) {
        std::cout << std::get<0>(triple) << " " << std::get<1>(triple) << " " << std::get<2>(triple) << std::endl;
    }
}

int main() {

    // printFileContent("input_examples/example.ttl");

    // parseNTFile("input_examples/example.nt");
    // parseTurtleFile("input_examples/example.ttl");

    InputParser parser;
    TripleStore store;

    std::vector<Triple> triples = parser.parseTurtle("input_examples/example.ttl");
    for (const auto& triple : triples) {
        store.addTriple(triple);
    }

    std::vector<Triple> queryResult = store.queryBySubject("http://example.org/Alice");
    for (const auto& triple : queryResult) {
        std::cout << std::get<0>(triple) << " " << std::get<1>(triple) << " " << std::get<2>(triple) << std::endl;
    }

    return 0;
}

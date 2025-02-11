#ifndef RDFPANDA_STORAGE_TRIPLESTORE_H
#define RDFPANDA_STORAGE_TRIPLESTORE_H

#include <vector>
#include <unordered_map>
#include <string>

//struct Triple {
//    std::string subject;
//    std::string predicate;
//    std::string object;
//};
using Triple = std::tuple<std::string, std::string, std::string>;

class TripleStore {
private:
    // 主存储：所有唯一的三元组
    std::vector<Triple> triples;

    // 多级索引
    std::unordered_map<std::string, std::vector<size_t>> subject_index;  // Subject → 主存储中的索引
    std::unordered_map<std::string, std::vector<size_t>> predicate_index; // Predicate → 索引
    std::unordered_map<std::string, std::vector<size_t>> object_index;    // Object → 索引

public:
    void addTriple(const Triple& triple);
    std::vector<Triple> queryBySubject(const std::string& subject);
    std::vector<Triple> queryByPredicate(const std::string& predicate);
    std::vector<Triple> queryByObject(const std::string& object);
    const std::vector<Triple>& getAllTriples() const { return triples; }
};

struct Rule {
    Triple head;
    std::vector<Triple> body;
};

#endif //RDFPANDA_STORAGE_TRIPLESTORE_H

#ifndef RDFPANDA_STORAGE_TRIPLESTORE_H
#define RDFPANDA_STORAGE_TRIPLESTORE_H

#include <utility>
#include <vector>
#include <unordered_map>
#include <string>

class Triple {
public:
    std::string subject;
    std::string predicate;
    std::string object;

    Triple(std::string subject, std::string predicate, std::string object)
            : subject(std::move(subject)), predicate(std::move(predicate)), object(std::move(object)) {}

    bool operator==(const Triple& rhs) const {
        return subject == rhs.subject && predicate == rhs.predicate && object == rhs.object;
    }

    bool operator!=(const Triple& rhs) const {
        return !(*this == rhs);
    }
};

class Rule {
public:
    std::string name;
    std::vector<Triple> body;
    Triple head;

    Rule(std::string  name, const std::vector<Triple>& body, Triple  head)
            : name(std::move(name)), body(body), head(std::move(head)) {}
};

class TripleStore {
private:
    // 主存储：所有唯一的三元组
    // todo: 最初版本先使用vector满足基本功能需求，后续需要优化成更高效的数据结构
    std::vector<Triple> triples;

    // 多级索引
    // todo: 最初版本配合主存储的vector，使用unordered_map存储以某string作为主/谓/宾语的所有三元组在vector中的下标，之后需要配合主存储优化
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

//struct Rule {
//    Triple head;
//    std::vector<Triple> body;
//};

#endif //RDFPANDA_STORAGE_TRIPLESTORE_H

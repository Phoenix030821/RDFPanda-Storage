#ifndef RDFPANDA_STORAGE_TRIPLESTORE_H
#define RDFPANDA_STORAGE_TRIPLESTORE_H

#include <utility>
#include <vector>
#include <unordered_map>
#include <string>

#include "Trie.h"

//// Triple 和 Rule 类已定义在Trie.h中

class TripleStore {
private:
    // 主存储：所有唯一的三元组
    // todo: 最初版本先使用vector满足基本功能需求，后续需要优化成更高效的数据结构
    std::vector<Triple> triples;
    // update: 使用Trie树优化
    Trie triePSO;
    Trie triePOS;

    // 多级索引
    // todo: 最初版本配合主存储的vector，使用unordered_map存储以某string作为主/谓/宾语的所有三元组在vector中的下标，之后需要配合主存储优化
    std::unordered_map<std::string, std::vector<size_t>> subject_index;  // Subject → 主存储中的索引
    std::unordered_map<std::string, std::vector<size_t>> predicate_index; // Predicate → 索引
    std::unordered_map<std::string, std::vector<size_t>> object_index;    // Object → 索引

public:
    void addTriple(const Triple& triple);
    void deleteTriple(const Triple& triple);
    std::vector<Triple> queryBySubject(const std::string& subject);
    std::vector<Triple> queryByPredicate(const std::string& predicate);
    std::vector<Triple> queryByObject(const std::string& object);
    const std::vector<Triple>& getAllTriples() const { return triples; }

    TrieNode* getNodeByTriple(const Triple& triple) const;

    TrieNode* getTriePSORoot() const { return triePSO.root; }
    TrieNode* getTriePOSRoot() const { return triePOS.root; }
};



#endif //RDFPANDA_STORAGE_TRIPLESTORE_H

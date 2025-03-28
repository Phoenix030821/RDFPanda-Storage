#include "TripleStore.h"

void TripleStore::addTriple(const Triple& triple) {
    // 检查是否已存在
    for (const auto& index : subject_index[triple.subject]) {
        if (triples[index].predicate == triple.predicate && triples[index].object == triple.object) {
            return;
        }
    }

    // 添加到主存储
    triples.push_back(triple);
    size_t index = triples.size() - 1;

    // 更新索引
    subject_index[triple.subject].push_back(index);
    predicate_index[triple.predicate].push_back(index);
    object_index[triple.object].push_back(index);

    // update: 使用Trie树优化
    triePSO.insertPSO(triple);
    triePOS.insertPOS(triple);
}

std::vector<Triple> TripleStore::queryBySubject(const std::string& subject) {
    // 返回主语为subject的所有三元组
    if (subject_index.find(subject) == subject_index.end()) {
        return {};
    }
    std::vector<Triple> result;
    for (size_t index : subject_index[subject]) {
        result.push_back(triples[index]);
    }
    return result;
}

std::vector<Triple> TripleStore::queryByPredicate(const std::string& predicate) {
    // 返回谓语为predicate的所有三元组
    if (predicate_index.find(predicate) == predicate_index.end()) {
        return {};
    }
    std::vector<Triple> result;
    for (size_t index : predicate_index[predicate]) {
        result.push_back(triples[index]);
    }
    return result;
}

std::vector<Triple> TripleStore::queryByObject(const std::string& object) {
    // 返回宾语为object的所有三元组
    if (object_index.find(object) == object_index.end()) {
        return {};
    }
    std::vector<Triple> result;
    for (size_t index : object_index[object]) {
        result.push_back(triples[index]);
    }
    return result;
}
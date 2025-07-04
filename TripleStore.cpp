#include "TripleStore.h"

void TripleStore::addTriple(const Triple& triple) {
    //  printf("Adding triple: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
    // 添加到vector
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

void TripleStore::deleteTriple(const Triple& triple) {
    // 从vector中删除三元组
    // printf("Deleting triple: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
    auto it = std::find(triples.begin(), triples.end(), triple);

    if (it != triples.end()) {
        size_t index = std::distance(triples.begin(), it);
        // triples.erase(it);
        // printf("Triple found at index: %zu\n", index);
        // 更新索引
        auto& subject_indices = subject_index[triple.subject];
        subject_indices.erase(std::remove(subject_indices.begin(), subject_indices.end(), index), subject_indices.end());
        if (subject_indices.empty()) {
            subject_index.erase(triple.subject);
        }

        auto& predicate_indices = predicate_index[triple.predicate];
        
        predicate_indices.erase(std::remove(predicate_indices.begin(), predicate_indices.end(), index), predicate_indices.end());
        if (predicate_indices.empty()) {
            predicate_index.erase(triple.predicate);
        }

        auto& object_indices = object_index[triple.object];
        object_indices.erase(std::remove(object_indices.begin(), object_indices.end(), index), object_indices.end());
        if (object_indices.empty()) {
            object_index.erase(triple.object);
        }

        // update: 使用Trie树优化
        triePSO.deletePSO(triple);
        triePOS.deletePOS(triple);
        // printf("\nDeleted triple: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
        // printf("Remaining triples: %zu\n", triples.size());
        // triePSO.printAll(); // 调试用，打印所有三元组
        // triePOS.printAll(); // 调试用，打印所有三元组
    }
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
        // printf("Found triple with predicate: %s at index: %zu\n", predicate.c_str(), index);
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

std::vector<Triple> TripleStore::getAllTriples() const {
    std::vector<Triple> allTriples;
    for(const auto& predicate : predicate_index) {
        for (size_t index : predicate.second) {
            allTriples.push_back(triples[index]);
        }
    }
    return allTriples;
}

TrieNode* TripleStore::getNodeByTriple(const Triple& triple) const {
    // 返回指定三元组的Trie节点
    TrieNode* node = triePSO.root;
    std::vector<std::string> keys = { triple.predicate, triple.subject, triple.object };
    for (const auto & key : keys) {
        if (node->children.find(key) == node->children.end()) {
            return nullptr;
        }
        node = node->children[key];
    }
    return node;
}
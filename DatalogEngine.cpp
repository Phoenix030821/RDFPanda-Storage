#include "DatalogEngine.h"

void DatalogEngine::infer() {
    bool newFactAdded = false;
    do {
        // 重置标志
        newFactAdded = false;

        // 根据自底向上，遍历规则，逐条应用
        for (const auto& rule : rules) {
            // 应用当前规则
            std::vector<Triple> inferredTriples = applyRule(rule);
            // 将新事实加入存储
            for (const auto& triple : inferredTriples) {
                if (std::find(store.getAllTriples().begin(), store.getAllTriples().end(), triple) == store.getAllTriples().end()) {
                    // 若存储结构中不存在，则将当前事实加入，并标记有新事实加入
                    store.addTriple(triple);
                    newFactAdded = true;
                }
            }
        }
    } while (newFactAdded);  // 若有新事实加入，则继续推理，否则结束
}

std::vector<Triple> DatalogEngine::applyRule(const Rule& rule) {
    std::vector<Triple> result;
    //todo: 实现规则匹配与应用逻辑
    result.push_back(rule.head);
    return result;
}
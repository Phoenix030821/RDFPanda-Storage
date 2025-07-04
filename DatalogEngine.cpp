#include <algorithm>
#include <map>
#include <set>
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <future>
#include "DatalogEngine.h"

#include <queue>

void DatalogEngine::initiateRulesMap() {
    // 建立规则关于规则体中各模式三元组的谓语的索引，方便迭代中用三元组触发规则的应用
    for (const auto& rule : rules) {
        for (const auto& triple : rule.body) {
            if (isVariable(triple.predicate)) {
                // 变量不作为索引
                continue;
            }
            std::string predicate = triple.predicate;

            if (rulesMap.find(predicate) == rulesMap.end()) {
                // 如果当前谓语不在map中，则添加
                rulesMap[predicate] = std::vector<std::pair<size_t, size_t>>();  // 规则下标，规则体中谓语下标
                // rulesMap[predicate] = std::vector<size_t>();  // 规则下标
            }

            // 由于按照下标遍历，故每个vector中最后一个元素最大，只需检测是否小于当前规则下标即可防止重复
            // if (!rulesMap[predicate].empty() && rulesMap[predicate].back() >= &rule - &rules[0]) {
            //     continue;  // 已存在当前规则下标
            // }

            // 向map中谓语对应的规则下标列表中添加当前规则的下标以及该谓语在规则体中的下标
            rulesMap[predicate].emplace_back(&rule - &rules[0], &triple - &rule.body[0]);
            // // 向map中谓语对应的规则下标列表中添加当前规则的下标
            // rulesMap[predicate].emplace_back(&rule - &rules[0]);

        }
    }

    for(const auto& rule : nonrecursiveRules) {
        for (const auto& triple : rule.body) {
            if (isVariable(triple.predicate)) {
                // 变量不作为索引
                continue;
            }
            std::string predicate = triple.predicate;

            if (nonrecursiveRulesMap.find(predicate) == nonrecursiveRulesMap.end()) {
                // 如果当前谓语不在map中，则添加
                nonrecursiveRulesMap[predicate] = std::vector<std::pair<size_t, size_t>>();  // 规则下标，规则体中谓语下标
            }

            // 向map中谓语对应的规则下标列表中添加当前规则的下标以及该谓语在规则体中的下标
            nonrecursiveRulesMap[predicate].emplace_back(&rule - &nonrecursiveRules[0], &triple - &rule.body[0]);
        }
    }
    for(const auto& rule : recursiveRules) {
        for (const auto& triple : rule.body) {
            if (isVariable(triple.predicate)) {
                // 变量不作为索引
                continue;
            }
            std::string predicate = triple.predicate;

            if (recursiveRulesMap.find(predicate) == recursiveRulesMap.end()) {
                // 如果当前谓语不在map中，则添加
                recursiveRulesMap[predicate] = std::vector<std::pair<size_t, size_t>>();  // 规则下标，规则体中谓语下标
            }

            // 向map中谓语对应的规则下标列表中添加当前规则的下标以及该谓语在规则体中的下标
            recursiveRulesMap[predicate].emplace_back(&rule - &recursiveRules[0], &triple - &rule.body[0]);
        }
    }
}

void DatalogEngine::initiateCounting() {
    std::vector<Triple> allTriples = store.getAllTriples();
    for (const auto& triple : allTriples) {
        if(nonrecursiveNum.find(triple) == nonrecursiveNum.end()) {
            nonrecursiveNum[triple] = 1;
        }
    }
}

void DatalogEngine::reason() {
    // bool newFactAdded = false;
    // int epoch = 0;

    // do {
    // std::cout << "Epoch: " << epoch++ << std::endl;
    // newFactAdded = false;
    std::queue<Triple> newFactQueue; // 存储新产生的事实，出队时触发对应规则的应用，并存到事实库中

    // 创建线程池
    std::vector<std::future<std::vector<Triple>>> futures;
    std::mutex storeMutex;

    std::atomic<int> reasonCount(0);

    // 先进行第一轮推理，初始时没有新事实，遍历规则逐条应用
    // int ruleId = 0;
    for (const auto& rule : rules) {
        // std::cout << "Applying rule: " << ruleId++ << std::endl;
        // 使用 std::async 异步执行规则
        reasonCount++;
        futures.push_back(std::async(std::launch::async, [&]() {
            std::vector<Triple> newFacts;
            std::map<std::string, std::string> bindings;
            leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts, bindings);
            return newFacts;
        }));
    }

    // 收集线程结果并合并
    for (auto& future : futures) {
        std::vector<Triple> newFacts = future.get();
        std::lock_guard<std::mutex> lock(storeMutex);
        for (const auto& triple : newFacts) {
            if (store.getNodeByTriple(triple) == nullptr) {
                // store.addTriple(triple);
                newFactQueue.push(triple);
                // newFactAdded = true;
            }
        }
    }


    std::atomic<int> activeTaskCount(0); // 活动任务计数器
    std::mutex queueMutex; // 保护队列的互斥锁

    ////////////////////////////////////
    /*
    while (!newFactQueue.empty() || activeTaskCount > 0) {
        reasonCount++;
        Triple currentTriple("", "", ""); // 当前三元组
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (!newFactQueue.empty()) {
                currentTriple = newFactQueue.front();
                newFactQueue.pop();
                activeTaskCount++; // 增加活动任务计数
            } else {
                // 队列为空但仍有活动任务，等待其他线程完成
                std::this_thread::yield();
                continue;
            }
        }

        // 将当前事实加入事实库
        {
            std::lock_guard<std::mutex> lock(storeMutex);
            if (store.getNodeByTriple(currentTriple) == nullptr) {
                store.addTriple(currentTriple);
                // newFactAdded = true;
            }
        }

        // 异步处理当前三元组
        std::future<void> future = std::async(std::launch::async, [&, currentTriple]() {
            // 根据rulesMap找到规则
            auto it = rulesMap.find(currentTriple.predicate);
            if (it != rulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = rules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = currentTriple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = currentTriple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings);

                    // 将新事实加入队列
                    {
                        std::lock_guard<std::mutex> lock(queueMutex);
                        for (const auto& fact : inferredFacts) {
                            // std::lock_guard<std::mutex> storeLock(storeMutex);
                            if (store.getNodeByTriple(fact) == nullptr) {
                                // store.addTriple(fact);
                                newFactQueue.push(fact);
                            }
                        }
                    }
                }
            }

            // 任务完成，减少活动任务计数
            activeTaskCount--;
        });

        // 确保任务完成
        future.get();
    }

    */
    /////////////////////////////////////

    std::atomic<bool> done(false);
    std::condition_variable cv;
    const auto threadCount = std::thread::hardware_concurrency();
    std::vector<std::thread> workers;

    // 工作线程
    auto worker = [&]() {
        while (true) {
            // reasonCount++;
            Triple currentTriple("", "", "");
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                cv.wait(lock, [&] { return !newFactQueue.empty() || done; });
                if (done && newFactQueue.empty()) break;
                currentTriple = newFactQueue.front();
                newFactQueue.pop();
                activeTaskCount++; // 增加活动任务计数
                // reasonCount++; // 统计推理次数
            }

            // 将当前事实加入事实库
            {
                std::lock_guard<std::mutex> lock(storeMutex);
                if (store.getNodeByTriple(currentTriple) == nullptr) {
                    store.addTriple(currentTriple);
                    // newFactAdded = true;
                    // reasonCount++;
                }
                reasonCount++;
            }

            // 处理 currentTriple，推理新事实并加锁入队
            // 根据rulesMap找到规则
            auto it = rulesMap.find(currentTriple.predicate);
            if (it != rulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = rules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = currentTriple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = currentTriple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings);
                    // reasonCount++;

                    // 将新事实加入队列
                    {
                        std::lock_guard<std::mutex> lock(queueMutex);
                        for (const auto& fact : inferredFacts) {
                            // std::lock_guard<std::mutex> storeLock(storeMutex);
                            if (store.getNodeByTriple(fact) == nullptr) {
                                // store.addTriple(fact);
                                newFactQueue.push(fact);
                                // reasonCount++;
                            }
                        }
                    }
                }
            }

            // 任务完成，减少活动任务计数
            activeTaskCount--;
        }
    };

    // 启动线程池
    workers.reserve(threadCount);
    for (int i = 0; i < threadCount; ++i) {
        workers.emplace_back(worker);
    }

    // 主线程不断唤醒工作线程
    while (true) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (newFactQueue.empty() && activeTaskCount == 0) {
                break;
            }
        }
        cv.notify_all();
        std::this_thread::yield();
    }

    // 结束所有线程
    done = true;
    cv.notify_all();
    for (auto& t : workers) t.join();


    // 输出推理完成后的事实库大小
    std::cout << "Total triples in store:           " << store.getAllTriples().size() << std::endl;
    // 输出总共推理的次数
    std::cout << "Total reasoning count:            " << reasonCount.load() << std::endl;
}

void DatalogEngine::reasonNaive() {

    std::queue<Triple> newFactQueue; // 存储新产生的事实，出队时触发对应规则的应用，并存到事实库中

    std::set<Triple> newFactsSet; // 用于去重新事实
    // 先进行第一轮推理，初始时没有新事实，遍历规则逐条应用
    // int ruleId = 0;
    for (const auto& rule : recursiveRules) {
        std::vector<Triple> newFacts;
        std::map<std::string, std::string> bindings;
        leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts, bindings);
        // printf("New facts derived from rule (%s): %zu\n", rule.name.c_str(), newFacts.size());
        // for(const auto& newFact : newFacts) {
        //     printf("(%s, %s, %s)\n", newFact.subject.c_str(), newFact.predicate.c_str(), newFact.object.c_str());
        // }
        for (const auto& triple : newFacts) {
            if (store.getNodeByTriple(triple) == nullptr) {
                // store.addTriple(triple);
                if(newFactsSet.find(triple) == newFactsSet.end()) {
                    newFactsSet.insert(triple);
                    newFactQueue.push(triple);
                    // printf("New fact added: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
                }
                if(recursiveNum.find(triple) == recursiveNum.end()) {
                    recursiveNum[triple] = 1;
                } else {
                    recursiveNum[triple]++;
                }
                // newFactAdded = true;
            }
        }
    }

    for (const auto& rule : nonrecursiveRules) {
        std::vector<Triple> newFacts;
        std::map<std::string, std::string> bindings;
        leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts, bindings);
        // printf("New facts derived from rule (%s): %zu\n", rule.name.c_str(), newFacts.size());
        // for(const auto& newFact : newFacts) {
        //     printf("(%s, %s, %s)\n", newFact.subject.c_str(), newFact.predicate.c_str(), newFact.object.c_str());
        // }
        for (const auto& triple : newFacts) {
            if (store.getNodeByTriple(triple) == nullptr) {
                // store.addTriple(triple);
                if(newFactsSet.find(triple) == newFactsSet.end()) {
                    newFactsSet.insert(triple);
                    newFactQueue.push(triple);
                    // printf("New fact added: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
                }
                if(nonrecursiveNum.find(triple) == nonrecursiveNum.end()) {
                    nonrecursiveNum[triple] = 1;
                } else {
                    nonrecursiveNum[triple]++;
                }
                // newFactAdded = true;
            }
        }
    }


    while (true) {
        // reasonCount++
        Triple currentTriple("", "", "");
        if (newFactQueue.empty()) break;
        currentTriple = newFactQueue.front();
        newFactQueue.pop();

        // 将当前事实加入事实库
        if (store.getNodeByTriple(currentTriple) == nullptr) {
            store.addTriple(currentTriple);
            // newFactAdded = true;
            // reasonCount++;
        }

        // 处理 currentTriple，推理新事实并加锁入队
        // 根据rulesMap找到规则
        auto it = recursiveRulesMap.find(currentTriple.predicate);
        if (it != recursiveRulesMap.end()) {
            for (const auto& rulePair : it->second) {
                size_t ruleIdx = rulePair.first;
                size_t patternIdx = rulePair.second;
                const Rule& rule = recursiveRules[ruleIdx];
                const Triple& pattern = rule.body[patternIdx];
                // printf("Applying rule: %s\n", rule.name.c_str());
                // printf("Pattern: (%s, %s, %s)\n", pattern.subject.c_str(), pattern.predicate.c_str(), pattern.object.c_str());
                // // 绑定变量
                std::map<std::string, std::string> bindings;
                if (isVariable(pattern.subject)) {
                    bindings[pattern.subject] = currentTriple.subject;
                }
                if (isVariable(pattern.object)) {
                    bindings[pattern.object] = currentTriple.object;
                }

                // 调用leapfrogTriejoin推理新事实
                std::vector<Triple> inferredFacts;
                leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings);

                for (const auto& fact : inferredFacts) {
                    // std::lock_guard<std::mutex> storeLock(storeMutex);
                    if (store.getNodeByTriple(fact) == nullptr) {
                        // store.addTriple(fact);
                        if(newFactsSet.find(fact) == newFactsSet.end()) {
                            newFactsSet.insert(fact);
                            newFactQueue.push(fact);
                            // printf("New fact added: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
                        }
                    }
                    // reasonCount++;
                    if(recursiveNum.find(fact) == recursiveNum.end()) {
                        recursiveNum[fact] = 1;
                    } else {
                        recursiveNum[fact]++;
                    }
                }
            }
        }

        it = nonrecursiveRulesMap.find(currentTriple.predicate);
        if (it != nonrecursiveRulesMap.end()) {
            for (const auto& rulePair : it->second) {
                size_t ruleIdx = rulePair.first;
                size_t patternIdx = rulePair.second;
                const Rule& rule = nonrecursiveRules[ruleIdx];
                const Triple& pattern = rule.body[patternIdx];
                // printf("Applying rule: %s\n", rule.name.c_str());
                // printf("Pattern: (%s, %s, %s)\n", pattern.subject.c_str(), pattern.predicate.c_str(), pattern.object.c_str());
                // // 绑定变量
                std::map<std::string, std::string> bindings;
                if (isVariable(pattern.subject)) {
                    bindings[pattern.subject] = currentTriple.subject;
                }
                if (isVariable(pattern.object)) {
                    bindings[pattern.object] = currentTriple.object;
                }

                // 调用leapfrogTriejoin推理新事实
                std::vector<Triple> inferredFacts;
                leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings);

                for (const auto& fact : inferredFacts) {
                    // std::lock_guard<std::mutex> storeLock(storeMutex);
                    if (store.getNodeByTriple(fact) == nullptr) {
                        // store.addTriple(fact);
                        if(newFactsSet.find(fact) == newFactsSet.end()) {
                            newFactsSet.insert(fact);
                            newFactQueue.push(fact);
                            // printf("New fact added: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
                        }
                        // reasonCount++;
                        if(nonrecursiveNum.find(fact) == nonrecursiveNum.end()) {
                            nonrecursiveNum[fact] = 1;
                        } else {
                            nonrecursiveNum[fact]++;
                        }
                    }
                }
            }
        }
    }

    // 输出推理完成后的事实库大小
    std::cout << "Total triples in store:           " << store.getAllTriples().size() << std::endl;
    // auto it = recursiveNum.begin();
    // std::cout << "Total recursive triples:          " << recursiveNum.size() << std::endl;
    // for(; it != recursiveNum.end(); it++) {
    //     std::cout << it->first.subject << " " << it->first.predicate << " " << it->first.object << " : " << it->second << std::endl;
    // }
    // it = nonrecursiveNum.begin();
    // std::cout << "Total nonrecursive triples:       " << nonrecursiveNum.size() << std::endl;
    // for(; it != nonrecursiveNum.end(); it++) {
    //     std::cout << it->first.subject << " " << it->first.predicate << " " << it->first.object << " : " << it->second << std::endl;
    // }

}


void DatalogEngine::leapfrogDRed(std::vector<Triple>& deletedFacts, std::vector<Triple>& insertedFacts) {
    // overdelete
    std::vector<Triple> overdeletedFacts;
    overdeleteDRed(overdeletedFacts, deletedFacts);
    printf("Overdeleted facts: %zu\n", overdeletedFacts.size());
    // for(const auto& fact: overdeletedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }

    // one-step redrive
    std::vector<Triple> redrivedFacts;
    for(auto& fact: overdeletedFacts) {
        if(originalStore.getNodeByTriple(fact) != nullptr) {
            redrivedFacts.push_back(fact);
            continue;
        }
        for(auto rule : rules) {
            if(rule.head.predicate != fact.predicate) {
                continue; // 只处理谓语匹配的规则
            }
            std::map<std::string, std::string> bindings;
            // 绑定变量
            if (isVariable(rule.head.subject)) {
                bindings[rule.head.subject] = fact.subject;
            }
            if (isVariable(rule.head.object)) {
                bindings[rule.head.object] = fact.object;
            }

            // 调用leapfrogTriejoin推理新事实
            std::vector<Triple> newFacts;
            leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, newFacts, bindings);
            // printf("New facts derived from rule (%s): %zu\n", rule.name.c_str(), newFacts.size());
            // for(const auto& newFact : newFacts) {
            //     printf("(%s, %s, %s)\n", newFact.subject.c_str(), newFact.predicate.c_str(), newFact.object.c_str());
            // }
            if(!newFacts.empty()) {
                if (store.getNodeByTriple(fact) == nullptr) {
                    redrivedFacts.push_back(fact);
                    break;
                }
            }
        }
    }

    
    printf("Redrived facts: %zu\n", redrivedFacts.size());
    // for(const auto& fact: redrivedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }
    // for(const auto& fact: redrivedFacts) {
    //     if (store.getNodeByTriple(fact) == nullptr) {
    //         store.addTriple(fact);
    //     }
    // }

    // insert
    insertDRed(insertedFacts, redrivedFacts);

    for(const auto& fact: deletedFacts) {
        originalStore.deleteTriple(fact);
    }
    
    for(const auto& fact: insertedFacts) {
        originalStore.addTriple(fact);
    }

    printf("Total triples in store: %zu\n", store.getAllTriples().size());

}

void DatalogEngine::overdeleteDRed(std::vector<Triple> &overdeletedFacts, std::vector<Triple> deletedFacts) {
    // D overdeletedFacts
    // N_D inferredFactsSet
    // delta_D deletedFacts
    // 对每个删除的事实，检查是否有规则可以应用
    std::set<Triple> overdeletedFactsSet;
    std::set<Triple> inferredFactsSet;
    
    // N_D = E-
    for(auto& fact: deletedFacts) {
        if (store.getNodeByTriple(fact) != nullptr) {
            inferredFactsSet.insert(fact);
        }
    }

    while(true) {
        std::vector<Triple> deltaD;
        // delta_D = N_D - D
        for(const auto& triple: inferredFactsSet) {
            if(overdeletedFactsSet.find(triple) == overdeletedFactsSet.end()) {
                deltaD.push_back(triple);
            }
        }
        // if delta_D = empty set   break
        if(deltaD.empty())
            break;

        inferredFactsSet.clear();
        // N_D = PI[I - D : delta_D]
        for (const auto& triple : deltaD) {
            // 根据谓语查找规则
            auto it = rulesMap.find(triple.predicate);
            if (it != rulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = rules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // printf("Pattern: (%s, %s, %s)\n", pattern.subject.c_str(), pattern.predicate.c_str(), pattern.object.c_str());
                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings); 
                    for(const auto& fact : inferredFacts) {
                        if (store.getNodeByTriple(fact) != nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }
        }
        // I -= delta_D
        for(const auto& fact: deltaD) {
            store.deleteTriple(fact);
        }
        // D = D U delta_D
        for (const auto& fact : deltaD) {
            //推理出的事实加入到overdeletedFacts
            overdeletedFactsSet.insert(fact);
        }
    }

    for(const auto& fact : overdeletedFactsSet) {
        // 将overdeletedFactsSet中的事实加入到overdeletedFacts中
        if (store.getNodeByTriple(fact) == nullptr) {
            overdeletedFacts.push_back(fact);
        }
    }
    
}

void DatalogEngine::insertDRed(std::vector<Triple> newFacts, std::vector<Triple> redrivedFacts) {
    // N_A = R + E+
    std::vector<Triple> allInsertedFacts;
    std::vector<Triple> insertedFacts;
    for(const auto& triple : newFacts) {
        insertedFacts.push_back(triple);
    }
    for(const auto& triple : redrivedFacts) {
        insertedFacts.push_back(triple);
    }
    while(true) {
        std::vector<Triple> deltaA;
        // delta_A = N_A - (I - D + A)
        for(const auto& triple: insertedFacts) {
            if (store.getNodeByTriple(triple) == nullptr) {
                deltaA.push_back(triple);
            }
        }
        // if delta_A = empty set   break
        if(deltaA.empty())
            break;
        printf("Delta A size: %zu\n", deltaA.size());
        // A = A U delta_A
        for (const auto& fact : deltaA) {
            if(store.getNodeByTriple(fact) == nullptr) {
                store.addTriple(fact);
                allInsertedFacts.push_back(fact);
            }
        }
        std::set<Triple> inferredFactsSet;
        for (const auto& triple : deltaA) {
            // 根据谓语查找规则
            auto it = rulesMap.find(triple.predicate);
            if (it != rulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = rules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings); 
                    for(const auto& fact : inferredFacts) {
                        if (store.getNodeByTriple(fact) == nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }
        }
        insertedFacts.clear();
        for(auto& fact : inferredFactsSet) {
            // 将推理出的事实加入到insertedFacts中
            if (store.getNodeByTriple(fact) == nullptr) {
                insertedFacts.push_back(fact);
            }
        }
    }

    printf("Inserted facts: %zu\n", allInsertedFacts.size());
    // for(const auto& fact: allInsertedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }

}

void DatalogEngine::leapfrogDRedCounting(std::vector<Triple>& deletedFacts, std::vector<Triple>& insertedFacts) {
    // overdelete
    std::vector<Triple> overdeletedFacts;
    overdeleteDRedCounting(overdeletedFacts, deletedFacts);
    printf("Overdeleted facts: %zu\n", overdeletedFacts.size());
    // for(const auto& fact: overdeletedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }
    // printf("Recursive facts count: %zu\n", recursiveNum.size());
    // for(auto &fact: recursiveNum) {
    //     printf("(%s, %s, %s) : %d\n", fact.first.subject.c_str(), fact.first.predicate.c_str(), fact.first.object.c_str(), fact.second);
    // }
    // printf("Nonrecursive facts count: %zu\n", nonrecursiveNum.size());
    // for(auto &fact: nonrecursiveNum) {
    //     printf("(%s, %s, %s) : %d\n", fact.first.subject.c_str(), fact.first.predicate.c_str(), fact.first.object.c_str(), fact.second);
    // }
    // one-step redrive
    std::vector<Triple> redrivedFacts;
    for(auto& fact: overdeletedFacts) {
        if(recursiveNum.find(fact) != recursiveNum.end() && recursiveNum[fact] > 0) {
            redrivedFacts.push_back(fact);
        }
    }

    
    printf("Redrived facts: %zu\n", redrivedFacts.size());
    // for(const auto& fact: redrivedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }
    // for(const auto& fact: redrivedFacts) {
    //     if (store.getNodeByTriple(fact) == nullptr) {
    //         store.addTriple(fact);
    //     }
    // }

    // insert
    insertDRedCounting(insertedFacts, redrivedFacts);

    for(const auto& fact: deletedFacts) {
        originalStore.deleteTriple(fact);
    }
    
    for(const auto& fact: insertedFacts) {
        originalStore.addTriple(fact);
    }
    
    printf("Total triples in store: %zu\n", store.getAllTriples().size());

}

void DatalogEngine::overdeleteDRedCounting(std::vector<Triple> &overdeletedFacts, std::vector<Triple> deletedFacts) {
    // D overdeletedFacts
    // N_D inferredFactsSet
    // delta_D deletedFacts
    // 对每个删除的事实，检查是否有规则可以应用
    std::multiset<Triple> overdeletedFactsSet;
    std::set<Triple> inferredFactsSet;
    
    // N_D = E-
    for(auto& fact: deletedFacts) {
        if (store.getNodeByTriple(fact) != nullptr) {
            inferredFactsSet.insert(fact);
            nonrecursiveNum[fact]--;
        }
    }

    while(true) {
        std::vector<Triple> deltaD;
        // delta_D = N_D - D
        for(const auto& triple: inferredFactsSet) {
            if(overdeletedFactsSet.find(triple) == overdeletedFactsSet.end() && nonrecursiveNum[triple] == 0) {
                deltaD.push_back(triple);
            }
        }
        // if delta_D = empty set   break
        if(deltaD.empty())
            break;

        inferredFactsSet.clear();
        // N_D = PI[I - D : delta_D]
        // printf("delta D: %zu\n", deltaD.size());
        for (const auto& triple : deltaD) {
            // printf("Processing triple: (%s, %s, %s)\n", triple.subject.c_str(), triple.predicate.c_str(), triple.object.c_str());
            // 根据谓语查找规则
            // nonrecursive
            auto it = nonrecursiveRulesMap.find(triple.predicate);
            if (it != nonrecursiveRulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = nonrecursiveRules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // printf("Pattern: (%s, %s, %s)\n", pattern.subject.c_str(), pattern.predicate.c_str(), pattern.object.c_str());
                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings); 
                    for(const auto& fact : inferredFacts) {
                        nonrecursiveNum[fact]--;
                        if (store.getNodeByTriple(fact) != nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }

            // recursive
            it = recursiveRulesMap.find(triple.predicate);
            if (it != recursiveRulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = recursiveRules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // printf("Pattern: (%s, %s, %s)\n", pattern.subject.c_str(), pattern.predicate.c_str(), pattern.object.c_str());
                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings);
                    // printf("Inferred facts size: %zu\n", inferredFacts.size()); 
                    for(const auto& fact : inferredFacts) {
                        // printf("Inferred fact: (%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
                        recursiveNum[fact]--;
                        if (store.getNodeByTriple(fact) != nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }
        }
        // I -= delta_D
        for(const auto& fact: deltaD) {
            store.deleteTriple(fact);
        }
        // D = D U delta_D
        for (const auto& fact : deltaD) {
            //推理出的事实加入到overdeletedFacts
            overdeletedFactsSet.insert(fact);
        }
    }

    for(const auto& fact : overdeletedFactsSet) {
        // 将overdeletedFactsSet中的事实加入到overdeletedFacts中
        if (store.getNodeByTriple(fact) == nullptr) {
            overdeletedFacts.push_back(fact);
        }
    }
    
}

void DatalogEngine::insertDRedCounting(std::vector<Triple> newFacts, std::vector<Triple> redrivedFacts) {
    // N_A = R + E+
    for(auto& fact : newFacts) {
        if (store.getNodeByTriple(fact) == nullptr) {
            if(nonrecursiveNum.find(fact) == nonrecursiveNum.end()) {
                nonrecursiveNum[fact] = 1;
            } else {
                nonrecursiveNum[fact]++;
            }
        }
    }
    std::vector<Triple> allInsertedFacts;
    std::vector<Triple> insertedFacts;
    for(const auto& triple : newFacts) {
        insertedFacts.push_back(triple);
    }
    for(const auto& triple : redrivedFacts) {
        insertedFacts.push_back(triple);
    }
    while(true) {
        std::vector<Triple> deltaA;
        // delta_A = N_A - (I - D + A)
        for(const auto& triple: insertedFacts) {
            if (store.getNodeByTriple(triple) == nullptr) {
                deltaA.push_back(triple);
            }
        }
        // if delta_A = empty set   break
        if(deltaA.empty())
            break;
        // A = A U delta_A
        for (const auto& fact : deltaA) {
            if(store.getNodeByTriple(fact) == nullptr) {
                store.addTriple(fact);
                allInsertedFacts.push_back(fact);
            }
        }
        std::set<Triple> inferredFactsSet;
        for (const auto& triple : deltaA) {
            // 根据谓语查找规则
            auto it = nonrecursiveRulesMap.find(triple.predicate);
            if (it != nonrecursiveRulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = nonrecursiveRules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings); 
                    for(const auto& fact : inferredFacts) {
                        if(nonrecursiveNum.find(fact) == nonrecursiveNum.end()) {
                            nonrecursiveNum[fact] = 1;
                        } else {
                            nonrecursiveNum[fact]++;
                        }
                        if (store.getNodeByTriple(fact) == nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }

            it = recursiveRulesMap.find(triple.predicate);
            if (it != recursiveRulesMap.end()) {
                for (const auto& rulePair : it->second) {
                    size_t ruleIdx = rulePair.first;
                    size_t patternIdx = rulePair.second;
                    const Rule& rule = recursiveRules[ruleIdx];
                    const Triple& pattern = rule.body[patternIdx];

                    // 绑定变量
                    std::map<std::string, std::string> bindings;
                    if (isVariable(pattern.subject)) {
                        bindings[pattern.subject] = triple.subject;
                    }
                    if (isVariable(pattern.object)) {
                        bindings[pattern.object] = triple.object;
                    }

                    // 调用leapfrogTriejoin推理新事实
                    std::vector<Triple> inferredFacts;
                    leapfrogTriejoin(store.getTriePSORoot(), store.getTriePOSRoot(), rule, inferredFacts, bindings); 
                    for(const auto& fact : inferredFacts) {
                        if(recursiveNum.find(fact) == recursiveNum.end()) {
                            recursiveNum[fact] = 1;
                        } else {
                            recursiveNum[fact]++;
                        }
                        if (store.getNodeByTriple(fact) == nullptr) {
                            inferredFactsSet.insert(fact);
                        }
                    }
                }
            }
        }
        insertedFacts.clear();
        for(auto& fact : inferredFactsSet) {
            // 将推理出的事实加入到insertedFacts中
            if (store.getNodeByTriple(fact) == nullptr) {
                insertedFacts.push_back(fact);
            }
        }
    }

    printf("Inserted facts: %zu\n", allInsertedFacts.size());
    // for(const auto& fact: allInsertedFacts) {
    //     printf("(%s, %s, %s)\n", fact.subject.c_str(), fact.predicate.c_str(), fact.object.c_str());
    // }

}
bool DatalogEngine::isVariable(const std::string& term) {
    // 判断是否为变量，变量以?开头，如"?x"
    return !term.empty() && term[0] == '?';
}

// 输入两棵trie，以及一条规则，将NewFacts里面填入推出的Facts
void DatalogEngine::leapfrogTriejoin(
    TrieNode* psoRoot, TrieNode* posRoot,
    const Rule& rule,
    std::vector<Triple>& newFacts,
    std::map<std::string, std::string>& bindings
) {

    std::set<std::string> variables;
    std::map<std::string, std::vector<std::pair<int, int>>> varPositions; // 变量 -> [(triple_idx, position)]
    // todo: 能不能根据varPositions来筛选代入新三元组对应变量后可能产生冲突的三元组模式？需要找出主语和宾语变量都包含在新三元组对应模式中的三元组模式
    // todo: 例如新三元组对应模式为A(?x,?y)，则需要找其他(?x,?y)、(?y,?x)、(?x,?x)、(?y,?y)的模式，并查询代入新值后的三元组是否存在于事实库中
    // todo: 相当于对于两个[(idx, pos)]数组，找出所有idx，使(idx, 0)和(idx, 2)都存在
    // update: 已实现
    // printf("Applying rule: %s\n", rule.name.c_str());
    // printf("bindings: ");
    // for (const auto& binding : bindings) {
    //     printf("%s -> %s, ", binding.first.c_str(), binding.second.c_str());
    // }
    // printf("\n");
    for (int i = 0; i < rule.body.size(); i++) {
        const Triple& triple = rule.body[i];
        if (isVariable(triple.subject)) {
            variables.insert(triple.subject);
            varPositions[triple.subject].emplace_back(i, 0); // 0 表示主语位置
        }
        if (isVariable(triple.predicate)) {
            variables.insert(triple.predicate);
            varPositions[triple.predicate].emplace_back(i, 1); // 1 表示谓语位置
            // 实际基本不考虑谓语为变量的情况，但以防万一还是加上
        }
        if (isVariable(triple.object)) {
            variables.insert(triple.object);
            varPositions[triple.object].emplace_back(i, 2); // 2 表示宾语位置
        }
    }

    if (!checkConflictingTriples(bindings, varPositions, rule)) {
        return;
    }

    // std::map<std::string, std::string> bindings;
    // 对每个变量进行leapfrog join
    join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, 0, newFacts);
}

// // 输入两棵trie，以及一条规则，将NewFacts里面填入推出的Facts
// void DatalogEngine::leapfrogTriejoinBackwards(
//     TrieNode* psoRoot, TrieNode* posRoot,
//     const Rule& rule,
//     std::vector<Triple>& newFacts,
//     std::map<std::string, std::string>& bindings,
//     Triple& currentTriple
// ) {

//     std::set<std::string> variables;
//     std::map<std::string, std::vector<std::pair<int, int>>> varPositions; // 变量 -> [(triple_idx, position)]

//     for (int i = 0; i < rule.body.size(); i++) {
//         const Triple& triple = rule.body[i];
//         if (isVariable(triple.subject)) {
//             variables.insert(triple.subject);
//             varPositions[triple.subject].emplace_back(i, 0); // 0 表示主语位置
//         }
//         if (isVariable(triple.predicate)) {
//             variables.insert(triple.predicate);
//             varPositions[triple.predicate].emplace_back(i, 1); // 1 表示谓语位置
//             // 实际基本不考虑谓语为变量的情况，但以防万一还是加上
//         }
//         if (isVariable(triple.object)) {
//             variables.insert(triple.object);
//             varPositions[triple.object].emplace_back(i, 2); // 2 表示宾语位置
//         }
//     }

//     if (!checkConflictingTriples(bindings, varPositions, rule)) {
//         return;
//     }

//     const Triple& head = rule.head;
//     bindings[head.subject] = currentTriple.subject;
//     bindings[head.object] = currentTriple.object;


//     // std::map<std::string, std::string> bindings;
//     // 对每个变量进行leapfrog join
//     join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, 0, newFacts);
// }

void DatalogEngine::join_by_variable(
    TrieNode* psoRoot, TrieNode* posRoot,
    const Rule& rule,  // 当前规则
    const std::set<std::string>& variables,  // 当前规则的变量全集
    const std::map<std::string, std::vector<std::pair<int, int>>>& varPositions,  // 变量 -> [(变量所在三元组模式在规则体中的下标, 主0/谓1/宾2)]
    std::map<std::string, std::string>& bindings,  // 变量 -> 变量当前的绑定值（常量，未绑定则为空）
    int varIdx,
    std::vector<Triple>& newFacts
) {

    // printf("join_by_variable called with varIdx: %d\n", varIdx);
    // printf("Current bindings: ");
    // for (const auto& binding : bindings) {
    //     printf("%s -> %s, ", binding.first.c_str(), binding.second.c_str());
    // }
    // printf("\n");  
    // 当所有变量都已绑定时，生成新的事实
    if (varIdx >= variables.size()) {
        //遍历rule的body中的三元组，是否在store中存在
        for( const auto& triple : rule.body) {
            const Triple &substitutedTriple = Triple(
                substituteVariable(triple.subject, bindings),
                substituteVariable(triple.predicate, bindings),
                substituteVariable(triple.object, bindings)
            );
            // printf("Checking triple: (%s, %s, %s)\n", substitutedTriple.subject.c_str(), substitutedTriple.predicate.c_str(), substitutedTriple.object.c_str());
            if (store.getNodeByTriple(substitutedTriple) == nullptr) {
                // 如果三元组不存在，则不生成新事实
                return;
            }
        }
        std::string newSubject = substituteVariable(rule.head.subject, bindings);
        std::string newPredicate = substituteVariable(rule.head.predicate, bindings);
        std::string newObject = substituteVariable(rule.head.object, bindings);

        newFacts.emplace_back(newSubject, newPredicate, newObject);
        return;
    }
    // 获取当前要处理的变量
    auto varIt = variables.begin();
    std::advance(varIt, varIdx);
    std::string currentVar = *varIt;

    // 如果当前变量已绑定，则直接处理下一个
    if (bindings.find(currentVar) != bindings.end()) {
        join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, varIdx + 1, newFacts);
        return;
    }
    // 对当前变量创建迭代器
    std::vector<TrieIterator*> iterators;
    for (const auto& pos : varPositions.at(currentVar)) {
        int tripleIdx = pos.first;
        int position = pos.second;
        const Triple& triple = rule.body[tripleIdx];

        TrieIterator* it = nullptr;

        // 根据变量位置选择适当的Trie
        if (position == 0) { // 主语位置
            // 如果宾语已绑定，就从posTrie中对应宾语的子节点中查找
            if (!isVariable(triple.object) || bindings.find(triple.object) != bindings.end()) {
                it = new TrieIterator(posRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    // 对已确定的宾语进行seek
                    TrieIterator objIt = it->open();
                    std::string objValue = substituteVariable(triple.object, bindings);
                    objIt.seek(objValue);
                    if (!objIt.atEnd() && objIt.key() == objValue) {
                        iterators.push_back(new TrieIterator(objIt.open()));
                    }
                }
            }
            // 否则，从psoTrie中查找
            else {
                it = new TrieIterator(psoRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    TrieIterator subIt = it->open();
                    iterators.push_back(new TrieIterator(subIt));
                }
            }
        }
        // 这里暂时不考虑谓语为变量，即position == 1的情况
        else if (position == 2) { // 宾语位置
            // 如果主语已绑定，就从psoTrie中对应主语的子节点中查找
            if (!isVariable(triple.subject) || bindings.find(triple.subject) != bindings.end()) {
                it = new TrieIterator(psoRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    // 对已确定的主语进行seek
                    TrieIterator subjIt = it->open();
                    std::string subjValue = substituteVariable(triple.subject, bindings);
                    subjIt.seek(subjValue);
                    if (!subjIt.atEnd() && subjIt.key() == subjValue) {
                        iterators.push_back(new TrieIterator(subjIt.open()));
                    }
                }
            }
            // 否则，从posTrie中查找
            else {
                it = new TrieIterator(posRoot);
                // 对谓语进行seek
                std::string predValue = substituteVariable(triple.predicate, bindings);
                it->seek(predValue);
                if (!it->atEnd() && it->key() == predValue) {
                    TrieIterator objIt = it->open();
                    iterators.push_back(new TrieIterator(objIt));
                }
            }
        }

        if (it && iterators.empty()) {
            delete it;
        }
    }

    // 对当前变量执行leapfrog join
    if (!iterators.empty()) {
        LeapfrogJoin lf(iterators);
        while (!lf.atEnd()) {

            std::string key = lf.key();
            bindings[currentVar] = key;  // 将当前变量绑定到迭代器的key上
            // 递归处理下一个变量
            join_by_variable(psoRoot, posRoot, rule, variables, varPositions, bindings, varIdx + 1, newFacts);

            lf.next();
        }

        // 清理迭代器
        for (auto it : iterators) {
            delete it;
        }
    }

    // 删除当前变量的绑定
    bindings.erase(currentVar);

}

// 辅助函数：若绑定中存在变量则替换其绑定的值，否则返回原字符串（此时为常量）
std::string DatalogEngine::substituteVariable(const std::string& term, const std::map<std::string, std::string>& bindings) {
    if (isVariable(term) && bindings.find(term) != bindings.end()) {
        return bindings.at(term);
    }
    return term;
}

bool DatalogEngine::checkConflictingTriples(
    const std::map<std::string, std::string>& bindings,
    const std::map<std::string, std::vector<std::pair<int, int>>>& varPositions,
    const Rule& rule
) const {
    // 遍历bindings中的变量
    for (const auto& [var, value] : bindings) {
        if (varPositions.find(var) == varPositions.end()) {
            continue; // 如果变量不在varPositions中，跳过
        }

        const auto& positions = varPositions.at(var);
        std::map<int, std::set<int>> idxToPos;

        // 构建idx到pos的映射
        for (const auto& [idx, pos] : positions) {
            idxToPos[idx].insert(pos);
        }

        // 检查是否同时存在(idx, 0)和(idx, 2)
        for (const auto& [idx, posSet] : idxToPos) {
            if (posSet.count(0) && posSet.count(2)) {
                // 构造实际的三元组
                const Triple& pattern = rule.body[idx];
                std::string subject = substituteVariable(pattern.subject, bindings);
                std::string predicate = substituteVariable(pattern.predicate, bindings);
                std::string object = substituteVariable(pattern.object, bindings);

                Triple actualTriple(subject, predicate, object);

                // 检查三元组是否存在于事实库中
                if (store.getNodeByTriple(actualTriple) != nullptr) {
                    return false;
                }
            }
        }
    }

    // 检查规则体中是否有只含常量不含变量的三元组模式
    for (const auto& triple : rule.body) {
        if (!isVariable(triple.subject) && !isVariable(triple.predicate) && !isVariable(triple.object)) {
            // 构造实际的三元组
            Triple actualTriple(triple.subject, triple.predicate, triple.object);

            // 检查三元组是否存在于事实库中
            if (store.getNodeByTriple(actualTriple) != nullptr) {
                return false;
            }
        }
    }

    return true;
}

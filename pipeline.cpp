#include "./pipeline.h"

auto ppl::internal::dfs_forwards(std::stack<int> &stack, const int &node, std::map<int, bool> &visited, const std::map<int, std::set<int>> &nodes_to_) -> void {
    visited.at(node) = true;
    for (const auto &node : nodes_to_.at(node)) {
        if (!visited.at(node)) {
            dfs_forwards(stack, node, visited, nodes_to_);
        }
    }
    stack.push(node);
}

auto ppl::internal::dfs_backwards(const int &node, std::map<int, bool> &visited, const std::map<int, std::map<int, int>> nodes_from_) -> void {
    visited.at(node) = true;
    for (const auto &slot : nodes_from_.at(node)) {
        const auto dst = slot.second;
        if (!visited.at(dst)) {
            dfs_backwards(dst, visited, nodes_from_);
        }
    }
}

auto ppl::internal::is_connected(const std::map<int, std::unique_ptr<node>> &nodes_, 
const std::map<int, std::set<int>> &nodes_to_, const std::map<int, std::map<int, int>> &nodes_from_) -> bool {
    auto visited_forwards = std::map<int, bool>{};
    auto visited_backwards = std::map<int, bool>{};
    for (const auto &node : nodes_) {
        visited_forwards.emplace(node.first, false);
        visited_backwards.emplace(node.first, false);
    }
    auto stack = std::stack<int>{};
    internal::dfs_forwards(stack, nodes_.begin()->first, visited_forwards, nodes_to_);
    internal::dfs_backwards(nodes_.begin()->first, visited_backwards, nodes_from_);
    for (const auto &node : nodes_) {
        if (!visited_forwards.at(node.first) && !visited_backwards.at(node.first)) {
            return false;
        }
    }
    return true;
}

auto ppl::internal::check_cycle(const int &node, std::map<int, bool> &visited, std::map<int, bool> &in_stack, const std::map<int, std::set<int>> &nodes_to_) -> bool {
    if (in_stack.at(node)) {
        return true;
    }
    if (visited.at(node)) {
        return false;
    }
    visited.at(node) = true;
    in_stack.at(node) = true;
    for (const auto v : nodes_to_.at(node)) {
        if (check_cycle(v, visited, in_stack, nodes_to_)) {
            return true;
        }
    }
    in_stack.at(node) = false;
    return false;
}

auto ppl::internal::has_cycle(const std::map<int, std::unique_ptr<node>> &nodes_, const std::map<int, std::set<int>> &nodes_to_) -> bool {
    auto visited = std::map<int, bool>{};
    auto in_stack = std::map<int, bool>{};
    for (const auto &node : nodes_) {
        visited.emplace(node.first, false);
        in_stack.emplace(node.first, false);
    }
    for (const auto &node : nodes_) {
        if (check_cycle(node.first, visited, in_stack, nodes_to_)) {
            return true;
        }
    }
    return false;
}

auto ppl::internal::topological_sort(const std::map<int, std::unique_ptr<node>> &nodes_, const std::map<int, std::set<int>> &nodes_to_) -> std::stack<int> {
    auto stack = std::stack<int>{};
    auto visited = std::map<int, bool>{};

    for (const auto &node : nodes_) {
        visited.emplace(node.first, false);
    }

    for (const auto &node : nodes_) {
        if (!visited.at(node.first)) {
            dfs_forwards(stack, node.first, visited, nodes_to_);
        }
    }

    return stack;
}

auto ppl::internal::update_poll(const std::set<int> &node_list, const poll status, std::map<int, poll> &node_polls_, const std::map<int, std::set<int>> &nodes_to_) -> void {
    for (const auto &node : node_list) {
        if (node_polls_.contains(node)) {
            if (node_polls_.at(node) != poll::closed) {
                node_polls_.at(node) = status;
            }
        } else {
            node_polls_.emplace(node, status);
        }
        update_poll(nodes_to_.at(node), status, node_polls_, nodes_to_);
    }
}
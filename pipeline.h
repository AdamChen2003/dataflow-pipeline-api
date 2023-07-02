#ifndef COMP6771_PIPELINE_H
#define COMP6771_PIPELINE_H

#include <type_traits>
#include <cassert>
#include <map>
#include <concepts>
#include <cstddef>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <stack>
#include <string>
#include <tuple>
#include <typeinfo>
#include <typeindex>
#include <utility>
#include <vector>

namespace ppl {
    // Errors that may occur in a pipeline.
    enum class pipeline_error_kind {
        // An expired node ID was provided.
        invalid_node_id,
        // Attempting to bind a non-existant slot.
        no_such_slot,
        // Attempting to bind to a slot that is already filled.
        slot_already_used,
        // The output type and input types for a connection don't match.
        connection_type_mismatch,
    };

    struct pipeline_error : std::exception {
        explicit pipeline_error(pipeline_error_kind err): err_{err} {};
        auto kind() const noexcept -> pipeline_error_kind {
            return err_;
        }
        auto what() const noexcept -> const char* override {
            switch (err_) {
                case pipeline_error_kind::invalid_node_id:
                    return "invalid node ID";
                case pipeline_error_kind::no_such_slot:
                    return "no such slot";
                case pipeline_error_kind::slot_already_used:
                    return "slot already used";
                case pipeline_error_kind::connection_type_mismatch:
                    return "connection type mismatch";
                break;
			}
            return "";
		}

    private:
        pipeline_error_kind err_;
    };

    enum class poll {
        // A value is available.
        ready,
        // No value is available this time, but there might be one later.
        empty,
        // No value is available, and there never will be again:
        // every future poll for this node will return `poll::closed` again.
        closed,
    };

    class node {
    public:
        auto virtual name() const -> std::string = 0;
        virtual ~node() noexcept = default;

    private:
        std::vector<std::type_index> input_types_;
        std::type_index output_type_ = std::type_index(typeid(void));
        bool is_source;
        bool is_sink;

        auto virtual poll_next() -> poll = 0;
        auto virtual connect(const node* src, const int slot) -> void = 0;
        friend class pipeline;
    };

    template <typename Output>
    struct producer : public node {
    public:
        using output_type = Output;
        auto virtual value() const -> const output_type& = 0;
    };

    template <>
    struct producer<void> : public node {
    public:
        using output_type = void;
    };

    template <typename Input, typename Output>
    struct component : public producer<Output> {
    public:
        using input_type = Input;
        using output_type = Output;
    };

    template <typename Input>
    struct sink : public component<std::tuple<Input>, void> {};

    template <typename Output>
    struct source : public component<std::tuple<>, Output> {
    private:
        auto connect(const node*, const int) -> void override final {};
    };

    namespace internal {
        template <typename T>
        struct is_a_tuple: std::false_type {};
        template <typename ...Ts>
        struct is_a_tuple<std::tuple<Ts...>>: std::true_type {};
        template <typename T>
        constexpr bool is_a_tuple_v = is_a_tuple<T>::value;

        template <int I = 0, typename... Ts>
        auto fill_types(std::vector<std::type_index> &input_types, const std::tuple<Ts...> &tup) -> void {
            if constexpr(I == sizeof...(Ts)){
                return;
            } else {
                input_types.push_back(std::type_index(typeid(std::get<I>(tup))));
                fill_types<I + 1>(input_types, tup);
            }
        }

        auto dfs_forwards(std::stack<int> &stack, const int &node, std::map<int, bool> &visited, const std::map<int, std::set<int>> &nodes_to_) -> void;
        auto dfs_backwards(const int &node, std::map<int, bool> &visited, const std::map<int, std::map<int, int>> nodes_from_) -> void;
        auto is_connected(const std::map<int, std::unique_ptr<node>> &nodes_, const std::map<int, std::set<int>> &nodes_to_, 
        const std::map<int, std::map<int, int>> &nodes_from_) -> bool;
        auto check_cycle(const int &node, std::map<int, bool> &visited, std::map<int, bool> &in_stack, const std::map<int, std::set<int>> &nodes_to_) -> bool;
        auto has_cycle(const std::map<int, std::unique_ptr<node>> &nodes_, const std::map<int, std::set<int>> &nodes_to_) -> bool;
        auto topological_sort(const std::map<int, std::unique_ptr<node>> &nodes_, const std::map<int, std::set<int>> &nodes_to_) -> std::stack<int>;
        auto update_poll(const std::set<int> &node_list, const poll status, std::map<int, poll> &node_polls_, const std::map<int, std::set<int>> &nodes_to_) -> void;
    }

    template <typename N>
    concept concrete_node = requires {
        typename N::input_type;
        typename N::output_type;
        requires std::derived_from<N, producer<typename N::output_type>>;
        requires !std::is_abstract_v<N>;
        requires internal::is_a_tuple_v<typename N::input_type>;
    };

    class pipeline {
    public:
        using node_id = int;
        pipeline() = default;
        pipeline(const pipeline &) = delete;
        pipeline(pipeline &&other) = default;
        auto operator=(const pipeline &) -> pipeline& = delete;
        auto operator=(pipeline &&) -> pipeline& = default;
        ~pipeline() noexcept = default;

        template <typename N, typename... Args>
        requires concrete_node<N> and std::constructible_from<N, Args...>
        auto create_node(Args&& ...args) -> node_id {
            latest_id_++;
            nodes_.emplace(latest_id_, std::make_unique<N>(std::forward<Args>(args)...));
            nodes_from_.emplace(latest_id_, std::map<int, node_id>{});
            nodes_to_.emplace(latest_id_, std::set<node_id>{});
            const auto inputs = typename N::input_type{};
            internal::fill_types(nodes_.at(latest_id_)->input_types_, inputs);
            if constexpr (!std::derived_from<N, producer<void>>) {
                nodes_.at(latest_id_)->output_type_ = std::type_index(typeid(typename N::output_type{}));
            }

            if (std::derived_from<N, component<std::tuple<>,typename N::output_type>>) {
                nodes_.at(latest_id_)->is_source = true;
            } else {
                nodes_.at(latest_id_)->is_source = false;
            }

            if (std::derived_from<N, producer<void>>) {
                nodes_.at(latest_id_)->is_sink = true;
            } else {
                nodes_.at(latest_id_)->is_sink = false;
            }

            return latest_id_;
        }

        auto erase_node(const node_id &n_id) -> void {
            if (!nodes_.contains(n_id)) {
                throw pipeline_error(pipeline_error_kind::invalid_node_id);
            }

            auto disconnected_nodes_from = std::vector<node_id>{};
            for (const auto &slot : nodes_from_.at(n_id)) {
                disconnected_nodes_from.push_back(slot.second);
            }
            for (const auto &node_from : disconnected_nodes_from) {
                disconnect(node_from, n_id);
            }

            auto disconnected_nodes_to = std::vector<node_id>{};
            for (const auto &node_to : nodes_to_.at(n_id)) {
                disconnected_nodes_to.push_back(node_to);
            }
            for (const auto &node_to : disconnected_nodes_to) {
                disconnect(n_id, node_to);
            }

            nodes_.erase(n_id);
            nodes_to_.erase(n_id);
            nodes_from_.erase(n_id);
        }

        auto get_node(const node_id &n_id) const -> const node* {
            return nodes_.contains(n_id) ? nodes_.at(n_id).get() : nullptr;
        }

        auto get_node(const node_id &n_id) -> node* {
            return nodes_.contains(n_id) ? nodes_.at(n_id).get() : nullptr;
        }

        auto connect(const node_id &src, const node_id &dst, const int &slot) -> void {
            if (!nodes_.contains(src) || !nodes_.contains(dst)) {
                throw pipeline_error(pipeline_error_kind::invalid_node_id);
            }
            const auto input_types = nodes_.at(dst).get()->input_types_;
            if (nodes_from_.at(dst).contains(slot)) {
                throw pipeline_error(pipeline_error_kind::slot_already_used);
            }
            if (input_types.size() <= static_cast<std::size_t>(slot) || slot < 0) {
                throw pipeline_error(pipeline_error_kind::no_such_slot);
            }
            if (input_types.at(static_cast<std::size_t>(slot)) != nodes_.at(src).get()->output_type_) {
                throw pipeline_error(pipeline_error_kind::connection_type_mismatch);
            }
            nodes_.at(dst).get()->connect(nodes_.at(src).get(), slot);
            nodes_from_.at(dst).emplace(slot, src);
            nodes_to_.at(src).insert(dst);
        }

        auto disconnect(const node_id &src, const node_id &dst) -> void {
            if (!nodes_.contains(src) || !nodes_.contains(dst)) {
                throw pipeline_error(pipeline_error_kind::invalid_node_id);
            }
            auto iter = std::find(nodes_to_.at(src).begin(), nodes_to_.at(src).end(), dst);
            if (iter != nodes_to_.at(src).end()) {
                nodes_to_.at(src).erase(iter);
                auto removed_slots = std::vector<int>{};
                for (const auto &slot : nodes_from_.at(dst)) {
                    if (slot.second == src) {
                        removed_slots.push_back(slot.first);
                    }
                }
                for (const auto &slot : removed_slots) {
                    nodes_from_.at(dst).erase(slot);
                }
            }
        }

        auto get_dependencies(const node_id &src) const -> std::vector<std::pair<node_id, int>> {
            if (!nodes_.contains(src)) {
                throw pipeline_error(pipeline_error_kind::invalid_node_id);
            }
            auto dependencies = std::vector<std::pair<node_id, int>>{};
            for (const auto &dst_node : nodes_to_.at(src)) {
                for (const auto &slot : nodes_from_.at(dst_node)) {
                    if (slot.second == src) {
                        dependencies.push_back(std::make_pair(dst_node, slot.first));
                    }
                }
            }
            return dependencies;
        }

        auto is_valid() const -> bool {
            bool contains_source = false;
            bool contains_sink = false;
            for (const auto &node : nodes_) {
                // All source slots for all nodes must be filled.
                if (node.second->input_types_.size() != nodes_from_.at(node.first).size()) {
                    return false;
                }
                // All non-sink nodes must have at least one dependent.
                if (!node.second->is_sink && nodes_to_.at(node.first).size() == 0) {
                    return false;
                }
                if (node.second->is_source) {
                    contains_source = true;
                }
                if (node.second->is_sink) {
                    contains_sink = true;
                }
            }
            return true && contains_source && contains_sink && internal::is_connected(nodes_, nodes_to_, nodes_from_)
             && !internal::has_cycle(nodes_, nodes_to_);
        }

        auto step() -> bool {
            auto stack = internal::topological_sort(nodes_, nodes_to_);
            node_polls_.clear();
            while (!stack.empty()) {
                const auto node = stack.top();
                if (!(node_polls_.contains(node) && (node_polls_.at(node) == poll::closed || node_polls_.at(node) == poll::empty))) {
                    const auto result = nodes_.at(node)->poll_next();
                    if (node_polls_.contains(node)) {
                        node_polls_.at(node) = result;
                    } else {
                        node_polls_.emplace(node, result);
                    }
                    if (result == poll::ready || result == poll::empty) {
                        if (result == poll::empty) {
                            // Make all dependent nodes empty
                            internal::update_poll(nodes_to_.at(node), result, node_polls_, nodes_to_);
                        }
                    } else {
                        // Make all dependent nodes closed
                        internal::update_poll(nodes_to_.at(node), result, node_polls_, nodes_to_);
                    }
                }
                stack.pop();
            }

            for (const auto &node : nodes_) {
                if (node.second->is_sink && !(node_polls_.contains(node.first) && node_polls_.at(node.first) == poll::closed)) {
                    return false;
                }
            }
            return true;
        }

        void run() {
            while (!step()) {}
        }

        friend std::ostream &operator<<(std::ostream &os, const pipeline &pipe) {
            os << "digraph G {\n";
            for (const auto &node : pipe.nodes_) {
                os << "  \"" + std::to_string(node.first) + " " + node.second->name() + "\"\n";
            }
            auto edges_list = std::vector<std::pair<int, int>>{};

            for (const auto &nodes_from : pipe.nodes_from_) {
                for (const auto &node : nodes_from.second) {
                    edges_list.push_back({node.second, nodes_from.first});
                }
            }

            std::sort(edges_list.begin(), edges_list.end(),
            [](const std::pair<int, int> &a, const std::pair<int, int> &b) -> bool { 
                if (a.first != b.first) {
                    return a.first < b.first;
                }
                return a.second < b.second;
            });

            os << '\n';
            for (const auto &edge : edges_list) {
                os << "  \"" + std::to_string(edge.first) + " " + pipe.nodes_.at(edge.first)->name()
                    + "\" -> \"" + std::to_string(edge.second) + " " + pipe.nodes_.at(edge.second)->name() + "\"\n";
            }

            os << "}\n";
            return os;
        }

    private:
        std::map<node_id, std::unique_ptr<node>> nodes_;
        node_id latest_id_;
        std::map<node_id, poll> node_polls_;
        std::map<node_id, std::set<node_id>> nodes_to_;
        std::map<node_id, std::map<int, node_id>> nodes_from_;
    };
}

#endif  // COMP6771_PIPELINE_H
